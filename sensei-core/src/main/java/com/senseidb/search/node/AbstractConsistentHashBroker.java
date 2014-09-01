package com.senseidb.search.node;

import com.google.common.collect.Sets;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.log4j.Logger;

import zu.core.cluster.ZuCluster;
import zu.core.cluster.routing.ConsistentHashRoutingAlgorithm;
import zu.core.cluster.routing.RoutingAlgorithm;
import zu.finagle.client.ZuFinagleServiceDecorator;
import zu.finagle.client.ZuTransportClientProxy;
import zu.finagle.serialize.ZuSerializer;

import com.senseidb.conf.SenseiConfParams;
import com.senseidb.metrics.MetricsConstants;
import com.senseidb.search.req.AbstractSenseiRequest;
import com.senseidb.search.req.AbstractSenseiResult;
import com.senseidb.search.req.ErrorType;
import com.senseidb.search.req.SenseiError;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.svc.api.SenseiException;
import com.twitter.finagle.Service;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 *
 * @param <REQUEST>
 * @param <RESULT>
 */
public abstract class AbstractConsistentHashBroker<REQUEST extends AbstractSenseiRequest, RESULT extends AbstractSenseiResult>
    extends AbstractSenseiBroker<REQUEST, RESULT> {
  private final static Logger logger = Logger.getLogger(AbstractConsistentHashBroker.class);
  private final static Logger queryLogger = Logger.getLogger("com.sensei.querylog");

  protected long _timeout = 8000;
  protected final ZuSerializer<REQUEST, RESULT> _serializer;
  private int _finagleThreadNumber = 20;

  private static Timer ScatterTimer = null;
  private static Timer GatherTimer = null;
  private static Timer TotalTimer = null;
  private static Meter SearchCounter = null;
  private static Meter ErrorMeter = null;
  private static Meter EmptyMeter = null;

  protected ZuFinagleServiceDecorator<REQUEST, RESULT> serviceDecorator;
  private final RoutingAlgorithm<Service<REQUEST, RESULT>> router;
  static {
    // register metrics monitoring for timers
    try {
      MetricName scatterMetricName = new MetricName(MetricsConstants.Domain, "timer",
          "scatter-time", "broker");
      ScatterTimer = Metrics.newTimer(scatterMetricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

      MetricName gatherMetricName = new MetricName(MetricsConstants.Domain, "timer", "gather-time",
          "broker");
      GatherTimer = Metrics.newTimer(gatherMetricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

      MetricName totalMetricName = new MetricName(MetricsConstants.Domain, "timer", "total-time",
          "broker");
      TotalTimer = Metrics.newTimer(totalMetricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

      MetricName searchCounterMetricName = new MetricName(MetricsConstants.Domain, "meter",
          "search-count", "broker");
      SearchCounter = Metrics.newMeter(searchCounterMetricName, "requets", TimeUnit.SECONDS);

      MetricName errorMetricName = new MetricName(MetricsConstants.Domain, "meter", "error-meter",
          "broker");
      ErrorMeter = Metrics.newMeter(errorMetricName, "errors", TimeUnit.SECONDS);

      MetricName emptyMetricName = new MetricName(MetricsConstants.Domain, "meter", "empty-meter",
          "broker");
      EmptyMeter = Metrics.newMeter(emptyMetricName, "null-hits", TimeUnit.SECONDS);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    }
  }

  /**
   * @param clusterClient
   * @param serializer
   *          The serializer used to serialize/deserialize request/response pairs
   */
  public AbstractConsistentHashBroker(ZuCluster clusterClient,
      ZuSerializer<REQUEST, RESULT> serializer, Configuration senseiConf) {
    super();
    _serializer = serializer;
    ZuTransportClientProxy<REQUEST, RESULT> proxy = new ZuTransportClientProxy<REQUEST, RESULT>(
        getMessageType(), _serializer);
    if (this instanceof SenseiSysBroker) {
      // hard code config for SenseiSysBroker
      _timeout = 8000;
      _finagleThreadNumber = 4;
    } else {
      _timeout = senseiConf.getLong(SenseiConfParams.SERVER_BROKER_TIMEOUT, 8000);
      _finagleThreadNumber = senseiConf.getInt(SenseiConfParams.SERVER_BROKER_FINAGLE_THREAD, 20);
    }
    serviceDecorator = new ZuFinagleServiceDecorator<REQUEST, RESULT>(proxy, Duration.apply(
      _timeout, TimeUnit.MILLISECONDS), _finagleThreadNumber);

    router = new ConsistentHashRoutingAlgorithm<Service<REQUEST, RESULT>>(serviceDecorator);
    clusterClient.addClusterEventListener(router);
  }

  public REQUEST customizeRequest(REQUEST request) {
    return request;
  }

  /**
   * @return an empty result instance. Used when the request cannot be properly
   *         processed or when the true result is empty.
   */
  @Override
  public abstract RESULT getEmptyResultInstance();

  /**
   * The method that provides the search service.
   *
   * @param req
   * @return
   * @throws SenseiException
   */
  @Override
  public RESULT browse(final REQUEST req) throws SenseiException {
    SearchCounter.mark();
    try {
      return TotalTimer.time(new Callable<RESULT>() {
        @Override
        public RESULT call() throws Exception {
          return doBrowse(req);
        }
      });
    } catch (Exception e) {
      ErrorMeter.mark();
      throw new SenseiException(e.getMessage(), e);
    }
  }

  /**
   * Merge results on the client/broker side. It likely works differently from
   * the one in the search node.
   *
   * @param request
   *          the original request object
   * @param resultList
   *          the list of results from all the requested partitions.
   * @return one single result instance that is merged from the result list.
   */
  public abstract RESULT mergeResults(REQUEST request, List<RESULT> resultList);

  protected String getRouteParam(REQUEST req) {
    String param = req.getRouteParam();
    if (param == null) {
      return RandomStringUtils.random(4);
    } else {
      return param;
    }
  }

  protected RESULT doBrowse(final REQUEST req) {
    final long time = System.currentTimeMillis();

    final List<RESULT> resultList = new ArrayList<RESULT>();

    try {
      resultList.addAll(ScatterTimer.time(new Callable<List<RESULT>>() {
        @Override
        public List<RESULT> call() throws Exception {
          return doCall(req);
        }
      }));
    } catch (Exception e) {
      ErrorMeter.mark();
      RESULT emptyResult = getEmptyResultInstance();
      logger.error("Error running scatter/gather", e);
      emptyResult.addError(new SenseiError("Error gathering the results, " + e.getMessage(),
          ErrorType.BrokerGatherError));
      emptyResult.setTime(System.currentTimeMillis() - time);
      return emptyResult;
    }

    if (resultList.size() == 0) {
      logger.error("no result received at all return empty result");
      RESULT emptyResult = getEmptyResultInstance();
      emptyResult.addError(new SenseiError("Error gathering the results. "
          + "no result received at all return empty result", ErrorType.BrokerGatherError));
      EmptyMeter.mark();
      emptyResult.setTime(System.currentTimeMillis() - time);
      return emptyResult;
    }

    RESULT result = null;
    try {
      result = GatherTimer.time(new Callable<RESULT>() {
        @Override
        public RESULT call() throws Exception {
          return mergeResults(req, resultList);
        }
      });
    } catch (Exception e) {
      result = getEmptyResultInstance();
      logger.error("Error gathering the results: ", e);
      result.addError(new SenseiError("Error gathering the results, " + e.getMessage(),
          ErrorType.BrokerGatherError));
      ErrorMeter.mark();
    }
    result.setTime(System.currentTimeMillis() - time);
    queryLogger.info("doBrowse took " + result.getTime() + "ms");
    return result;
  }

  @SuppressWarnings("unchecked")
  protected List<RESULT> doCall(REQUEST req) {
    Set<Integer> allShards = router.getShards();
    
    Set<Integer> shards = req.getPartitions();
    if (shards == null || shards.isEmpty()) {
      shards = allShards;
    } else {
      shards = Sets.intersection(allShards, shards);
    }

    Map<Service<REQUEST, RESULT>, REQUEST> serviceToRequest = new HashMap<Service<REQUEST, RESULT>, REQUEST>();

    byte[] routeBytes = getRouteParam(req).getBytes();
    for (Integer shard : shards) {
      Service<REQUEST, RESULT> service = router.route(routeBytes, shard);
      if (service == null) {
        logger.warn("router returned null as a destination service");
        continue;
      }

      REQUEST requestToNode = serviceToRequest.get(service);
      if (requestToNode == null) {
        // TODO: Cloning is yucky per http://www.artima.com/intv/bloch13.htm
        requestToNode = (REQUEST) (((SenseiRequest) req).clone());
        requestToNode = customizeRequest(requestToNode);
        requestToNode.setPartitions(new HashSet<Integer>());
        serviceToRequest.put(service, requestToNode);
      }
      requestToNode.getPartitions().add(shard);
    }

    return executeRequestsInParallel(serviceToRequest, _timeout);
  }

  protected abstract String getMessageType();

  @Override
  public void shutdown() {
    logger.info("shutting down broker...");
  }

  protected List<RESULT> executeRequestsInParallel(
      final Map<Service<REQUEST, RESULT>, REQUEST> serviceToRequest, long timeout) {
    final long start = System.currentTimeMillis();
    final List<Future<RESULT>> futures = new ArrayList<Future<RESULT>>();
    final List<RESULT> results = new ArrayList<RESULT>();
    final Map<Service<REQUEST, RESULT>, Long> latencies = new HashMap<Service<REQUEST, RESULT>, Long>();
    for (final Entry<Service<REQUEST, RESULT>, REQUEST> entry : serviceToRequest.entrySet()) {
      latencies.put(entry.getKey(), (long) -1);
      futures.add(entry.getKey().apply(entry.getValue())
          .addEventListener(new FutureEventListener<RESULT>() {

            @Override
            public void onFailure(Throwable t) {
              logger.error("Failed to get response from " + getServiceAddress(entry.getKey()), t);
            }

            @Override
            public void onSuccess(RESULT result) {
              synchronized (results) {
                results.add(result);
                latencies.put(entry.getKey(), System.currentTimeMillis() - start);
              }
            }
          }));
    }

    Future<List<RESULT>> collected = Future.collect(futures);
    try {
      collected.apply(Duration.apply(timeout, TimeUnit.MILLISECONDS));
    } catch (Exception e) {
      logger.error("Failed to get results from all nodes, exception: " + e.getMessage());
    }

    String latencyLog = "";
    for (final Entry<Service<REQUEST, RESULT>, Long> entry : latencies.entrySet()){
      if (entry.getValue() == -1) {
        logger.error("Missed result from " + getServiceAddress(entry.getKey()));
        continue;
      }
      latencyLog += getServiceAddress(entry.getKey()) + ":" + entry.getValue() + "ms;";
    }

    logger.info(String.format("Getting responses from %d nodes took %dms, nodes latency distribution: %s", results.size(),
      (System.currentTimeMillis() - start), latencyLog));
    return results;
  }

  protected static Set<InetSocketAddress> getNodesAddresses(
      Map<Integer, List<InetSocketAddress>> clusterView) {
    Set<InetSocketAddress> nodes = new HashSet<InetSocketAddress>();
    for (List<InetSocketAddress> inetSocketAddressList : clusterView.values()) {
      nodes.addAll(inetSocketAddressList);
    }
    return nodes;
  }

  public InetSocketAddress getServiceAddress(Service<REQUEST, RESULT> service) {
    return router.getServiceAddress(service);
  }
}
