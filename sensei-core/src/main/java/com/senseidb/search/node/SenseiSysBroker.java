package com.senseidb.search.node;

import java.net.InetSocketAddress;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import zu.core.cluster.ZuCluster;
import zu.core.cluster.ZuClusterEventListener;

import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiSystemInfo;
import com.senseidb.svc.impl.SysSenseiCoreServiceImpl;
import com.twitter.finagle.Service;

public class SenseiSysBroker extends AbstractConsistentHashBroker<SenseiRequest, SenseiSystemInfo>
    implements ZuClusterEventListener {
  private final static Logger logger = Logger.getLogger(SenseiSysBroker.class);
  private final static long TIMEOUT_MILLIS = 8000L;
  private final long _timeoutMillis = TIMEOUT_MILLIS;
  private final Comparator<String> _versionComparator;
  private Map<InetSocketAddress, Service<SenseiRequest, SenseiSystemInfo>> _nodeAddressToService;
  private Map<Service<SenseiRequest, SenseiSystemInfo>, InetSocketAddress> _nodeServiceToAddress;

  public SenseiSysBroker(ZuCluster clusterClient, Comparator<String> versionComparator,
      Configuration senseiConf) {
    super(clusterClient, SysSenseiCoreServiceImpl.JAVA_SERIALIZER, senseiConf);
    _versionComparator = versionComparator;
    clusterClient.addClusterEventListener(this);
  }

  @Override
  public SenseiSystemInfo mergeResults(SenseiRequest request, List<SenseiSystemInfo> resultList) {
    SenseiSystemInfo result = new SenseiSystemInfo();
    if (resultList == null) return result;

    for (SenseiSystemInfo res : resultList) {
      result.setNumDocs(result.getNumDocs() + res.getNumDocs());
      result.setSchema(res.getSchema());
      if (result.getLastModified() < res.getLastModified()) result.setLastModified(res
          .getLastModified());
      if (result.getVersion() == null
          || _versionComparator.compare(result.getVersion(), res.getVersion()) < 0) result
          .setVersion(res.getVersion());
      if (res.getFacetInfos() != null) result.setFacetInfos(res.getFacetInfos());
      if (res.getClusterInfo() != null) {
        if (result.getClusterInfo() != null) result.getClusterInfo().addAll(res.getClusterInfo());
        else result.setClusterInfo(res.getClusterInfo());
      }
    }

    return result;
  }

  @Override
  protected List<SenseiSystemInfo> doCall(final SenseiRequest req) {

    Map<Service<SenseiRequest, SenseiSystemInfo>, SenseiRequest> serviceToRequest = new HashMap<Service<SenseiRequest, SenseiSystemInfo>, SenseiRequest>();
    for (Service<SenseiRequest, SenseiSystemInfo> service : _nodeAddressToService.values()) {
      serviceToRequest.put(service, req);
    }

    return executeRequestsInParallel(serviceToRequest, _timeoutMillis);
  }

  @Override
  public SenseiSystemInfo getEmptyResultInstance() {
    return new SenseiSystemInfo();
  }

  @Override
  protected String getMessageType() {
    return SysSenseiCoreServiceImpl.MESSAGE_TYPE_NAME;
  }

  @Override
  public void clusterChanged(Map<Integer, List<InetSocketAddress>> clusterView) {
    logger.info("clusterChanged(): Received new clusterView from zu " + clusterView);
    Set<InetSocketAddress> nodes = getNodesAddresses(clusterView);
    Map<InetSocketAddress, Service<SenseiRequest, SenseiSystemInfo>> addressToService = new HashMap<InetSocketAddress, Service<SenseiRequest, SenseiSystemInfo>>();
    Map<Service<SenseiRequest, SenseiSystemInfo>, InetSocketAddress> serviceToAddress = new HashMap<Service<SenseiRequest, SenseiSystemInfo>, InetSocketAddress>();
    for (InetSocketAddress nodeAddress : nodes) {
      Service<SenseiRequest, SenseiSystemInfo> service = serviceDecorator.decorate(nodeAddress);
      addressToService.put(nodeAddress, service);
      serviceToAddress.put(service, nodeAddress);
    }
    _nodeAddressToService = addressToService;
    _nodeServiceToAddress = serviceToAddress;
  }

  @Override
  public void nodesRemoved(Set<InetSocketAddress> removedNodes) {
  }

  @Override
  public InetSocketAddress getServiceAddress(Service<SenseiRequest, SenseiSystemInfo> service) {
    return _nodeServiceToAddress.get(service);
  }
}
