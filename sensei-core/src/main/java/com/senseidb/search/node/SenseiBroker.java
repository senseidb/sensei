package com.senseidb.search.node;

import java.net.InetSocketAddress;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.lucene.search.SortField;

import proj.zoie.api.indexing.AbstractZoieIndexable;
import zu.core.cluster.ZuCluster;
import zu.core.cluster.ZuClusterEventListener;

import com.browseengine.bobo.api.BrowseHit.SerializableField;
import com.browseengine.bobo.api.FacetSpec;
import com.senseidb.indexing.DefaultJsonSchemaInterpreter;
import com.senseidb.search.req.ErrorType;
import com.senseidb.search.req.SenseiError;
import com.senseidb.search.req.SenseiHit;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.svc.impl.CoreSenseiServiceImpl;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

/**
 * This SenseiBroker routes search(browse) request using the routers created by
 * the supplied router factory. It uses scatter-gather handling
 * mechanism to handle distributed search, which does not support request based
 * context sensitive routing.
 */
public class SenseiBroker extends AbstractConsistentHashBroker<SenseiRequest, SenseiResult>
    implements ZuClusterEventListener {
  private final static Logger logger = Logger.getLogger(SenseiBroker.class);

  private static Counter numberOfNodesInTheCluster = Metrics.newCounter(new MetricName(
      SenseiBroker.class, "numberOfNodesInTheCluster"));
  private volatile boolean disconnected;

  public SenseiBroker(ZuCluster clusterClient, Configuration senseiConf) {
    super(clusterClient, CoreSenseiServiceImpl.JAVA_SERIALIZER, senseiConf);
    clusterClient.addClusterEventListener(this);
  }

  public static void recoverSrcData(SenseiResult res, SenseiHit[] hits, boolean isFetchStoredFields) {
    if (hits != null) {
      for (SenseiHit hit : hits) {
        try {
          byte[] dataBytes = hit.getStoredValue();
          if (dataBytes == null || dataBytes.length == 0) {
            dataBytes = hit.getFieldBinaryValue(AbstractZoieIndexable.DOCUMENT_STORE_FIELD);
            if (dataBytes != null) {
              List<SerializableField> fields = new ArrayList<SerializableField>(hit.getStoredFields());
              ListIterator<SerializableField> lIter = fields.listIterator();
              while (lIter.hasNext()) {
                SerializableField field = lIter.next();
                if (AbstractZoieIndexable.DOCUMENT_STORE_FIELD.equals(field.name())) {
                  lIter.remove();
                }
              }
              hit.setStoredFields(fields);
            }
          } else {
            hit.setStoredValue(null);
          }

          if (dataBytes != null && dataBytes.length > 0) {
            byte[] data = null;
            try {
              // TODO need check SenseiSchema.isCompressSrcData()
              data = DefaultJsonSchemaInterpreter.decompress(dataBytes);
            } catch (Exception ex) {
              data = dataBytes;
            }
            hit.setSrcData(new String(data, "UTF-8"));
          }
        } catch (Exception e) {
          logger.error(e.getMessage(), e);
          res.getErrors().add(new SenseiError(e.getMessage(), ErrorType.BrokerGatherError));
        }

        recoverSrcData(res, hit.getSenseiGroupHits(), isFetchStoredFields);

        // Remove stored fields since the user is not requesting:
        if (!isFetchStoredFields) {
          hit.setStoredFields((List<SerializableField>) null);
        }
      }
    }
  }

  @Override
  public SenseiResult mergeResults(SenseiRequest request, List<SenseiResult> resultList) {
    SenseiResult res = ResultMerger.merge(request, resultList, false);

    boolean fetchStoredFields = request.isFetchAllStoredFields() ||
      request.getStoredFieldsToFetch() != null && !request.getStoredFieldsToFetch().isEmpty();
    if (fetchStoredFields || request.isFetchStoredValue()) {
      long start = System.currentTimeMillis();
      recoverSrcData(res, res.getSenseiHits(), fetchStoredFields);
      res.setTime(res.getTime() + (System.currentTimeMillis() - start));
    }

    return res;
  }

  @Override
  public SenseiResult getEmptyResultInstance() {
    return new SenseiResult();
  }

  @Override
  public SenseiRequest customizeRequest(SenseiRequest request) { // Rewrite offset and count.
    request.setCount(request.getOffset() + request.getCount());
    request.setOffset(0);

    // Rewrite facet max count.
    Map<String, FacetSpec> facetSpecs = request.getFacetSpecs();
    if (facetSpecs != null) {
      for (Map.Entry<String, FacetSpec> entry : facetSpecs.entrySet()) {
        FacetSpec spec = entry.getValue();
        if (spec != null && spec.getMaxCount() < 50) spec.setMaxCount(50);
      }
    }

    // Rewrite select list to include sort and group by fields:
    if (request.getSelectSet() != null) {
      List<String> selectList = request.getSelectList();
      SortField[] sortFields = request.getSort();
      if (sortFields != null && sortFields.length != 0) {
        for (int i = 0; i < sortFields.length; ++i) {
          if (sortFields[i].getType() != SortField.Type.SCORE
              && sortFields[i].getType() != SortField.Type.DOC) {
            String field = sortFields[i].getField();
            selectList.add(field);
          }
        }
      }
      String[] groupByFields = request.getGroupBy();
      if (groupByFields != null && groupByFields.length != 0) {
        for (int i = 0; i < groupByFields.length; ++i) {
          selectList.add(groupByFields[i]);
        }
      }
      String[] distinctFields = request.getDistinct();
      if (distinctFields != null && distinctFields.length != 0) {
        for (int i = 0; i < distinctFields.length; ++i) {
          selectList.add(distinctFields[i]);
        }
      }
      request.setSelectList(selectList);
    }

    return request;
  }

  public boolean isDisconnected() {
    return disconnected;
  }

  @Override
  protected String getMessageType() {
    return CoreSenseiServiceImpl.MESSAGE_TYPE_NAME;
  }

  @Override
  public void clusterChanged(Map<Integer, List<InetSocketAddress>> clusterView) {
    logger.info("clusterChanged(): Received new clusterView from zu " + clusterView);
    Set<InetSocketAddress> nodeAddresses = getNodesAddresses(clusterView);
    synchronized (SenseiBroker.class) {
      numberOfNodesInTheCluster.clear();
      numberOfNodesInTheCluster.inc(nodeAddresses.size());
    }
  }

  @Override
  public void nodesRemoved(Set<InetSocketAddress> nodesRemoved) {
  }

}
