package com.senseidb.svc.impl;

import java.util.Comparator;

import org.apache.commons.configuration.Configuration;

import zu.core.cluster.ZuCluster;

import com.senseidb.search.node.AbstractConsistentHashBroker;
import com.senseidb.search.node.SenseiBroker;
import com.senseidb.search.node.broker.BrokerConfig;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.search.req.SenseiSystemInfo;
import com.senseidb.svc.api.SenseiException;
import com.senseidb.svc.api.SenseiService;

public class ClusteredSenseiServiceImpl implements SenseiService {
  private SenseiBroker _senseiBroker;
  private AbstractConsistentHashBroker<SenseiRequest, SenseiSystemInfo> _senseiSysBroker;
  private BrokerConfig _brokerConfig;

  public ClusteredSenseiServiceImpl(Configuration senseiConf, Comparator<String> versionComparator,
      ZuCluster clusterClient) {
    _brokerConfig = new BrokerConfig(senseiConf);
    _brokerConfig.init(clusterClient);
    _senseiBroker = _brokerConfig.buildSenseiBroker();
    _senseiSysBroker = _brokerConfig.buildSysSenseiBroker(versionComparator);
  }

  @Override
  public SenseiResult doQuery(SenseiRequest req) throws SenseiException {
    return _senseiBroker.browse(req);
  }

  @Override
  public SenseiSystemInfo getSystemInfo() throws SenseiException {
    return _senseiSysBroker.browse(new SenseiRequest());
  }

  @Override
  public void shutdown() {
    try {
      if (_senseiBroker != null) {
        _senseiBroker.shutdown();
        _senseiBroker = null;
      }
    } finally {
      try {
        if (_senseiSysBroker != null) {
          _senseiSysBroker.shutdown();
          _senseiSysBroker = null;
        }
      } finally {
        if (_brokerConfig != null) {
          _brokerConfig.shutdown();
          _brokerConfig = null;
        }
      }

    }
  }
}
