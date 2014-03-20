package com.senseidb.search.node.broker;

import java.util.Comparator;

import org.apache.commons.configuration.Configuration;

import zu.core.cluster.ZuCluster;

import com.senseidb.conf.SenseiConfParams;
import com.senseidb.search.node.AbstractConsistentHashBroker;
import com.senseidb.search.node.SenseiBroker;
import com.senseidb.search.node.SenseiSysBroker;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiSystemInfo;
import com.senseidb.servlet.SenseiConfigServletContextListener;
import com.twitter.common.net.pool.DynamicHostSet.MonitorException;

public class BrokerConfig {
  protected String clusterName;
  protected String zkurl;
  protected int zkTimeout;
  protected int writeTimeoutMillis;
  protected int connectTimeoutMillis;
  protected int maxConnectionsPerNode;
  protected int staleRequestTimeoutMins;
  protected int staleRequestCleanupFrequencyMins;

  private SenseiBroker senseiBroker;
  private SenseiSysBroker senseiSysBroker;
  private ZuCluster clusterClient;
  private final Configuration senseiConf;

  public BrokerConfig(Configuration conf) {
    senseiConf = conf;
    clusterName = senseiConf.getString(SenseiConfParams.SENSEI_CLUSTER_NAME);
    zkurl = senseiConf.getString(SenseiConfParams.SENSEI_CLUSTER_URL);
    zkTimeout = senseiConf.getInt(SenseiConfParams.SENSEI_CLUSTER_TIMEOUT, 300000);
    zkurl = senseiConf.getString(SenseiConfigServletContextListener.SENSEI_CONF_ZKURL, zkurl);
    clusterName = senseiConf.getString(SenseiConfigServletContextListener.SENSEI_CONF_CLUSTER_NAME,
      clusterName);
    zkTimeout = senseiConf.getInt(SenseiConfigServletContextListener.SENSEI_CONF_ZKTIMEOUT,
      zkTimeout);
    connectTimeoutMillis = senseiConf.getInt(
      SenseiConfigServletContextListener.SENSEI_CONF_NC_CONN_TIMEOUT, 1000);
    writeTimeoutMillis = senseiConf.getInt(
      SenseiConfigServletContextListener.SENSEI_CONF_NC_WRITE_TIMEOUT, 150);
    maxConnectionsPerNode = senseiConf.getInt(
      SenseiConfigServletContextListener.SENSEI_CONF_NC_MAX_CONN_PER_NODE, 5);
    staleRequestTimeoutMins = senseiConf.getInt(
      SenseiConfigServletContextListener.SENSEI_CONF_NC_STALE_TIMEOUT_MINS, 10);
    staleRequestCleanupFrequencyMins = senseiConf.getInt(
      SenseiConfigServletContextListener.SENSEI_CONF_NC_STALE_CLEANUP_FREQ_MINS, 10);
  }

  public void init() {
    this.init(null);
  }

  public void init(ZuCluster zuCluster) {
    if (zuCluster != null) {
      clusterClient = zuCluster;
      return;
    }
    String[] url = zkurl.split(":");
    String host = url[0];
    int zkPort = Integer.parseInt(url[1]);
    try {
      clusterClient = new ZuCluster(host, zkPort, clusterName, zkTimeout / 1000);
    } catch (MonitorException e) {
      throw new RuntimeException(e);
    }
  }

  public void shutdown() {
    clusterClient.shutdown();
  }

  public SenseiBroker buildSenseiBroker() {
    senseiBroker = new SenseiBroker(clusterClient, senseiConf);
    return senseiBroker;
  }

  public AbstractConsistentHashBroker<SenseiRequest, SenseiSystemInfo> buildSysSenseiBroker(
      Comparator<String> versionComparator) {
    senseiSysBroker = new SenseiSysBroker(clusterClient, versionComparator, senseiConf);
    return senseiSysBroker;
  }

  public SenseiBroker getSenseiBroker() {
    return senseiBroker;
  }

  public SenseiSysBroker getSenseiSysBroker() {
    return senseiSysBroker;
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public void setZkurl(String zkurl) {
    this.zkurl = zkurl;
  }

  public void setZkTimeout(int zkTimeout) {
    this.zkTimeout = zkTimeout;
  }

  public void setWriteTimeoutMillis(int writeTimeoutMillis) {
    this.writeTimeoutMillis = writeTimeoutMillis;
  }

  public void setConnectTimeoutMillis(int connectTimeoutMillis) {
    this.connectTimeoutMillis = connectTimeoutMillis;
  }

  public void setMaxConnectionsPerNode(int maxConnectionsPerNode) {
    this.maxConnectionsPerNode = maxConnectionsPerNode;
  }

  public void setStaleRequestTimeoutMins(int staleRequestTimeoutMins) {
    this.staleRequestTimeoutMins = staleRequestTimeoutMins;
  }

  public void setStaleRequestCleanupFrequencyMins(int staleRequestCleanupFrequencyMins) {
    this.staleRequestCleanupFrequencyMins = staleRequestCleanupFrequencyMins;
  }

  public ZuCluster getClusterClient() {
    return clusterClient;
  }

}
