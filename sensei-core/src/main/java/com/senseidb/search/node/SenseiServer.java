package com.senseidb.search.node;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.management.StandardMBean;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.mortbay.jetty.Server;

import proj.zoie.api.DataProvider;
import zu.core.cluster.ZuCluster;
import zu.finagle.server.ZuFinagleServer;
import zu.finagle.server.ZuTransportService;

import com.senseidb.conf.SenseiConfParams;
import com.senseidb.conf.SenseiServerBuilder;
import com.senseidb.jmx.JmxUtil;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.req.AbstractSenseiRequest;
import com.senseidb.search.req.AbstractSenseiResult;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.search.req.SenseiSystemInfo;
import com.senseidb.svc.impl.AbstractSenseiCoreService;
import com.senseidb.svc.impl.CoreSenseiServiceImpl;
import com.senseidb.svc.impl.SenseiCoreServiceMessageHandler;
import com.senseidb.svc.impl.SysSenseiCoreServiceImpl;
import com.senseidb.util.NetUtil;
import com.twitter.common.zookeeper.ServerSet.UpdateException;

public class SenseiServer {
  private static final Logger logger = Logger.getLogger(SenseiServer.class);

  private static final String AVAILABLE = "available";
  private static final String UNAVAILABLE = "unavailable";
  private final int _id;
  private final int _port;
  private final int[] _partitions;
  private final SenseiCore _core;
  private final Configuration _senseiConf;
  private final List<AbstractSenseiCoreService<AbstractSenseiRequest, AbstractSenseiResult>> _externalSvc;

  protected volatile boolean _available = false;

  private final SenseiPluginRegistry pluginRegistry;

  private final ZuTransportService transportService;
  private final ZuFinagleServer server;
  private final ZuCluster cluster;

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public SenseiServer(SenseiCore senseiCore, SenseiPluginRegistry pluginRegistry,
      ZuCluster cluster, Configuration senseiConf) throws ConfigurationException {
    _core = senseiCore;
    this.pluginRegistry = pluginRegistry;
    this.cluster = cluster;

    _senseiConf = senseiConf;
    _id = senseiConf.getInt(SenseiConfParams.NODE_ID);
    _port = senseiConf.getInt(SenseiConfParams.SERVER_PORT);
    _partitions = senseiCore.getPartitions();

    this.transportService = new ZuTransportService();
    int serverPort = senseiConf.getInt(SenseiConfParams.SERVER_PORT);
    String hostAddress;
    try {
      hostAddress = NetUtil.getHostAddress();
    } catch (Exception e) {
      throw new ConfigurationException(e.getMessage(), e);
    }
    this.server = new ZuFinagleServer("sensei-finagle-server-" + _id, new InetSocketAddress(
        hostAddress, serverPort), transportService.getService());

    _externalSvc = (List) pluginRegistry.resolveBeansByListKey(SenseiConfParams.SENSEI_PLUGIN_SVCS,
      AbstractSenseiCoreService.class);
  }

  private static String help() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("Usage: <conf.dir> [availability]\n");
    buffer.append("====================================\n");
    buffer.append("conf.dir - server configuration directory, required\n");
    buffer
        .append("availability - \"available\" or \"unavailable\", optional default is \"available\"\n");
    buffer.append("====================================\n");
    return buffer.toString();
  }

  public DataProvider<?> getDataProvider() {
    return _core.getDataProvider();
  }

  public SenseiCore getSenseiCore() {
    return _core;
  }

  public void shutdown() {
    try {
      logger.info("shutting down node...");
      try {
        _core.shutdown();
        pluginRegistry.stop();
        server.leaveCluster(cluster);
        server.shutdown();
        cluster.shutdown();
        _core.getPluggableSearchEngineManager().close();
      } catch (Exception e) {
        logger.warn(e.getMessage());
      }
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void start(boolean available) throws Exception {
    _core.start();
    logger.info("Cluster Id: " + cluster.getClusterId());

    AbstractSenseiCoreService<SenseiRequest, SenseiResult> coreSenseiService = new CoreSenseiServiceImpl(
        _core, _senseiConf);
    AbstractSenseiCoreService<SenseiRequest, SenseiSystemInfo> sysSenseiCoreService = new SysSenseiCoreServiceImpl(
        _core, _senseiConf);
    SenseiCoreServiceMessageHandler<SenseiRequest, SenseiResult> senseiMsgHandler = new SenseiCoreServiceMessageHandler<SenseiRequest, SenseiResult>(
        coreSenseiService);
    SenseiCoreServiceMessageHandler<SenseiRequest, SenseiSystemInfo> senseiSysMsgHandler = new SenseiCoreServiceMessageHandler<SenseiRequest, SenseiSystemInfo>(
        sysSenseiCoreService);

    transportService.registerHandler(senseiMsgHandler);
    transportService.registerHandler(senseiSysMsgHandler);

    server.start();

    if (_externalSvc != null) {
      for (AbstractSenseiCoreService svc : _externalSvc) {
        transportService.registerHandler(new SenseiCoreServiceMessageHandler(svc));
      }
    }

    setAvailable(available);

    SenseiServerAdminMBean senseiAdminMBean = getAdminMBean();
    StandardMBean bean = new StandardMBean(senseiAdminMBean, SenseiServerAdminMBean.class);
    JmxUtil.registerMBean(bean, "name", "sensei-server-" + _id);
  }

  private Set<Integer> getPartitions() {
    Set<Integer> shards = new HashSet<Integer>();
    for (Integer partition : _partitions) {
      shards.add(partition);
    }
    return shards;
  }

  private String getLocalIpAddress() throws SocketException, UnknownHostException {
    String addr = NetUtil.getHostAddress();
    return String.format("%s:%d", addr, _port);
  }

  private SenseiServerAdminMBean getAdminMBean() {
    return new SenseiServerAdminMBean() {
      @Override
      public int getId() {
        return _id;
      }

      @Override
      public int getPort() {
        return _port;
      }

      @Override
      public String getPartitions() {
        StringBuffer sb = new StringBuffer();
        if (_partitions.length > 0) sb.append(String.valueOf(_partitions[0]));
        for (int i = 1; i < _partitions.length; i++) {
          sb.append(',');
          sb.append(String.valueOf(_partitions[i]));
        }
        return sb.toString();
      }

      @Override
      public boolean isAvailable() {
        return SenseiServer.this.isAvailable();
      }

      @Override
      public void setAvailable(boolean available) {
        SenseiServer.this.setAvailable(available);
      }
    };
  }

  public void setAvailable(boolean available) {
    String ipAddr;
    try {
      ipAddr = getLocalIpAddress();
    } catch (Exception e) {
      logger.warn("cannot get local IP address", e);
      ipAddr = "unknown";
    }

    if (available) {
      try {
        logger.info("making available node " + _id + " @port:" + _port + " IP address : " + ipAddr
            + " for partitions: " + Arrays.toString(_partitions));
        server.joinCluster(cluster, getPartitions());
        _available = true;
      } catch (Exception e) {
        logger.error("error joining cluster", e);
      }
    } else {
      logger.info("making unavailable node " + _id + " @port:" + _port + " IP address : " + ipAddr
          + " for partitions: " + Arrays.toString(_partitions));
      try {
        server.leaveCluster(cluster);
        _available = false;
      } catch (UpdateException e) {
        logger.error("error leaving cluster", e);
      }
    }
  }

  public boolean isAvailable() {
    return _available;
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.out.println(help());
      System.exit(1);
    }

    File confDir = null;

    try {
      confDir = new File(args[0]);
    } catch (Exception e) {
      System.out.println(help());
      System.exit(1);
    }

    boolean available = true;
    for (int i = 1; i < args.length; i++) {
      if (args[i] != null) {
        if (AVAILABLE.equalsIgnoreCase(args[i])) {
          available = true;
        }
        if (UNAVAILABLE.equalsIgnoreCase(args[i])) {
          available = false;
        }
      }
    }

    SenseiServerBuilder senseiServerBuilder = new SenseiServerBuilder(confDir, null);

    final SenseiServer server = senseiServerBuilder.buildServer();

    final Server jettyServer = senseiServerBuilder.buildHttpRestServer();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {

        try {
          server.setAvailable(false);
          jettyServer.stop();
        } catch (Exception e) {
          logger.error(e.getMessage(), e);
        } finally {
          server.shutdown();
        }
      }
    });

    server.start(available);
    jettyServer.start();
  }

}
