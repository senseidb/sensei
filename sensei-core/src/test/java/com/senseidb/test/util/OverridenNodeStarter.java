package com.senseidb.test.util;

import java.io.File;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.mortbay.jetty.Server;

import com.senseidb.conf.SenseiServerBuilder;
import com.senseidb.search.node.SenseiBroker;
import com.senseidb.search.node.SenseiServer;
import com.senseidb.search.node.broker.BrokerConfig;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.test.SenseiStarter;

public class OverridenNodeStarter {
  private boolean serverStarted = false;
  private Server jettyServer;
  private SenseiServer server;
  private SenseiBroker senseiBroker;
  private BrokerConfig brokerConfig;
  private PropertiesConfiguration senseiConfiguration;

  public void start(PropertiesConfiguration senseiConfiguration) {
    this.senseiConfiguration = senseiConfiguration;
    if (!serverStarted) {
      try {
        // rmrf(new File(indexDir));
        SenseiServerBuilder senseiServerBuilder = new SenseiServerBuilder(senseiConfiguration);
        server = senseiServerBuilder.buildServer();
        jettyServer = senseiServerBuilder.buildHttpRestServer();
        server.start(true);
        jettyServer.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
          @Override
          public void run() {
            if (serverStarted) {
              shutdown();
            }
          }
        });

        brokerConfig = new BrokerConfig(senseiConfiguration);
        brokerConfig.init(SenseiStarter.createZuCluster());
        senseiBroker = brokerConfig.buildSenseiBroker();

      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  public void waitTillServerStarts(int expectedDocs) throws Exception {
    int counter = 0;
    while (true) {
      SenseiResult senseiResult = senseiBroker.browse(new SenseiRequest());
      if (senseiBroker.isDisconnected()) {
        brokerConfig.shutdown();
        Thread.sleep(5000);
        brokerConfig.init(SenseiStarter.createZuCluster());
        senseiBroker = brokerConfig.buildSenseiBroker();
        System.out.println("Restarted the broker");
      }
      int totalDocs = senseiResult.getTotalDocs();
      System.out.println("TotalDocs = " + totalDocs);
      if (counter > 200) {
        throw new IllegalStateException("Wait timeout");
      }
      if (totalDocs >= expectedDocs) {
        break;
      }
      Thread.sleep(1000);
      counter++;
    }
  }

  public static boolean rmrf(File f) {
    if (f == null || !f.exists()) {
      return true;
    }
    if (f.isDirectory()) {
      for (File sub : f.listFiles()) {
        if (!rmrf(sub)) return false;
      }
    }
    return f.delete();
  }

  public boolean isServerStarted() {
    return serverStarted;
  }

  public void shutdown() {
    senseiBroker = null;
    try {
      server.setAvailable(false);
      server.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        serverStarted = false;
        jettyServer.stop();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public SenseiBroker getSenseiBroker() {
    return senseiBroker;
  }

  public PropertiesConfiguration getSenseiConfiguration() {
    return senseiConfiguration;
  }
}
