package com.senseidb.util;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.mortbay.jetty.Server;

import com.senseidb.conf.SenseiServerBuilder;
import com.senseidb.search.node.SenseiBroker;
import com.senseidb.search.node.SenseiServer;
import com.senseidb.search.node.broker.BrokerConfig;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.svc.api.SenseiException;

public class SingleNodeStarter {
  private static boolean serverStarted = false;
  private static Server jettyServer;
  private static SenseiServer server;
  private static SenseiBroker senseiBroker;
  private static BrokerConfig brokerConfig;

  public static void start(String localPath, int expectedDocs) {
    start(new File(getUri(localPath)), expectedDocs);
  }

  public static void start(File confDir, int expectedDocs) {
    if (!serverStarted) {
      try {
        PropertiesConfiguration senseiConfiguration = new PropertiesConfiguration(new File(confDir,
            "sensei.properties"));
        // rmrf(new File(indexDir));
        SenseiServerBuilder senseiServerBuilder = new SenseiServerBuilder(confDir, null);
        server = senseiServerBuilder.buildServer();
        jettyServer = senseiServerBuilder.buildHttpRestServer();
        server.start(true);
        jettyServer.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
          @Override
          public void run() {
            shutdown();
          }
        });
        brokerConfig = new BrokerConfig(senseiConfiguration);
        brokerConfig.init(null);
        senseiBroker = brokerConfig.buildSenseiBroker();
        waitTillServerStarts(expectedDocs);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  public static void waitTillServerStarts(int expectedDocs) throws SenseiException,
      InterruptedException {
    int counter = 0;
    while (true) {
      SenseiResult senseiResult = senseiBroker.browse(new SenseiRequest());
      if (senseiBroker.isDisconnected()) {
        brokerConfig.shutdown();
        Thread.sleep(5000);
        brokerConfig.init(null);
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

  public static boolean isServerStarted() {
    return serverStarted;
  }

  private static URI getUri(String localPath) {
    try {
      return SingleNodeStarter.class.getClassLoader().getResource(localPath).toURI();
    } catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static void shutdown() {
    try {
      jettyServer.stop();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      server.shutdown();
      serverStarted = false;
      try {
        senseiBroker.shutdown();
      } finally {
        senseiBroker = null;
        try {
          brokerConfig.shutdown();
        } finally {
          brokerConfig = null;
        }
      }
    }
  }
}
