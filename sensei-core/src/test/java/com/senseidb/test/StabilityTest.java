package com.senseidb.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.junit.Ignore;

import com.senseidb.test.util.OverridenNodeStarter;

@Ignore
public class StabilityTest extends TestCase {
  private final List<OverridenNodeStarter> nodes = new ArrayList<OverridenNodeStarter>();

  public static class ConfigHolder {
    String nodeId;
    String serverPort;
    String indexDirectory;
    String brokerPort;
    String clientName;

    public ConfigHolder(String nodeId, String serverPort, String indexDirectory, String brokerPort,
        String clientName) {
      super();
      this.nodeId = nodeId;
      this.serverPort = serverPort;
      this.indexDirectory = indexDirectory;
      this.brokerPort = brokerPort;
      this.clientName = clientName;
    }

    public void enhance(PropertiesConfiguration configuration) {
      configuration.setProperty("sensei.node.id", nodeId);
      configuration.setProperty("sensei.server.port", serverPort);
      configuration.setProperty("sensei.broker.port", brokerPort);
      configuration.setProperty("sensei.index.directory", indexDirectory);
      configuration.setProperty("sensei.search.cluster.client-name", clientName);
    }
  }

  @Override
  protected void setUp() throws Exception {
    File confDir1 = new File(SenseiStarter.class.getClassLoader().getResource("test-conf/node1")
        .toURI());
    File confDir2 = new File(SenseiStarter.class.getClassLoader().getResource("test-conf/node2")
        .toURI());
    for (int i = 0; i < 10; i++) {
      PropertiesConfiguration senseiConf = new PropertiesConfiguration();
      senseiConf.setDelimiterParsingDisabled(true);
      if (i % 2 == 0) {
        senseiConf.load(new File(confDir1, "sensei.properties"));
      } else {
        senseiConf.load(new File(confDir2, "sensei.properties"));
      }
      new ConfigHolder("" + i, String.valueOf(1228 + i), "sensei-index-test" + i,
          String.valueOf(11000 + i), "clientName").enhance(senseiConf);
      FileUtils.deleteDirectory(new File("sensei-index-test" + i));
      OverridenNodeStarter overridenNodeStarter = new OverridenNodeStarter();
      overridenNodeStarter.start(senseiConf);
      // overridenNodeStarter.waitTillServerStarts(10000);
      nodes.add(overridenNodeStarter);
    }
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    for (OverridenNodeStarter nodeStarter : nodes) {
      nodeStarter.shutdown();
    }
  }

  public void test1EqualLoad() throws Exception {
    List<Thread> threads = new ArrayList<Thread>();
    for (int i = 0; i < 10; i++) {
      Thread thread = new Thread() {
        @Override
        public void run() {
          while (true) {
            try {
              String req = "{  \"bql\": \"select * FROM sensei WHERE groupid >= 1 AND groupid<= 1343700000000 ORDER BY groupid ASC limit 0, 0\"}";
              JSONObject res = search(new JSONObject(req));
              System.out.println(" Got the reposnse with time - " + (res.getInt("time"))
                  + "and numHits = " + res.getInt("numhits"));
              if (res.getJSONArray("errors").length() > 0) {
                System.out.println(res.toString(1));
              }
            } catch (Exception ex) {
              ex.printStackTrace();
            }
          }
        }
      };
      threads.add(thread);
      thread.start();
    }
    Thread.sleep(Long.MAX_VALUE);
  }

  private volatile int currentlyShutdownServer = 0;

  public void test2ShutdownNodesOneByOne() throws Exception {
    new Thread() {
      @Override
      public void run() {
        while (true) {
          for (int i = 0; i < 10; i++) {
            try {
              currentlyShutdownServer = i;
              System.out.println("!!!Shutting down server - " + i);
              OverridenNodeStarter nodeStarter = nodes.get(i);
              Thread.sleep(500);
              PropertiesConfiguration senseiConfiguration = nodeStarter.getSenseiConfiguration();
              nodeStarter.shutdown();
              nodeStarter.start(senseiConfiguration);
              System.out.println("!!!The server has been restarted - " + i);
            } catch (Exception ex) {
              ex.printStackTrace();
            }
          }

        }
      }
    }.start();

    test1EqualLoad();
  }

  public JSONObject search(JSONObject req) throws Exception {
    // the node that is currently not in the shutdown state
    return search(
      new URL("http://localhost:"
          + String.valueOf(11000 + ((currentlyShutdownServer + 5) % 10) + "/sensei")),
      req.toString());
  }

  public JSONObject search(URL url, String req) throws Exception {
    URLConnection conn = url.openConnection();
    conn.setDoOutput(true);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream(),
        "UTF-8"));
    String reqStr = req;
    System.out.println("req: " + reqStr);
    writer.write(reqStr, 0, reqStr.length());
    writer.flush();
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(conn.getInputStream(), "UTF-8"));
    StringBuilder sb = new StringBuilder();
    String line = null;
    while ((line = reader.readLine()) != null)
      sb.append(line);
    String res = sb.toString();
    // System.out.println("res: " + res);
    res = res.replace('\u0000', '*'); // replace the seperator for test case;
    JSONObject ret = new JSONObject(res);
    if (ret.opt("totaldocs") != null) {
      // assertEquals(15000L, ret.getLong("totaldocs"));
    }
    writer.close();
    reader.close();
    return ret;
  }
}
