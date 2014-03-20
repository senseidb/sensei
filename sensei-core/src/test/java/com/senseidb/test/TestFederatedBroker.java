package com.senseidb.test;

import junit.framework.TestCase;

import org.json.JSONObject;
import org.junit.Ignore;

@Ignore
public class TestFederatedBroker extends TestCase {

  static {
    SenseiStarter.start("test-conf/node1", "test-conf/node2");
  }

  public void test1OneClusterIsEnough() throws Exception {
    String req = "{\"size\":10, \"query\":{\"path\":{\"makemodel\":\"asian/acura/3.2tl\"}}, \"selections\":[{\"range\":{\"year\":{\"to\":\"2002\",\"include_lower\":true,\"include_upper\":false,\"from\":\"2000\"}}}]}";
    JSONObject reqJson = new JSONObject(req);
    JSONObject res = TestSensei.search(reqJson);
    assertEquals("numhits is wrong", 24, res.getInt("numhits"));
    res = TestSensei.search(SenseiStarter.federatedBrokerUrl, reqJson.toString());
    assertEquals("numhits is wrong", 24, res.getInt("numhits"));
  }

  public void test2TwoClusters() throws Exception {
    String req = "{\"size\":40, \"query\":{\"path\":{\"makemodel\":\"asian/acura/3.2tl\"}}, \"selections\":[{\"range\":{\"year\":{\"to\":\"2002\",\"include_lower\":true,\"include_upper\":false,\"from\":\"2000\"}}}]}";
    JSONObject reqJson = new JSONObject(req);
    JSONObject res = TestSensei.search(reqJson);
    assertEquals("numhits is wrong", 24, res.getInt("numhits"));
    res = TestSensei.search(SenseiStarter.federatedBrokerUrl, reqJson.toString());
    assertEquals("numhits is wrong", 48, res.getInt("numhits"));
    assertEquals("hits are wrong", 40, res.getJSONArray("hits").length());
  }

}
