package com.senseidb.search.req.mapred;

import junit.framework.TestCase;

import org.json.JSONObject;

import com.senseidb.test.SenseiStarter;
import com.senseidb.test.TestSensei;

public class TestMapReduce extends TestCase {

  static {
    SenseiStarter.start("test-conf/node1", "test-conf/node2");
  }

  public void test2GroupByColorAndGroupId() throws Exception {
    String req = "{\"size\":0,\"filter\":{\"terms\":{\"color\":{\"includes\":[],\"excludes\":[\"gold\"],\"operator\":\"or\"}}}"
        + ", \"mapReduce\":{\"function\":\"com.senseidb.search.req.mapred.CountGroupByMapReduce\",\"parameters\":{\"columns\":[\"groupid\", \"color\"]}}}";
    JSONObject reqJson = new JSONObject(req);
    System.out.println(reqJson.toString(1));
    JSONObject res = TestSensei.search(reqJson);

    JSONObject highestResult = res.getJSONObject("mapReduceResult").getJSONArray("groupedCounts")
        .getJSONObject(0);
    assertEquals(8, highestResult.getInt(highestResult.keys().next().toString()));
  }

  public void test4MaxMapReduce() throws Exception {
    String req = "{\"filter\":{\"term\":{\"color\":\"red\"}}"
        + ", \"mapReduce\":{\"function\":\"sensei.max\",\"parameters\":{\"column\":\"groupid\"}}}";
    JSONObject res = TestSensei.search(new JSONObject(req));
    JSONObject mapReduceResult = res.getJSONObject("mapReduceResult");
    assertEquals(14990, Double.valueOf((Double.parseDouble(mapReduceResult.getString("max"))))
        .longValue());
    assertEquals(14994, Long.parseLong(mapReduceResult.getString("uid")));
    req = "{\"filter\":{\"term\":{\"color\":\"red\"}}"
        + ",\"selections\":[{\"terms\":{\"groupid\":{\"excludes\":[14990],\"operator\":\"or\"}}}]"
        + ", \"mapReduce\":{\"function\":\"sensei.max\",\"parameters\":{\"column\":\"groupid\"}}}";
    res = TestSensei.search(new JSONObject(req));
    mapReduceResult = res.getJSONObject("mapReduceResult");
    assertEquals(14980, Double.valueOf((Double.parseDouble(mapReduceResult.getString("max"))))
        .longValue());
    // assertEquals(14989, Long.parseLong(mapReduceResult.getString("uid")));

  }

  public void test5DistinctCount() throws Exception {
    String req = "{\"filter\":{\"term\":{\"color\":\"red\"}}"
        + ", \"mapReduce\":{\"function\":\"sensei.distinctCount\",\"parameters\":{\"column\":\"groupid\"}}}";
    JSONObject res = TestSensei.search(new JSONObject(req));
    JSONObject mapReduceResult = res.getJSONObject("mapReduceResult");
    assertEquals(964, Long.parseLong(mapReduceResult.getString("distinctCount")));

  }

  public void test6MinMapReduce() throws Exception {
    String req = "{\"filter\":{\"term\":{\"tags\":\"reliable\"}}"
        + ", \"mapReduce\":{\"function\":\"sensei.min\",\"parameters\":{\"column\":\"groupid\"}}}";
    JSONObject res = TestSensei.search(new JSONObject(req));
    JSONObject mapReduceResult = res.getJSONObject("mapReduceResult");
    assertEquals(-15000L, Double.valueOf((Double.parseDouble(mapReduceResult.getString("min"))))
        .longValue());
    assertEquals(0L, Long.parseLong(mapReduceResult.getString("uid")));
    req = "{\"filter\":{\"term\":{\"tags\":\"reliable\"}}"
        + ", \"mapReduce\":{\"function\":\"sensei.min\",\"parameters\":{\"column\":\"year\"}}}";
    res = TestSensei.search(new JSONObject(req));
    mapReduceResult = res.getJSONObject("mapReduceResult");
    assertEquals(1993L, Double.valueOf((Double.parseDouble(mapReduceResult.getString("min"))))
        .longValue());
  }

  public void test7SumMapReduce() throws Exception {
    String req = "{\"filter\":{\"term\":{\"color\":\"red\"}}, "
        + " \"mapReduce\":{\"function\":\"sensei.sum\",\"parameters\":{\"column\":\"groupid\"}}}";
    JSONObject res = TestSensei.search(new JSONObject(req));
    JSONObject mapReduceResult = res.getJSONObject("mapReduceResult");
    assertEquals(16036500, mapReduceResult.getLong("sum"));
  }

  public void test8AvgMapReduce() throws Exception {
    String req = "{\"filter\":{\"term\":{\"color\":\"red\"}}, "
        + " \"mapReduce\":{\"function\":\"sensei.avg\",\"parameters\":{\"column\":\"groupid\"}}}";
    JSONObject res = TestSensei.search(new JSONObject(req));
    JSONObject mapReduceResult = res.getJSONObject("mapReduceResult");
    assertEquals(7424, mapReduceResult.getLong("avg"));
    assertEquals(2160, Long.parseLong(mapReduceResult.getString("count")));
  }

  public void test9FacetCountMapReduce() throws Exception {
    String req = "{\"facets\": {\"color\": {\"max\": 10, \"minCount\": 1, \"expand\": false, \"order\": \"hits\"}}"
        + ", \"mapReduce\":{\"function\":\"com.senseidb.search.req.mapred.FacetCountsMapReduce\",\"parameters\":{\"column\":\"color\"}}}";
    JSONObject res = TestSensei.search(new JSONObject(req));
    JSONObject mapReduceResult = res.getJSONObject("mapReduceResult");
    System.out.println(mapReduceResult.toString(1));
    assertEquals(3141, mapReduceResult.getJSONObject("facetCounts").getInt("black"));
    assertEquals(2196, mapReduceResult.getJSONObject("facetCounts").getInt("white"));

  }

  public void test10FacetCountMapReduceWithFilter() throws Exception {
    String req = "{\"facets\": {\"color\": {\"max\": 10, \"minCount\": 1, \"expand\": false, \"order\": \"hits\"}}"
        + ", \"mapReduce\":{\"function\":\"com.senseidb.search.req.mapred.FacetCountsMapReduce\",\"parameters\":{\"column\":\"color\"}}, "
        + "\"filter\":{\"term\":{\"tags\":\"reliable\"}}}";
    JSONObject res = TestSensei.search(new JSONObject(req));
    JSONObject mapReduceResult = res.getJSONObject("mapReduceResult");
    System.out.println(mapReduceResult.toString(1));
    assertEquals(2259, mapReduceResult.getJSONObject("facetCounts").getInt("black"));
    assertEquals(1560, mapReduceResult.getJSONObject("facetCounts").getInt("white"));

  }

  public void test11SumMapReduceBQL() throws Exception {
    String req = "{\"bql\":\"SELECT sum(year) FROM cars WHERE color = 'red'\"}";
    JSONObject res = TestSensei.search(new JSONObject(req));
    JSONObject mapReduceResult = res.getJSONObject("mapReduceResult");
    assertEquals(4314485, mapReduceResult.getLong("sum"));

  }

  public void test12AvgMapReduceBQL() throws Exception {
    String req = "{\"bql\":\"SELECT avg(price), avg(year) FROM cars WHERE color = 'red'\"}";
    JSONObject res = TestSensei.search(new JSONObject(req));
    System.out.println(res.toString(1));
    JSONObject mapReduceResult = res.getJSONObject("mapReduceResult");

    assertEquals(
      1997.446,
      mapReduceResult.getJSONArray("results").getJSONObject(1).getJSONObject("result")
          .getDouble("avg"), 0.01);

  }

  public void test13CountMapReduceBQLWithGroupBy() throws Exception {
    String req = "{\"bql\":\"SELECT  sum(year), sum(price) FROM cars WHERE color = 'red' GROUP BY category top 5 LIMIT 0\"}";
    JSONObject res = TestSensei.search(new JSONObject(req));
    System.out.println("!!top5=" + res.toString(1));
    JSONObject mapReduceResult = res.getJSONObject("mapReduceResult");

    JSONObject firstObject = mapReduceResult.getJSONArray("grouped").getJSONObject(0);
    assertEquals("exotic", firstObject.getString("group"));
    assertEquals(8040600, firstObject.getLong("sum"));
  }

  public void test14CountMapReduceBQLWithMultipleGroupBy() throws Exception {
    String req = "{\"bql\":\"SELECT color, sum(year), sum(price),count(*)  FROM cars WHERE color = 'red' GROUP BY category, color limit 10\"}";
    JSONObject res = TestSensei.search(new JSONObject(req));
    System.out.println(res.toString(1));
    JSONObject mapReduceResult = res.getJSONObject("mapReduceResult");
    assertEquals(10,
      mapReduceResult.getJSONArray("results").getJSONObject(1).getJSONObject("result")
          .getJSONArray("grouped").length());
  }

  public void test14MapReduce() throws Exception {
    String req = "{\"bql\":\"SELECT * FROM cars WHERE color <> 'gold' EXECUTE(com.senseidb.search.req.mapred.CountGroupByMapReduce, {'columns':['groupid', 'color']})\"}";
    JSONObject res = TestSensei.search(new JSONObject(req));

    JSONObject highestResult = res.getJSONObject("mapReduceResult").getJSONArray("groupedCounts")
        .getJSONObject(0);
    assertEquals(8, highestResult.getInt(highestResult.keys().next().toString()));
  }

  public void test15MinGroupByReduce() throws Exception {
    String req = "{\"bql\":\"SELECT min(groupid) group by groupid \"}";
    JSONObject res = TestSensei.search(new JSONObject(req));
    System.out.println(res.toString(1));
    JSONObject mapReduceResult = res.getJSONObject("mapReduceResult");
    JSONObject firstObject = mapReduceResult.getJSONArray("grouped").getJSONObject(0);
    assertEquals("-00000000000000015000", firstObject.getString("group"));
    assertEquals(-15000, firstObject.getLong("min"));
  }

  public void test16SumGroupByReduce() throws Exception {
    String req = "{\"bql\":\"SELECT sum(groupid), sum(groupid) group by groupid \"}";
    JSONObject res = TestSensei.search(new JSONObject(req));
    System.out.println(res.toString(1));
    JSONObject mapReduceResult = res.getJSONObject("mapReduceResult");
    JSONObject firstObject = mapReduceResult.getJSONArray("grouped").getJSONObject(0);
    assertEquals("00000000000000014990", firstObject.getString("group"));
    assertEquals(149900, firstObject.getLong("sum"));
  }

  public void test16MaxGroupByReduce() throws Exception {
    String req = "{\"bql\":\"SELECT max(groupid) group by groupid limit 0\"}";
    JSONObject res = TestSensei.search(new JSONObject(req));
    System.out.println(res.toString(1));
    JSONObject mapReduceResult = res.getJSONObject("mapReduceResult");
    JSONObject firstObject = mapReduceResult.getJSONArray("grouped").getJSONObject(0);
    assertEquals("00000000000000014990", firstObject.getString("group"));
    assertEquals(14990, firstObject.getLong("max"));
  }

  public void test17CountByMultiColumn() throws Exception {
    // we shouldn't get an error here
    String req = "{\"bql\":\"SELECT count(groupid) group by tags limit 1\"}";
    JSONObject res = TestSensei.search(new JSONObject(req));
    System.out.println(res.toString(1));
    assertEquals(1, res.getJSONArray("hits").length());
    assertEquals(0, res.getJSONArray("errors").length());
  }

}
