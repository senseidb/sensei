package com.senseidb.test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Ignore;

import com.senseidb.search.req.ErrorType;
import com.senseidb.search.req.mapred.CombinerStage;
import com.senseidb.search.req.mapred.FacetCountAccessor;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.search.req.mapred.IntArray;
import com.senseidb.search.req.mapred.SenseiMapReduce;

public class ErrorHandlingTest extends TestCase {

  public static class MapReduceAdapter implements SenseiMapReduce<Serializable, Serializable> {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    @Override
    public void init(JSONObject params) {
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Serializable map(IntArray docIds, int docIdCount, long[] uids, FieldAccessor accessor,
        FacetCountAccessor facetCountAccessor) {
      return new ArrayList();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public List<Serializable> combine(List<Serializable> mapResults, CombinerStage combinerStage) {
      return new ArrayList();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Serializable reduce(List<Serializable> combineResults) {
      return new ArrayList();
    }

    @Override
    public JSONObject render(Serializable reduceResult) {
      return new JSONObject();
    }
  }

  public static class test1JsonError extends MapReduceAdapter {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    @Override
    public void init(JSONObject params) {
      throw new RuntimeException("JsonException", new JSONException("JsonException"));
    }
  }

  public static class test2BoboError extends MapReduceAdapter {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    @Override
    public Serializable map(IntArray docIds, int docIdCount, long[] uids, FieldAccessor accessor,
        FacetCountAccessor facetCountAccessor) {
      throw new RuntimeException("Map exception");
    }
  }

  public static class test3PartitionLevelError extends MapReduceAdapter {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    @Override
    public List<Serializable> combine(List<Serializable> mapResults, CombinerStage combinerStage) {
      if (combinerStage == CombinerStage.partitionLevel) {
        throw new RuntimeException("partition combiner exception");
      }
      return super.combine(mapResults, combinerStage);
    }
  }

  public static class test4NodeLevelError extends MapReduceAdapter {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    @Override
    public List<Serializable> combine(List<Serializable> mapResults, CombinerStage combinerStage) {
      if (combinerStage == CombinerStage.nodeLevel) {
        throw new RuntimeException("node combiner exception");
      }
      return super.combine(mapResults, combinerStage);
    }
  }

  public static class test5BrokerLevelError extends MapReduceAdapter {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    @Override
    public Serializable reduce(List<Serializable> combineResults) {
      throw new RuntimeException("The exception on broker level");
    }
  }

  public static class test6NonSerializableError extends MapReduceAdapter {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public static class NonSerializable implements Serializable {
      /**
       *
       */
      private static final long serialVersionUID = 1L;
    }

    @Override
    public List<Serializable> combine(List<Serializable> mapResults, CombinerStage combinerStage) {

      return new ArrayList<Serializable>(java.util.Arrays.asList(new NonSerializable()));
    }
  }

  public static class test7ResponseJsonError extends MapReduceAdapter {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    @Override
    public JSONObject render(Serializable reduceResult) {
      throw new RuntimeException(new JSONException("renderError"));
    }
  }

  static {
    SenseiStarter.start("test-conf/node1", "test-conf/node2");
  }

  public void test1ExceptionOInitLevel() throws Exception {
    String req = "{ \"mapReduce\":{\"function\":\"" + test1JsonError.class.getName() + "\"}}";

    JSONObject reqJson = new JSONObject(req);
    System.out.println(reqJson.toString(1));
    JSONObject res = TestSensei.search(reqJson);
    assertEquals(ErrorType.JsonParsingError.getDefaultErrorCode(), res.getInt("errorCode"));
    assertResponseContainsErrors(res, ErrorType.JsonParsingError);
  }

  public void test2BoboError() throws Exception {
    String req = "{ \"mapReduce\":{\"function\":\"" + test2BoboError.class.getName() + "\"}}";

    JSONObject reqJson = new JSONObject(req);
    System.out.println(reqJson.toString(1));
    JSONObject res = TestSensei.search(reqJson);
    assertEquals(ErrorType.BoboExecutionError.getDefaultErrorCode(), res.getInt("errorCode"));
    assertResponseContainsErrors(res, ErrorType.BoboExecutionError, ErrorType.BoboExecutionError,
      ErrorType.BoboExecutionError);
  }

  public void test3PartitionLevelError() throws Exception {
    String req = "{ \"mapReduce\":{\"function\":\"" + test3PartitionLevelError.class.getName()
        + "\"}}";

    JSONObject reqJson = new JSONObject(req);
    System.out.println(reqJson.toString(1));
    JSONObject res = TestSensei.search(reqJson);
    assertEquals(ErrorType.BoboExecutionError.getDefaultErrorCode(), res.getInt("errorCode"));
    assertResponseContainsErrors(res, ErrorType.BoboExecutionError, ErrorType.BoboExecutionError,
      ErrorType.BoboExecutionError);
  }

  public void test4NodeLevelError() throws Exception {
    String req = "{ \"mapReduce\":{\"function\":\"" + test4NodeLevelError.class.getName() + "\"}}";

    JSONObject reqJson = new JSONObject(req);
    System.out.println(reqJson.toString(1));
    JSONObject res = TestSensei.search(reqJson);
    assertEquals(ErrorType.MergePartitionError.getDefaultErrorCode(), res.getInt("errorCode"));
    assertResponseContainsErrors(res, ErrorType.MergePartitionError, ErrorType.MergePartitionError);
  }

  public void test5BrokerLevelError() throws Exception {
    String req = "{ \"mapReduce\":{\"function\":\"" + test5BrokerLevelError.class.getName()
        + "\"}}";

    JSONObject reqJson = new JSONObject(req);
    System.out.println(reqJson.toString(1));
    JSONObject res = TestSensei.search(reqJson);
    assertEquals(ErrorType.BrokerGatherError.getDefaultErrorCode(), res.getInt("errorCode"));
    assertResponseContainsErrors(res, ErrorType.BrokerGatherError);
  }

  @Ignore
  public void ntest6NonSerializableError() throws Exception {
    String req = "{ \"mapReduce\":{\"function\":\"" + test6NonSerializableError.class.getName()
        + "\"}}";

    JSONObject reqJson = new JSONObject(req);
    System.out.println(reqJson.toString(1));
    JSONObject res = TestSensei.search(reqJson);
    assertEquals(ErrorType.BrokerGatherError.getDefaultErrorCode(), res.getInt("errorCode"));
    assertResponseContainsErrors(res, ErrorType.BrokerGatherError);
  }

  public void test7ResponseJsonError() throws Exception {
    String req = "{ \"mapReduce\":{\"function\":\"" + test7ResponseJsonError.class.getName()
        + "\"}}";

    JSONObject reqJson = new JSONObject(req);
    System.out.println(reqJson.toString(1));
    JSONObject res = TestSensei.search(reqJson);
    assertEquals(ErrorType.JsonParsingError.getDefaultErrorCode(), res.getInt("errorCode"));
    assertResponseContainsErrors(res, ErrorType.JsonParsingError);
  }

  public void test8BQLError() throws Exception {
    String req = "{\"bql\":\"select1 * from cars\"}";

    JSONObject reqJson = new JSONObject(req);
    System.out.println(reqJson.toString(1));
    JSONObject res = TestSensei.search(reqJson);
    assertEquals(ErrorType.BQLParsingError.getDefaultErrorCode(), res.getInt("errorCode"));
    assertResponseContainsErrors(res, ErrorType.BQLParsingError);
  }

  public void test9BQLError() throws Exception {
    String req = "{\"bql\":\"SELECT category FROM cars WHERE color = 'red' AND year > 2000 AND year < 1995 OR price < 1750.00\"}";

    JSONObject reqJson = new JSONObject(req);
    System.out.println(reqJson.toString(1));
    JSONObject res = TestSensei.search(reqJson);
    assertEquals(ErrorType.BQLParsingError.getDefaultErrorCode(), res.getInt("errorCode"));
    assertResponseContainsErrors(res, ErrorType.BQLParsingError);
  }

  private void assertResponseContainsErrors(JSONObject res, ErrorType... jsonParsingErrors)
      throws JSONException {
    for (int i = 0; i < jsonParsingErrors.length; i++) {
      assertEquals(jsonParsingErrors[i].name(),
        res.getJSONArray("errors").getJSONObject(i).get("errorType"));
    }
    assertEquals(jsonParsingErrors.length, res.getJSONArray("errors").length());
  }

}
