package com.senseidb.search.req.mapred.functions.groupby;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;

import com.senseidb.search.req.mapred.SingleFieldAccessor;
import com.senseidb.util.JSONUtil;

public class AggregateFunctionFactory {
  public static AggregateFunction<?> valueOf(String name, String column) {
    name = name.toLowerCase();
    if (name.endsWith("avg")) {
      return new AvgAggregationFunction(column);
    }
    if (name.endsWith("sum")) {
      return new SumAggregationFunction(column);
    }
    if (name.endsWith("count")) {
      return new CountAggregationFunction(column);
    }
    if (name.endsWith("max")) {
      return new MaxAggregationFunction(column);
    }
    if (name.endsWith("min")) {
      return new MinAggregationFunction(column);
    }
    return null;
  }

  public static List<String> sort(final Map<String, ? extends GroupedValue> reduceResult) {
    if (reduceResult == null) {
      return Collections.emptyList();
    }
    List<String> ret = new ArrayList<String>(reduceResult.keySet());
    Collections.sort(ret, new Comparator<String>() {

      @Override
      public int compare(String o1, String o2) {
        return reduceResult.get(o2).compareTo(reduceResult.get(o1));
      }
    });
    return ret;
  }

  public static class SumGroupedValue implements GroupedValue {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    long sum = 0;

    @Override
    public int compareTo(GroupedValue o) {
      long val = sum - ((SumGroupedValue) o).sum;
      if (val < 0) return -1;
      if (val == 0) return 0;
      return 1;
    }

    @Override
    public void merge(GroupedValue anotherValue) {
      sum += ((SumGroupedValue) anotherValue).sum;
    }

  }

  public static class SumAggregationFunction implements AggregateFunction<SumGroupedValue> {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    public SumAggregationFunction(String column) {
    }

    @Override
    public SumGroupedValue produceSingleValue(SingleFieldAccessor accessor, int docId) {
      SumGroupedValue ret = new SumGroupedValue();
      ret.sum = accessor.getLong(docId);
      return ret;
    }

    @Override
    public Object toJson(HashMap<String, SumGroupedValue> reduceResult) {
      try {
        JSONArray ret = new JSONUtil.FastJSONArray();
        for (String key : AggregateFunctionFactory.sort(reduceResult)) {
          SumGroupedValue value = reduceResult.get(key);
          ret.put(new JSONUtil.FastJSONObject().put("sum", value.sum).put("group", key));
        }
        return ret;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

  }

  public static class CountGroupedValue implements GroupedValue {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    long count = 0;

    @Override
    public int compareTo(GroupedValue o) {
      long val = count - ((CountGroupedValue) o).count;
      if (val < 0) return -1;
      if (val == 0) return 0;
      return 1;
    }

    @Override
    public void merge(GroupedValue anotherValue) {
      count += ((CountGroupedValue) anotherValue).count;
    }

    @Override
    public String toString() {
      return "CountGroupedValue [count=" + count + "]";
    }

  }

  public static class CountAggregationFunction implements AggregateFunction<CountGroupedValue> {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    public CountAggregationFunction(String column) {
    }

    @Override
    public CountGroupedValue produceSingleValue(SingleFieldAccessor accessor, int docId) {
      CountGroupedValue ret = new CountGroupedValue();
      ret.count = 1;
      return ret;
    }

    @Override
    public Object toJson(HashMap<String, CountGroupedValue> reduceResult) {
      try {
        JSONArray ret = new JSONUtil.FastJSONArray();
        for (String key : AggregateFunctionFactory.sort(reduceResult)) {
          CountGroupedValue value = reduceResult.get(key);
          ret.put(new JSONUtil.FastJSONObject().put("count", value.count).put("group", key));
        }
        return ret;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

  }

  public static class AvgAggregationFunction implements AggregateFunction<AvgGroupedValue> {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    public AvgAggregationFunction(String column) {
    }

    @Override
    public AvgGroupedValue produceSingleValue(SingleFieldAccessor accessor, int docId) {
      AvgGroupedValue ret = new AvgGroupedValue();
      ret.avg = accessor.getDouble(docId);
      ret.count = 1;
      return ret;
    }

    @Override
    public Object toJson(HashMap<String, AvgGroupedValue> reduceResult) {
      try {
        JSONArray ret = new JSONUtil.FastJSONArray();
        for (String key : AggregateFunctionFactory.sort(reduceResult)) {
          AvgGroupedValue value = reduceResult.get(key);
          ret.put(new JSONUtil.FastJSONObject().put("avg", String.format("%1.5f", value.avg)).put(
            "group", key));
        }
        return ret;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

  }

  public static class AvgGroupedValue implements GroupedValue {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    double avg = 0;
    int count = 0;

    @Override
    public int compareTo(GroupedValue o) {
      double val = avg - ((AvgGroupedValue) o).avg;
      if (val < 0) return -1;
      if (val == 0) return 0;
      return 1;
    }

    @Override
    public void merge(GroupedValue anotherValue) {
      AvgGroupedValue anValue = ((AvgGroupedValue) anotherValue);
      int newCount = count + anValue.count;
      avg = (avg * count + anValue.avg * anValue.count) / newCount;
      count = newCount;
    }

  }

  public static class MaxAggregationFunction implements AggregateFunction<MaxGroupedValue> {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    public MaxAggregationFunction(String column) {
    }

    @Override
    public MaxGroupedValue produceSingleValue(SingleFieldAccessor accessor, int docId) {
      MaxGroupedValue ret = new MaxGroupedValue();
      ret.max = accessor.getDouble(docId);
      ret.uid = docId;
      return ret;
    }

    @Override
    public Object toJson(HashMap<String, MaxGroupedValue> reduceResult) {
      try {
        JSONArray ret = new JSONUtil.FastJSONArray();
        for (String key : AggregateFunctionFactory.sort(reduceResult)) {
          MaxGroupedValue value = reduceResult.get(key);
          ret.put(new JSONUtil.FastJSONObject().put("max", String.format("%1.5f", value.max)).put(
            "group", key));
        }
        return ret;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

  }

  public static class MaxGroupedValue implements GroupedValue {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    double max = 0;
    long uid;

    @Override
    public int compareTo(GroupedValue o) {
      double val = max - ((MaxGroupedValue) o).max;
      if (val < 0) return -1;
      if (val == 0) return 0;
      return 1;
    }

    @Override
    public void merge(GroupedValue anotherValue) {
      MaxGroupedValue anValue = ((MaxGroupedValue) anotherValue);
      if (anValue.max > max) {
        max = anValue.max;
        uid = anValue.uid;
      }

    }

    @Override
    public String toString() {
      return "MaxGroupedValue [max=" + max + ", uid=" + uid + "]";
    }

  }

  public static class MinAggregationFunction implements AggregateFunction<MinGroupedValue> {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    public MinAggregationFunction(String column) {
    }

    @Override
    public MinGroupedValue produceSingleValue(SingleFieldAccessor accessor, int docId) {
      MinGroupedValue ret = new MinGroupedValue();
      ret.min = accessor.getDouble(docId);
      ret.uid = docId;
      return ret;
    }

    @Override
    public Object toJson(HashMap<String, MinGroupedValue> reduceResult) {
      try {
        JSONArray ret = new JSONUtil.FastJSONArray();
        for (String key : AggregateFunctionFactory.sort(reduceResult)) {
          MinGroupedValue value = reduceResult.get(key);
          ret.put(new JSONUtil.FastJSONObject().put("min", String.format("%1.5f", value.min)).put(
            "group", key));
        }
        return ret;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

  }

  public static class MinGroupedValue implements GroupedValue {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    double min = 0;
    long uid;

    @Override
    public int compareTo(GroupedValue o) {
      double val = min - ((MinGroupedValue) o).min;
      val = val * -1;
      if (val < 0) return -1;
      if (val == 0) return 0;
      return 1;
    }

    @Override
    public void merge(GroupedValue anotherValue) {
      MinGroupedValue anValue = ((MinGroupedValue) anotherValue);

      if (anValue.min < min) {
        min = anValue.min;
        uid = anValue.uid;
      }

    }

    @Override
    public String toString() {
      return "MinGroupedValue [min=" + min + ", uid=" + uid + "]";
    }

  }
}
