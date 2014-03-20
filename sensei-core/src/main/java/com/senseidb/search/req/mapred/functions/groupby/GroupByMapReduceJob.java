package com.senseidb.search.req.mapred.functions.groupby;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.browseengine.bobo.api.BoboSegmentReader;
import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.search.req.mapred.CombinerStage;
import com.senseidb.search.req.mapred.FacetCountAccessor;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.search.req.mapred.IntArray;
import com.senseidb.search.req.mapred.SenseiMapReduce;
import com.senseidb.search.req.mapred.SingleFieldAccessor;
import com.senseidb.util.JSONUtil;

class MapResult implements Serializable {
  /**
   *
   */
  private static final long serialVersionUID = 1L;

  @SuppressWarnings("rawtypes")
  public MapResult(int initialCapacity, TermValueList[] dictionaries, BoboSegmentReader indexReader) {
    this.dictionaries = dictionaries;
    this.indexReader = indexReader;
    results = new Long2ObjectOpenHashMap<GroupedValue>(initialCapacity);
  }

  public Long2ObjectOpenHashMap<GroupedValue> results;
  public TermValueList<?>[] dictionaries;
  public BoboSegmentReader indexReader;

  @Override
  public String toString() {
    return "MapResult [results=" + results + ", dictionaries="
        + java.util.Arrays.toString(dictionaries) + "]";
  }

}

public class GroupByMapReduceJob implements
    SenseiMapReduce<Serializable, HashMap<String, GroupedValue>> {

  /**
   *
   */
  private static final long serialVersionUID = 1L;
  public static final int TRIM_SIZE = 200;
  private String[] columns;
  private String metric;
  private String function;
  @SuppressWarnings("rawtypes")
  private AggregateFunction aggregateFunction;
  private int top = 10;

  @Override
  public void init(JSONObject params) {
    try {
      metric = params.getString("metric");
      function = params.getString("function");
      aggregateFunction = AggregateFunctionFactory.valueOf(function, metric);
      JSONArray columnsJson = params.getJSONArray("columns");
      columns = new String[columnsJson.length()];
      top = params.optInt("top", 10);
      for (int i = 0; i < columnsJson.length(); i++) {
        columns[i] = columnsJson.getString(i);
      }
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }

  }

  @Override
  public Serializable map(IntArray docIds, int docIdCount, long[] uids, FieldAccessor accessor,
      FacetCountAccessor facetCountsAccessor) {
    SingleFieldAccessor singleFieldAccessor = "count".equalsIgnoreCase(function) ? null : accessor
        .getSingleFieldAccessor(metric);

    TermValueList<?>[] dictionaries = new TermValueList[columns.length];
    for (int i = 0; i < columns.length; i++) {
      dictionaries[i] = accessor.getTermValueList(columns[i]);
    }
    MapResult mapResult = new MapResult(TRIM_SIZE, dictionaries, accessor.getBoboIndexReader());

    SingleFieldAccessor[] orders = new SingleFieldAccessor[columns.length];
    for (int i = 0; i < columns.length; i++) {
      orders[i] = accessor.getSingleFieldAccessor(columns[i]);
    }
    int[] numBits = new int[columns.length];
    int totalBitSet = 0;
    for (int i = 0; i < columns.length; i++) {
      numBits[i] = BitHacks.findLogBase2(dictionaries[i].size()) + 1;
      totalBitSet += numBits[i];
    }
    if (totalBitSet > 64) {
      throw new IllegalArgumentException("Too many columns for an efficient group by");
    }
    for (int i = 0; i < docIdCount; i++) {
      long key = getKey(dictionaries, orders, numBits, docIds.get(i));

      GroupedValue value = mapResult.results.get(key);

      if (value != null) {
        value.merge(aggregateFunction.produceSingleValue(singleFieldAccessor, docIds.get(i)));
      } else {
        mapResult.results.put(key,
          aggregateFunction.produceSingleValue(singleFieldAccessor, docIds.get(i)));
      }
    }

    if (mapResult.results.size() > Math.max(TRIM_SIZE, top) * 20) {
      trimToSize(mapResult.results, Math.max(TRIM_SIZE, top) * 5);
    }

    return mapResult;
  }

  private long getKey(TermValueList<?>[] dictionaries, SingleFieldAccessor[] orders, int[] numBits,
      int docId) {
    long ret = 0L;
    int i = 0;
    // StringBuilder b = new StringBuilder();
    while (true) {
      if (i >= numBits.length) {
        break;
      }
      ret = ret << numBits[i];
      ret |= orders[i].getDictionaryId(docId);
      // b.append(dictionaries[i].get(orders[i].get(docId)));
      if (i >= numBits.length - 1) {
        break;
      }

      i++;
    }
    // System.out.println(b.toString() + "\n" + decodeKey(new String[dictionaries.length],
    // dictionaries, numBits, ret));
    return ret;
  }

  private String decodeKey(String[] str, TermValueList<?>[] dictionaries, int[] numBits, long key) {
    int i = numBits.length - 1;
    while (i >= 0) {
      long number = key & (-1L >>> (64 - numBits[i]));
      str[i] = dictionaries[i].get((int) number);
      key >>>= numBits[i];

      i--;
    }

    StringBuilder builder = new StringBuilder();
    for (int j = 0; j < str.length - 1; j++) {
      builder.append(str[j]).append(":");
    }
    builder.append(str[str.length - 1]);
    return builder.toString();
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<Serializable> combine(List<Serializable> mapResults, CombinerStage combinerStage) {
    // System.out.println("Combine - " + mapResults);

    /*
     * if (combinerStage == CombinerStage.partitionLevel) { if (map == null) { return
     * Collections.EMPTY_LIST; } trimToSize(map, TRIM_SIZE * 5); List<HashMap<String, GroupedValue>>
     * ret = java.util.Arrays.asList(map); map = new HashMap<String, GroupedValue>(); return ret; }
     */
    if (combinerStage == CombinerStage.partitionLevel) {
      if (mapResults.size() == 0) {
        return Collections.emptyList();
      }
      if (mapResults.size() == 1) {
        HashMap<String, GroupedValue> ret = convert((MapResult) mapResults.get(0));
        return java.util.Arrays.asList((Serializable) ret);
      }
      HashMap<BoboSegmentReader, MapResult> results = new HashMap<BoboSegmentReader, MapResult>();
      for (int i = 0; i < mapResults.size(); i++) {
        MapResult current = (MapResult) mapResults.get(i);
        if (results.get(current.indexReader) != null) {

          Long2ObjectOpenHashMap<GroupedValue> currentMergedResults = results
              .get(current.indexReader).results;

          Long2ObjectOpenHashMap<GroupedValue> currentResultsToMerge = current.results;
          for (long key : currentResultsToMerge.keySet()) {
            GroupedValue groupedValue = currentMergedResults.get(key);
            if (groupedValue != null) {
              groupedValue.merge(currentResultsToMerge.get(key));
            } else {
              currentMergedResults.put(key, currentResultsToMerge.get(key));
            }
          }
          // .putAll(currentResultsToMerge);
          trimToSize(currentResultsToMerge, Math.max(TRIM_SIZE, top));
        } else {
          results.put(current.indexReader, current);
        }
      }
      HashMap<String, GroupedValue> ret = null;
      for (BoboSegmentReader key : results.keySet()) {
        if (ret == null) {
          ret = convert(results.get(key));
        } else {
          merge(ret, convert(results.get(key)));
        }

      }
      // System.out.println("End combine - " + ret);
      return java.util.Arrays.asList((Serializable) ret);
    }
    if (mapResults.size() == 0) {
      return Collections.emptyList();
    }
    if (mapResults.size() == 1) {
      HashMap<String, GroupedValue> ret = (HashMap<String, GroupedValue>) mapResults.get(0);
      return java.util.Arrays.asList((Serializable) ret);
    }
    HashMap<String, GroupedValue> firstMap = (HashMap<String, GroupedValue>) mapResults.get(0);
    for (int i = 1; i < mapResults.size(); i++) {
      merge(firstMap, (HashMap<String, GroupedValue>) mapResults.get(i));
    }
    trimToSize(firstMap, Math.max(TRIM_SIZE, top));
    return java.util.Arrays.asList((Serializable) firstMap);

  }

  private HashMap<String, GroupedValue> convert(MapResult mapResult) {
    HashMap<String, GroupedValue> ret = new HashMap<String, GroupedValue>(mapResult.results.size());
    String[] temp = new String[mapResult.dictionaries.length];
    int[] numBits = new int[columns.length];
    for (int i = 0; i < columns.length; i++) {
      numBits[i] = BitHacks.findLogBase2(mapResult.dictionaries[i].size()) + 1;
    }
    for (long key : mapResult.results.keySet()) {
      ret.put(decodeKey(temp, mapResult.dictionaries, numBits, key), mapResult.results.get(key));
    }
    return ret;
  }

  private void merge(HashMap<String, GroupedValue> firstMap, HashMap<String, GroupedValue> secondMap) {
    for (Map.Entry<String, GroupedValue> entry : secondMap.entrySet()) {
      GroupedValue groupedValue = firstMap.get(entry.getKey());
      if (groupedValue != null) {
        groupedValue.merge(entry.getValue());
      } else {
        firstMap.put(entry.getKey(), entry.getValue());
      }
    }

  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public HashMap<String, GroupedValue> reduce(List<Serializable> combineResultsRaw) {
    List<HashMap<String, GroupedValue>> combineResults = (List) combineResultsRaw;
    if (combineResults.size() == 0) {
      return null;
    }
    if (combineResults.size() == 1) {
      return combineResults.get(0);
    }
    HashMap<String, GroupedValue> firstMap = combineResults.get(0);
    for (int i = 1; i < combineResults.size(); i++) {
      merge(firstMap, combineResults.get(i));
    }
    trimToSize(firstMap, TRIM_SIZE);
    return firstMap;
  }

  /**
   * Tries to trim the map to smaller size
   *
   * @param map
   * @param count
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private static void trimToSize(Map<String, ? extends Comparable> map, int count) {

    if (map.size() < count) {
      return;
    }
    double trimRatio = ((double) count) / map.size() * 2;
    if (trimRatio >= 1.0D) {
      return;
    }
    int queueSize = (int) (map.size() / Math.log(map.size()) / 4);
    PriorityQueue<Comparable> queue = new PriorityQueue<Comparable>(queueSize);
    int i = 0;
    int addElementRange = map.size() / queueSize;
    for (Comparable groupedValue : map.values()) {
      if (i == addElementRange) {
        i = 0;
        queue.add(groupedValue);
      } else {
        i++;
      }
    }
    int elementIndex = (int) (queue.size() * (1.0d - trimRatio));
    if (elementIndex >= queue.size()) {
      elementIndex = queue.size() - 1;
    }

    int counter = 0;
    Comparable newMinimumValue = null;
    while (!queue.isEmpty()) {
      if (counter == elementIndex) {
        newMinimumValue = queue.poll();
        break;
      } else {
        counter++;
        queue.poll();
      }
    }
    if (newMinimumValue == null) {
      return;
    }
    Iterator<? extends Comparable> iterator = map.values().iterator();
    int numToRemove = map.size() - count;
    counter = 0;
    while (iterator.hasNext()) {
      if (iterator.next().compareTo(newMinimumValue) <= 0) {
        counter++;
        iterator.remove();
        if (counter >= numToRemove) {
          break;
        }
      }
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private static void trimToSize(Long2ObjectMap<? extends Comparable> map, int count) {

    if (map.size() < count) {
      return;
    }
    double trimRatio = ((double) count) / map.size() * 2;
    if (trimRatio >= 1.0D) {
      return;
    }
    int queueSize = (int) (map.size() / Math.log(map.size()) / 4);
    PriorityQueue<Comparable> queue = new PriorityQueue<Comparable>(queueSize);
    int i = 0;
    int addElementRange = map.size() / queueSize;
    for (Comparable groupedValue : map.values()) {
      if (i == addElementRange) {
        i = 0;
        queue.add(groupedValue);
      } else {
        i++;
      }
    }
    int elementIndex = (int) (queue.size() * (1.0d - trimRatio));
    if (elementIndex >= queue.size()) {
      elementIndex = queue.size() - 1;
    }

    int counter = 0;
    Comparable newMinimumValue = null;
    while (!queue.isEmpty()) {
      if (counter == elementIndex) {
        newMinimumValue = queue.poll();
        break;
      } else {
        counter++;
        queue.poll();
      }
    }
    if (newMinimumValue == null) {
      return;
    }
    Iterator<? extends Comparable> iterator = map.values().iterator();
    int numToRemove = map.size() - count;
    counter = 0;
    while (iterator.hasNext()) {
      if (iterator.next().compareTo(newMinimumValue) <= 0) {
        counter++;
        iterator.remove();
        if (counter >= numToRemove) {
          break;
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public JSONObject render(HashMap<String, GroupedValue> reduceResult) {
    try {
      Object result = aggregateFunction.toJson(reduceResult);
      if (result instanceof JSONObject) {
        return (JSONObject) result;
      } else if (result instanceof JSONArray) {
        JSONArray jsonArrResult = (JSONArray) result;
        if (jsonArrResult.length() > top) {
          JSONArray newArr = new JSONUtil.FastJSONArray();
          for (int i = 0; i < top; i++) {
            newArr.put(jsonArrResult.get(i));
          }
          jsonArrResult = newArr;
        }
        return new JSONUtil.FastJSONObject().put("grouped", jsonArrResult).put("column", metric);
      } else {
        return new JSONUtil.FastJSONObject().put("grouped", result).put("column", metric);
      }
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

}
