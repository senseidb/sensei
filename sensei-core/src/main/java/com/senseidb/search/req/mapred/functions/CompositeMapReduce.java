package com.senseidb.search.req.mapred.functions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.util.Assert;

import com.senseidb.search.req.mapred.CombinerStage;
import com.senseidb.search.req.mapred.FacetCountAccessor;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.search.req.mapred.IntArray;
import com.senseidb.search.req.mapred.SenseiMapReduce;
import com.senseidb.search.req.mapred.impl.MapReduceRegistry;
import com.senseidb.util.JSONUtil;
import com.senseidb.util.Pair;

public class CompositeMapReduce implements SenseiMapReduce<Serializable, Serializable> {
  private List<Pair<String, SenseiMapReduce>> innerFunctions = new ArrayList<Pair<String,SenseiMapReduce>>();
  private Map<Key, Pair<String, SenseiMapReduce>> innerFunctionsRefs = new LinkedHashMap<Key, Pair<String,SenseiMapReduce>>();
  public static class Key implements Serializable {
    private static AtomicLong atomicLong = new AtomicLong();
    private long value;
    public Key() {
      value = atomicLong.incrementAndGet();
    }
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (int) (value ^ (value >>> 32));
      return result;
    }
    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Key other = (Key) obj;
      if (value != other.value)
        return false;
      return true;
    }
    
  }
  
  
  @Override
  public void init(JSONObject params) {
    try {
    if (params.optJSONArray("array") != null) {
      JSONArray array = params.optJSONArray("array");
      for (int i = 0; i < array.length(); i++) {
        JSONObject mapRed = array.optJSONObject(i);
        if (mapRed == null) {
          continue;
        }
          String function = mapRed.getString("mapReduce");
          SenseiMapReduce senseiMapReduce = MapReduceRegistry.get(function);
          Assert.notNull(senseiMapReduce, "Could not retrieve map reduce function by the identifier");
          innerFunctions.add(new Pair(function, senseiMapReduce));
          senseiMapReduce.init(mapRed);
      }
      
    } else {
      Iterator<String> keys = params.keys();
      while (keys.hasNext()) {
        String key = keys.next();
        JSONObject innerFunctionParams = params.optJSONObject(key);
        SenseiMapReduce senseiMapReduce = MapReduceRegistry.get(key);
        Assert.notNull(senseiMapReduce, "Could not retrieve map reduce function by the identifier");
        innerFunctions.add(new Pair(key, senseiMapReduce));
        senseiMapReduce.init(innerFunctionParams);
      }
     
    }
    
    
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
    for (Pair<String, SenseiMapReduce> pair : innerFunctions) {
      innerFunctionsRefs.put(new Key(), pair);
    }
   

  }

  @Override
  public Serializable map(IntArray docIds, int docIdCount, long[] uids, FieldAccessor accessor, FacetCountAccessor facetCountsAccessor) {
    HashMap<Key, Serializable> mapResults = new HashMap<Key, Serializable>();
    for (Key id : innerFunctionsRefs.keySet()) {
      Pair<String, SenseiMapReduce> pair = innerFunctionsRefs.get(id);
      SenseiMapReduce senseiMapReduce = pair.getSecond();
      mapResults.put(id, senseiMapReduce.map(docIds, docIdCount, uids, accessor, facetCountsAccessor));
    }
    return mapResults;
  }

  @Override
  public List<Serializable> combine(List<Serializable> mapResults, CombinerStage combinerStage) {
    if (mapResults.size() < 1) {
      return mapResults;
    }
    
    HashMap<Key, Serializable> firstResult = (HashMap<Key, Serializable>) mapResults.get(0);
    
    HashMap<Key, ArrayList<Serializable>> resultsPerFunction = aggregate(mapResults, firstResult.keySet(), combinerStage == CombinerStage.partitionLevel);
    firstResult.clear();
    for (Key key : resultsPerFunction.keySet()) {
      SenseiMapReduce function = combinerStage == CombinerStage.partitionLevel ? innerFunctionsRefs.get(key).getSecond() : MapReduceRegistry.get(innerFunctionsRefs.get(key).getFirst());
      firstResult.put(key, (Serializable) function.combine(resultsPerFunction.get(key), combinerStage));
    }
    ArrayList<Serializable> ret = new ArrayList<Serializable>();
    ret.add(firstResult);
    return ret;
  }



  @Override
  public Serializable reduce(List<Serializable> combineResults) {
    if (combineResults.size() == 0) {
      return null;
    }
   
    HashMap<Key, Serializable> firstResult = (HashMap<Key, Serializable>) combineResults.get(0);
    HashMap<Key, ArrayList<Serializable>> resultsPerFunction = aggregate(combineResults, firstResult.keySet(), false);
    firstResult.clear();
    for (Key key : resultsPerFunction.keySet()) {
      SenseiMapReduce function = MapReduceRegistry.get(innerFunctionsRefs.get(key).getFirst());
      
      firstResult.put(key, (Serializable) function.reduce(resultsPerFunction.get(key)));
    }
    return firstResult;
  }
 
  private HashMap<Key, ArrayList<Serializable>> aggregate(List<Serializable> mapResults, Set<Key> keys, boolean isPartitionLevel) {
    HashMap<Key, ArrayList<Serializable>> resultsPerFunction = new HashMap<Key, ArrayList<Serializable>>(keys.size());
    for (Key key : keys) {
      resultsPerFunction.put(key, new ArrayList<Serializable>());
    }
    for (Serializable mapResultRaw : mapResults) {
      HashMap<Key, Serializable> mapResult = (HashMap<Key, Serializable>) mapResultRaw;
      for (Key key : keys) {
        
        Serializable value = mapResult.get(key);
        if (value instanceof List && !isPartitionLevel && ((List)value).size() == 1) {
          value = (Serializable) ((List)value).get(0);
        }
        resultsPerFunction.get(key).add(value);
      }
    }
    return resultsPerFunction;
  }
  
  @Override
  public JSONObject render(Serializable reduceResultRaw) {
    try {
      HashMap<Key, Serializable> reduceResult = (HashMap<Key, Serializable>) reduceResultRaw;
      JSONObject ret = new JSONUtil.FastJSONObject();
      JSONArray array = new JSONUtil.FastJSONArray();
      for (Key key : innerFunctionsRefs.keySet()) {
        JSONObject entry = new JSONUtil.FastJSONObject();
        entry.put("result", innerFunctionsRefs.get(key).getSecond().render(reduceResult.get(key)));
        entry.put("function", innerFunctionsRefs.get(key).getFirst());
        array.put(entry);
      }
      ret.put("results", array);
      return ret;
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

}
