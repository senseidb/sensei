package com.senseidb.search.req.mapred.functions;

import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.search.req.mapred.CombinerStage;
import com.senseidb.search.req.mapred.FacetCountAccessor;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.search.req.mapred.IntArray;
import com.senseidb.search.req.mapred.SenseiMapReduce;
import com.senseidb.util.JSONUtil.FastJSONObject;

public class CountMapReduce implements SenseiMapReduce<Double, Double> {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  @Override
  public void init(JSONObject params) {

  }

  @Override
  public Double map(IntArray docIds, int docIdCount, long[] uids, FieldAccessor accessor,
      FacetCountAccessor facetCountAccessor) {
    double ret = 0;
    for (int i = 0; i < docIdCount; i++) {
      ret += 1;
    }
    return ret;
  }

  @Override
  public List<Double> combine(List<Double> mapResults, CombinerStage combinerStage) {
    double ret = 0;
    for (Double count : mapResults) {
      ret += count;
    }
    mapResults.clear();
    mapResults.add(ret);
    return mapResults;
  }

  @Override
  public Double reduce(List<Double> combineResults) {
    double ret = 0;
    for (Double count : combineResults) {
      ret += count;
    }
    return ret;
  }

  @Override
  public JSONObject render(Double reduceResult) {

    try {
      return new FastJSONObject().put("count", String.format("%1.5f", reduceResult));
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

}
