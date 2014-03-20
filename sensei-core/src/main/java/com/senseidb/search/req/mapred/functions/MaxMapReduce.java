package com.senseidb.search.req.mapred.functions;

import java.io.Serializable;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.search.req.mapred.CombinerStage;
import com.senseidb.search.req.mapred.FacetCountAccessor;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.search.req.mapred.IntArray;
import com.senseidb.search.req.mapred.SenseiMapReduce;
import com.senseidb.search.req.mapred.SingleFieldAccessor;
import com.senseidb.util.JSONUtil.FastJSONObject;

public class MaxMapReduce implements SenseiMapReduce<MaxResult, MaxResult> {

  /**
   *
   */
  private static final long serialVersionUID = 1L;
  private String column;

  @Override
  public MaxResult map(IntArray docIds, int docIdCount, long[] uids, FieldAccessor accessor,
      FacetCountAccessor facetCountAccessor) {
    double max = Double.MIN_VALUE;
    double tmp = 0;
    long uid = 0l;
    SingleFieldAccessor singleFieldAccessor = accessor.getSingleFieldAccessor(column);
    for (int i = 0; i < docIdCount; i++) {
      tmp = singleFieldAccessor.getDouble(docIds.get(i));
      if (max < tmp) {
        max = tmp;
        if (uids != null && !(uids.length == 1 && uids[0] == Long.MIN_VALUE)) {
          uid = uids[docIds.get(i)];
        } else {
          uid = docIds.get(i);
        }
      }
    }
    return new MaxResult(max, uid);
  }

  @Override
  public List<MaxResult> combine(List<MaxResult> mapResults, CombinerStage combinerStage) {
    if (mapResults.isEmpty()) {
      return mapResults;
    }
    MaxResult ret = mapResults.get(0);
    for (int i = 1; i < mapResults.size(); i++) {
      if (ret.value < mapResults.get(i).value) {
        ret = mapResults.get(i);
      }
    }
    return java.util.Arrays.asList(ret);
  }

  @Override
  public MaxResult reduce(List<MaxResult> combineResults) {
    if (combineResults.isEmpty()) {
      return null;
    }
    MaxResult ret = combineResults.get(0);
    for (int i = 1; i < combineResults.size(); i++) {
      if (ret.value < combineResults.get(i).value) {
        ret = combineResults.get(i);
      }
    }
    return ret;
  }

  @Override
  public JSONObject render(MaxResult reduceResult) {

    try {
      if (reduceResult == null) {
        return new FastJSONObject().put("max", "null");
      }
      return new FastJSONObject().put("max", String.format("%1.5f", reduceResult.value)).put("uid",
        reduceResult.uid);
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void init(JSONObject params) {
    column = params.optString("column");
    if (column == null) {
      throw new IllegalStateException("Column parameter shouldn't be null");
    }
  }

}

class MaxResult implements Serializable {
  /**
   *
   */
  private static final long serialVersionUID = 1L;
  public double value;
  public long uid;

  public MaxResult(double value, long uid) {
    super();
    this.value = value;
    this.uid = uid;
  }

}
