package com.senseidb.search.req.mapred.functions;

import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import com.browseengine.bobo.facets.data.TermNumberList;
import com.senseidb.search.req.mapred.CombinerStage;
import com.senseidb.search.req.mapred.FacetCountAccessor;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.search.req.mapred.IntArray;
import com.senseidb.search.req.mapred.SenseiMapReduce;
import com.senseidb.search.req.mapred.SingleFieldAccessor;
import com.senseidb.util.JSONUtil.FastJSONObject;

public class SumMapReduce implements SenseiMapReduce<Double, Double> {
  /**
   *
   */
  private static final long serialVersionUID = 1L;
  private String column;

  @Override
  public void init(JSONObject params) {
    column = params.optString("column");
    if (column == null) {
      throw new IllegalStateException("Column parameter shouldn't be null");
    }

  }

  @Override
  public Double map(IntArray docIds, int docIdCount, long[] uids, FieldAccessor accessor,
      FacetCountAccessor facetCountAccessor) {
    double ret = 0;
    SingleFieldAccessor singleFieldAccessor = accessor.getSingleFieldAccessor(column);
    if (!(accessor.getTermValueList(column) instanceof TermNumberList)) {
      throw new IllegalStateException("SumMapReduce needs numeric column");
    }
    for (int i = 0; i < docIdCount; i++) {
      ret += singleFieldAccessor.getDouble(docIds.get(i));
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
      return new FastJSONObject().put("sum", String.format("%1.5f", reduceResult));
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

}
