package com.senseidb.search.req.mapred.functions;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

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

public class DistinctCountMapReduce implements SenseiMapReduce<IntOpenHashSet, Integer> {

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
  public IntOpenHashSet map(IntArray docId, int docIdCount, long[] uids, FieldAccessor accessor,
      FacetCountAccessor facetCountAccessor) {

    SingleFieldAccessor singleFieldAccessor = accessor.getSingleFieldAccessor(column);
    IntOpenHashSet intSet = new IntOpenHashSet();
    if (!(accessor.getTermValueList(column) instanceof TermNumberList)) {
      for (int i = 0; i < docIdCount; i++) {
        singleFieldAccessor.get(docId.get(i)).hashCode();
        intSet.add(singleFieldAccessor.get(docId.get(i)).hashCode());
      }
    } else {
      for (int i = 0; i < docIdCount; i++) {
        singleFieldAccessor.getInteger(docId.get(i));
        intSet.add(singleFieldAccessor.get(docId.get(i)).hashCode());
      }
    }

    return intSet;
  }

  @Override
  public List<IntOpenHashSet> combine(List<IntOpenHashSet> mapResults, CombinerStage combinerStage) {
    if (mapResults.isEmpty()) {
      return mapResults;
    }
    IntOpenHashSet ret = mapResults.get(0);
    for (int i = 1; i < mapResults.size(); i++) {
      ret.addAll(mapResults.get(i));
    }
    mapResults.clear();
    mapResults.add(ret);
    return mapResults;
  }

  @Override
  public Integer reduce(List<IntOpenHashSet> combineResults) {
    if (combineResults.isEmpty()) {
      return 0;
    }
    IntOpenHashSet ret = combineResults.get(0);
    for (int i = 1; i < combineResults.size(); i++) {
      ret.addAll(combineResults.get(i));
    }

    return ret.size();
  }

  @Override
  public JSONObject render(Integer reduceResult) {
    // TODO Auto-generated method stub
    try {
      return new FastJSONObject().put("distinctCount", reduceResult);
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }
  }

}
