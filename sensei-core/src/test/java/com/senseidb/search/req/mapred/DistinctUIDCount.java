package com.senseidb.search.req.mapred;

import java.util.HashSet;
import java.util.List;
import org.json.JSONException;
import org.json.JSONObject;

@SuppressWarnings("unchecked")
public class DistinctUIDCount implements SenseiMapReduce<HashSet<Long>, Integer> {
  private static final long serialVersionUID = 1L;

  public void init(JSONObject params) {

  }

  @Override
  public HashSet<Long> map(IntArray docIds, int docIdCount, long[] uids, FieldAccessor accessor,
      FacetCountAccessor facetCountAccessor) {
    HashSet<Long> ret = new HashSet<Long>(docIdCount);
    for (int i = 0; i < docIdCount; i++) {
      ret.add(uids[docIds.get(i)]);
    }
    return ret;
  }

  @Override
  public List<HashSet<Long>> combine(List<HashSet<Long>> mapResults, CombinerStage combinerStage) {
    HashSet<Long> ret = new HashSet<Long>();
    for (HashSet<Long> mapResult : mapResults) {
      ret.addAll(mapResult);
    }
    return java.util.Arrays.asList(ret);
  }

  @Override
  public Integer reduce(List<HashSet<Long>> combineResults) {
    HashSet<Long> ret = new HashSet<Long>();
    for (HashSet<Long> mapResult : combineResults) {
      ret.addAll(mapResult);
    }
    return ret.size();
  }

  @Override
  public JSONObject render(Integer reduceResult) {

    try {
      return new JSONObject().put("distinctUidCount", reduceResult);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

}