package com.senseidb.search.req.mapred.functions.groupby;

import java.io.Serializable;
import java.util.HashMap;

import com.senseidb.search.req.mapred.SingleFieldAccessor;

public interface AggregateFunction<T extends GroupedValue> extends Serializable {
  public T produceSingleValue(SingleFieldAccessor accessor, int docId);

  public Object toJson(HashMap<String, T> reduceResult);

}
