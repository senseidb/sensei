package com.senseidb.search.req.mapred.functions.groupby;

import java.io.Serializable;

interface GroupedValue extends Comparable<GroupedValue>, Serializable {
  //AggregateFunction getFunction(FieldAccessor accessor, int docId);
  void merge(GroupedValue anotherValue);
  
}