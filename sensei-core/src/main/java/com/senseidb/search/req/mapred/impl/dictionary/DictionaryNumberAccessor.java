package com.senseidb.search.req.mapred.impl.dictionary;

public interface DictionaryNumberAccessor {
   public int getIntValue(int valueId);
   public float getFloatValue(int valueId);
   public long getLongValue(int valueId);
   public double getDoubleValue(int valueId);
}
