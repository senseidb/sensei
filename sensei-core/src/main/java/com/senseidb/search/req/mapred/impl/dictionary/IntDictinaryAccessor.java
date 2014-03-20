package com.senseidb.search.req.mapred.impl.dictionary;

import com.browseengine.bobo.facets.data.TermIntList;

public class IntDictinaryAccessor implements DictionaryNumberAccessor {
  private final TermIntList dictionary;

  public IntDictinaryAccessor(TermIntList dictionary) {
    this.dictionary = dictionary;
  }

  @Override
  public int getIntValue(int valueId) {
    return dictionary.getPrimitiveValue(valueId);
  }

  @Override
  public float getFloatValue(int valueId) {
    return dictionary.getPrimitiveValue(valueId);
  }

  @Override
  public long getLongValue(int valueId) {
    return dictionary.getPrimitiveValue(valueId);
  }

  @Override
  public double getDoubleValue(int valueId) {
    return dictionary.getPrimitiveValue(valueId);
  }

}
