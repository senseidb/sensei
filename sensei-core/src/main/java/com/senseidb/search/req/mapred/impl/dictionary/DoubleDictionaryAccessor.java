package com.senseidb.search.req.mapred.impl.dictionary;

import com.browseengine.bobo.facets.data.TermDoubleList;

public class DoubleDictionaryAccessor implements DictionaryNumberAccessor {
  private final TermDoubleList dictionary;

  public DoubleDictionaryAccessor(TermDoubleList dictionary) {
    this.dictionary = dictionary;
  }

  @Override
  public int getIntValue(int valueId) {
    return (int) dictionary.getPrimitiveValue(valueId);
  }

  @Override
  public float getFloatValue(int valueId) {
    return (float) dictionary.getPrimitiveValue(valueId);
  }

  @Override
  public long getLongValue(int valueId) {
    return (long) dictionary.getPrimitiveValue(valueId);
  }

  @Override
  public double getDoubleValue(int valueId) {
    return dictionary.getPrimitiveValue(valueId);
  }

}