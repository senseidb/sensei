package com.senseidb.search.req.mapred.impl.dictionary;

import com.browseengine.bobo.facets.data.TermLongList;

public class LongDictionaryAccessor implements DictionaryNumberAccessor {
  private final TermLongList dictionary;

  public LongDictionaryAccessor(TermLongList dictionary) {
    this.dictionary = dictionary;
  }

  @Override
  public int getIntValue(int valueId) {
    return (int) dictionary.getPrimitiveValue(valueId);
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