package com.senseidb.search.req.mapred.impl.dictionary;

import com.browseengine.bobo.facets.data.TermShortList;

public class ShortDictionaryAccessor implements DictionaryNumberAccessor {
    private final TermShortList dictionary;

    public ShortDictionaryAccessor(TermShortList dictionary) {
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
