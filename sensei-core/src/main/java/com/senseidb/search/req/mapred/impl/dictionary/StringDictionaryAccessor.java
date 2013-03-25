package com.senseidb.search.req.mapred.impl.dictionary;

import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermValueList;

public class StringDictionaryAccessor implements DictionaryNumberAccessor {
    private final TermValueList dictionary;

    public StringDictionaryAccessor(TermValueList dictionary) {
        this.dictionary = dictionary;
    }

    @Override
    public int getIntValue(int valueId) {
        String str = dictionary.get(valueId);
        return str != null ? Integer.parseInt(str) : 0;
    }

    @Override
    public float getFloatValue(int valueId) {
        String str = dictionary.get(valueId);
        return str != null ? Float.parseFloat(str) : 0;
    }

    @Override
    public long getLongValue(int valueId) {
        String str = dictionary.get(valueId);
        return str != null ? Long.parseLong(str) : 0;
    }

    @Override
    public double getDoubleValue(int valueId) {
        String str = dictionary.get(valueId);
        return str != null ? Double.parseDouble(str) : 0;
    }

    }
