package com.senseidb.search.req.mapred.impl.dictionary;

import com.browseengine.bobo.facets.data.TermDoubleList;
import com.browseengine.bobo.facets.data.TermFloatList;
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermShortList;
import com.browseengine.bobo.facets.data.TermValueList;

public class AccessorFactory {
    public static DictionaryNumberAccessor get(TermValueList dictionary) {
        if (dictionary instanceof TermShortList) {
            return new ShortDictionaryAccessor((TermShortList) dictionary);
        }
        if (dictionary instanceof TermIntList) {
            return new IntDictinaryAccessor((TermIntList) dictionary);
        }
        if (dictionary instanceof TermLongList) {
            return new LongDictionaryAccessor((TermLongList) dictionary);
        }
        if (dictionary instanceof TermFloatList) {
            return new FloatDictionaryAccessor((TermFloatList) dictionary);
        }
        if (dictionary instanceof TermDoubleList) {
            return new DoubleDictionaryAccessor((TermDoubleList) dictionary);
        }
        return new StringDictionaryAccessor(dictionary);
    }
}
