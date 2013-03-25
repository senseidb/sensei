package com.senseidb.search.req.mapred.impl;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.TermValueList;
import com.browseengine.bobo.util.BigSegmentedArray;
import com.senseidb.search.req.mapred.SingleFieldAccessor;
import com.senseidb.search.req.mapred.impl.dictionary.AccessorFactory;
import com.senseidb.search.req.mapred.impl.dictionary.DictionaryNumberAccessor;

public class SingleFieldAccessorImpl implements SingleFieldAccessor {
    private BigSegmentedArray orderArray;
    private DictionaryNumberAccessor dictionaryNumberAccessor;
    private TermValueList valArray;
    private final FacetHandler facetHandler;
    private final BoboIndexReader reader;

    @SuppressWarnings("rawtypes")
    public SingleFieldAccessorImpl(FacetDataCache dataCache,  FacetHandler facetHandler, BoboIndexReader reader) {
        this.facetHandler = facetHandler;
        this.reader = reader;
        orderArray = dataCache.orderArray;
        dictionaryNumberAccessor = AccessorFactory.get(dataCache.valArray);
        valArray = dataCache.valArray;
    }
    @Override
    public Object get(int docId) {
        return valArray.getRawValue(orderArray.get(docId));
    }

    @Override
    public String getString(int docId) {
        return valArray.get(orderArray.get(docId));
    }

    @Override
    public long getLong(int docId) {
        return dictionaryNumberAccessor.getLongValue(orderArray.get(docId));
    }

    @Override
    public double getDouble(int docId) {
        return dictionaryNumberAccessor.getDoubleValue(orderArray.get(docId));
    }

    @Override
    public short getShort(int docId) {
        return (short) dictionaryNumberAccessor.getIntValue(orderArray.get(docId));
    }

    @Override
    public int getInteger(int docId) {
        return dictionaryNumberAccessor.getIntValue(orderArray.get(docId));
    }

    @Override
    public float getFloat(int docId) {
        return (short) dictionaryNumberAccessor.getFloatValue(orderArray.get(docId));
    }

    @Override
    public Object[] getArray(int docId) {
        return facetHandler.getRawFieldValues(reader, docId);
    }
    @Override
    public int getDictionaryId(int docId) {
      
      return orderArray.get(docId);
    }

}
