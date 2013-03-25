package com.senseidb.search.req.mapred.impl;

import java.util.Set;

import proj.zoie.api.DocIDMapper;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.search.req.SenseiSystemInfo.SenseiFacetInfo;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.search.req.mapred.IntArray;

public interface FieldAccessorFactory {
    public FieldAccessor getAccessor(Set<SenseiFacetInfo> facetInfos, BoboIndexReader boboIndexReader, DocIDMapper mapper);
    public IntArray getDocArray(BoboIndexReader boboIndexReader);
}
