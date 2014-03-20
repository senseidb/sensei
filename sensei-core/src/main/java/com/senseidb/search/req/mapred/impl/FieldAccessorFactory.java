package com.senseidb.search.req.mapred.impl;

import java.util.Set;

import proj.zoie.api.DocIDMapper;

import com.browseengine.bobo.api.BoboSegmentReader;
import com.senseidb.search.req.SenseiSystemInfo.SenseiFacetInfo;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.search.req.mapred.IntArray;

public interface FieldAccessorFactory {
  public FieldAccessor getAccessor(Set<SenseiFacetInfo> facetInfos,
      BoboSegmentReader boboIndexReader, DocIDMapper mapper);

  public IntArray getDocArray(BoboSegmentReader boboIndexReader);
}
