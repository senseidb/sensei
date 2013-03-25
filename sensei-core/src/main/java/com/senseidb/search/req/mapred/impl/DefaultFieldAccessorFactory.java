package com.senseidb.search.req.mapred.impl;

import java.util.Set;

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.ZoieSegmentReader;
import proj.zoie.api.impl.DocIDMapperImpl;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.search.req.SenseiSystemInfo.SenseiFacetInfo;
import com.senseidb.search.req.mapred.DefaultIntArray;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.search.req.mapred.FieldAccessorImpl;
import com.senseidb.search.req.mapred.IntArray;

public class DefaultFieldAccessorFactory implements FieldAccessorFactory {
  @Override
  public FieldAccessor getAccessor(Set<SenseiFacetInfo> facetInfos, BoboIndexReader boboIndexReader, DocIDMapper mapper) {
    return new FieldAccessorImpl(facetInfos, boboIndexReader, mapper);
  }

  @Override
  public IntArray getDocArray(BoboIndexReader boboIndexReader) {
    ZoieSegmentReader<?> zoieReader = (ZoieSegmentReader<?>)(boboIndexReader.getInnerReader());
    DocIDMapperImpl docIDMapper = (DocIDMapperImpl) zoieReader.getDocIDMaper();
    return new DefaultIntArray(docIDMapper.getDocArray());
  }

}
