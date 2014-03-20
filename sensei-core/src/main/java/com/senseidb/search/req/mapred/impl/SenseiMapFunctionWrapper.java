package com.senseidb.search.req.mapred.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Set;

import proj.zoie.api.ZoieSegmentReader;
import proj.zoie.api.impl.DocIDMapperImpl;

import com.browseengine.bobo.api.BoboSegmentReader;
import com.browseengine.bobo.facets.FacetCountCollector;
import com.browseengine.bobo.mapred.BoboMapFunctionWrapper;
import com.browseengine.bobo.mapred.MapReduceResult;
import com.senseidb.search.req.SenseiSystemInfo.SenseiFacetInfo;
import com.senseidb.search.req.mapred.CombinerStage;
import com.senseidb.search.req.mapred.DefaultIntArray;
import com.senseidb.search.req.mapred.FacetCountAccessor;
import com.senseidb.search.req.mapred.IntArray;
import com.senseidb.search.req.mapred.SenseiMapReduce;

/**
 * Inctance of this class is the part of the senseiReuqest, and it keep the intermediate step of the map reduce job
 * @author vzhabiuk
 *
 */
@SuppressWarnings("rawtypes")
public class SenseiMapFunctionWrapper implements BoboMapFunctionWrapper {
  private final MapReduceResult result;
  private final SenseiMapReduce mapReduceStrategy;
  private final Set<SenseiFacetInfo> facetInfos;
  public static final int BUFFER_SIZE = 6000;
  private final int[] partialDocIds;;
  private int docIdIndex = 0;
  private final FieldAccessorFactory fieldAccessorFactory;

  public SenseiMapFunctionWrapper(SenseiMapReduce mapReduceStrategy,
      Set<SenseiFacetInfo> facetInfos, FieldAccessorFactory fieldAccessorFactory) {
    super();
    this.mapReduceStrategy = mapReduceStrategy;
    this.fieldAccessorFactory = fieldAccessorFactory;
    partialDocIds = new int[BUFFER_SIZE];
    result = new MapReduceResult();
    this.facetInfos = facetInfos;
  }

  /*
   * (non-Javadoc)
   * @see
   * com.browseengine.bobo.mapred.BoboMapFunctionWrapper#mapFullIndexReader(com.browseengine.bobo
   * .api.BoboSegmentReader)
   */
  @SuppressWarnings("unchecked")
  @Override
  public void mapFullIndexReader(BoboSegmentReader reader,
      FacetCountCollector[] facetCountCollectors) {
    ZoieSegmentReader<?> zoieReader = (ZoieSegmentReader<?>) (reader.getInnerReader());
    DocIDMapperImpl docIDMapper = (DocIDMapperImpl) zoieReader.getDocIDMapper();
    IntArray docArray = fieldAccessorFactory.getDocArray(reader);
    Serializable mapResult = mapReduceStrategy.map(docArray, docArray.size(),
      zoieReader.getUIDArray(), fieldAccessorFactory.getAccessor(facetInfos, reader, docIDMapper),
      new FacetCountAccessor(facetCountCollectors));
    if (mapResult != null) {
      result.getMapResults().add(mapResult);
    }
  }

  /*
   * (non-Javadoc)
   * @see com.browseengine.bobo.mapred.BoboMapFunctionWrapper#mapSingleDocument(int,
   * com.browseengine.bobo.api.BoboSegmentReader)
   */
  @SuppressWarnings("unchecked")
  @Override
  public final void mapSingleDocument(int docId, BoboSegmentReader reader) {
    if (docIdIndex < BUFFER_SIZE - 1) {
      partialDocIds[docIdIndex++] = docId;
      return;
    }
    if (docIdIndex == BUFFER_SIZE - 1) {
      partialDocIds[docIdIndex++] = docId;
      ZoieSegmentReader<?> zoieReader = (ZoieSegmentReader<?>) (reader.getInnerReader());
      DocIDMapperImpl docIDMapper = (DocIDMapperImpl) zoieReader.getDocIDMapper();
      Serializable mapResult = mapReduceStrategy
          .map(new DefaultIntArray(partialDocIds), BUFFER_SIZE, zoieReader.getUIDArray(),
            fieldAccessorFactory.getAccessor(facetInfos, reader, docIDMapper),
            FacetCountAccessor.EMPTY);
      if (mapResult != null) {
        result.getMapResults().add(mapResult);
      }
      docIdIndex = 0;
    }
  }

  /*
   * (non-Javadoc)
   * @see
   * com.browseengine.bobo.mapred.BoboMapFunctionWrapper#finalizeSegment(com.browseengine.bobo.api
   * .BoboSegmentReader)
   */
  @SuppressWarnings("unchecked")
  @Override
  public void finalizeSegment(BoboSegmentReader reader, FacetCountCollector[] facetCountCollectors) {

    if (docIdIndex > 0) {
      ZoieSegmentReader<?> zoieReader = (ZoieSegmentReader<?>) (reader.getInnerReader());
      DocIDMapperImpl docIDMapper = (DocIDMapperImpl) zoieReader.getDocIDMapper();
      Serializable mapResult = mapReduceStrategy.map(new DefaultIntArray(partialDocIds),
        docIdIndex, zoieReader.getUIDArray(), fieldAccessorFactory.getAccessor(facetInfos, reader,
          docIDMapper), new FacetCountAccessor(facetCountCollectors));
      if (mapResult != null) {
        result.getMapResults().add(mapResult);
      }
    }
    docIdIndex = 0;
  }

  /*
   * (non-Javadoc)
   * @see com.browseengine.bobo.mapred.BoboMapFunctionWrapper#finalizePartition()
   */
  @SuppressWarnings("unchecked")
  @Override
  public void finalizePartition() {
    result.setMapResults(new ArrayList(mapReduceStrategy.combine(result.getMapResults(),
      CombinerStage.partitionLevel)));
  }

  @Override
  public MapReduceResult getResult() {
    return result;
  }

}
