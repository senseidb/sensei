package com.senseidb.indexing.activity;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.ZoieSegmentReader;

import com.browseengine.bobo.api.BoboSegmentReader;
import com.senseidb.search.node.SenseiCore;
import com.senseidb.search.plugin.PluggableSearchEngineManager;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

public class BoboIndexTracker {
  private final static Logger logger = Logger.getLogger(PluggableSearchEngineManager.class);
  private static Counter recoveredIndexInBoboFacetDataCache;
  private static Counter facetMappingMismatch;

  static {
    recoveredIndexInBoboFacetDataCache = Metrics.newCounter(new MetricName(
        CompositeActivityManager.class, "recoveredIndexInBoboFacetDataCache"));
    facetMappingMismatch = Metrics.newCounter(new MetricName(BoboIndexTracker.class,
        "facetMappingMismatch"));
  }

  public synchronized static void updateExistingBoboIndexes(SenseiCore senseiCore, int shard,
      long uid, int index, Set<String> facets) {
    IndexReaderFactory<BoboSegmentReader> indexReaderFactory = senseiCore
        .getIndexReaderFactory(shard);
    if (indexReaderFactory == null) {
      logger.error("Can't get index reader factory for shard: " + shard);
      return;
    }
    List<ZoieMultiReader<BoboSegmentReader>> indexReaders = null;
    try {
      indexReaders = indexReaderFactory.getIndexReaders();
      List<BoboSegmentReader> boboReaders = ZoieMultiReader.extractDecoratedReaders(indexReaders);
      for (BoboSegmentReader boboSegmentReader : boboReaders) {
        recoverReaderIfNeeded(uid, index, facets, boboSegmentReader);
      }
    } catch (IOException ex) {
      logger.error(ex.getMessage(), ex);
    } finally {
      if (indexReaders != null) {
        indexReaderFactory.returnIndexReaders(indexReaders);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private final static void recoverReaderIfNeeded(long uid, int index, Set<String> facets,
      BoboSegmentReader boboSegmentReader) {
    ZoieSegmentReader<BoboSegmentReader> zoieSegmentReader = (ZoieSegmentReader<BoboSegmentReader>) boboSegmentReader
        .getInnerReader();
    if (zoieSegmentReader == null) {
      return;
    }

    DocIDMapper mapper = zoieSegmentReader.getDocIDMapper();
    if (mapper == null) return;
    int docId = mapper.getDocID(uid);
    if (docId < 0) {
      return;
    }
    BoboSegmentReader decoratedReader = zoieSegmentReader.getDecoratedReader();
    for (String facet : facets) {
      Object facetData = decoratedReader.getFacetData(facet);
      if (!(facetData instanceof int[])) {
        logger.warn("The facet " + facet + " should have a facet data of type int[] but not "
            + facetData.getClass().toString());
        continue;
      }
      int[] indexes = (int[]) facetData;
      if (indexes.length <= docId) {
        logger
            .warn(String
                .format(
                  "The facet [%s] is supposed to contain the uid [%s] as the docid [%s], but its index array is only [%s] long",
                  facet, uid, docId, indexes.length));
        facetMappingMismatch.inc();
        continue;
      }
      if (indexes[docId] > -1 && indexes[docId] != index) {
        logger
            .warn(String
                .format(
                  "The facet [%s] is supposed to contain the uid [%s] as the docid [%s], with docId index [%s] but it contains index [%s]",
                  facet, uid, docId, index, indexes[docId]));
        facetMappingMismatch.inc();
        continue;
      }
      if (indexes[docId] == -1) {
        indexes[docId] = index;
        recoveredIndexInBoboFacetDataCache.inc();
      }
    }
    return;
  }

}
