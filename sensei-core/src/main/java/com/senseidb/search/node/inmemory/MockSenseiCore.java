package com.senseidb.search.node.inmemory;

import java.util.Collections;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.util.Version;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieMultiReader;

import com.browseengine.bobo.api.BoboSegmentReader;
import com.senseidb.indexing.SenseiIndexPruner;
import com.senseidb.search.node.DefaultSenseiMultiBrowsableFactory;
import com.senseidb.search.node.DefaultSenseiSubBrowsableFactory;
import com.senseidb.search.node.SenseiCore;
import com.senseidb.search.node.SenseiIndexReaderDecorator;
import com.senseidb.search.node.impl.DefaultJsonQueryBuilderFactory;
import com.senseidb.search.req.mapred.impl.DefaultFieldAccessorFactory;

public class MockSenseiCore extends SenseiCore {

  private final ThreadLocal<MockIndexReaderFactory> mockIndexReaderFactory = new ThreadLocal<MockIndexReaderFactory>();
  private final int[] partitions;
  private static MockIndexReaderFactory emptyIndexFactory = new MockIndexReaderFactory(
      Collections.<ZoieMultiReader<BoboSegmentReader>> emptyList());

  public MockSenseiCore(int[] partitions, SenseiIndexReaderDecorator senseiIndexReaderDecorator) {
    super(0, new int[] { 0 }, null, null, new DefaultJsonQueryBuilderFactory(new QueryParser(
        Version.LUCENE_43, "contents", new StandardAnalyzer(Version.LUCENE_43))),
        new DefaultSenseiSubBrowsableFactory(), new DefaultSenseiMultiBrowsableFactory(),
        new DefaultFieldAccessorFactory(), senseiIndexReaderDecorator);
    this.partitions = partitions;
    setIndexPruner(new SenseiIndexPruner.DefaultSenseiIndexPruner());
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public IndexReaderFactory getIndexReaderFactory(int partition) {
    if (partition == partitions[0]) {
      return mockIndexReaderFactory.get();
    } else {
      return emptyIndexFactory;
    }
  }

  public void setIndexReaderFactory(MockIndexReaderFactory indexReaderFactory) {
    mockIndexReaderFactory.set(indexReaderFactory);
  }

  @Override
  public int[] getPartitions() {
    return partitions;
  }
}
