package com.senseidb.search.node.inmemory;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieMultiReader;

import com.browseengine.bobo.api.BoboSegmentReader;

public class MockIndexReaderFactory implements IndexReaderFactory<BoboSegmentReader> {
  private final Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_43);

  private final List<ZoieMultiReader<BoboSegmentReader>> readers;

  public MockIndexReaderFactory(List<ZoieMultiReader<BoboSegmentReader>> readers) {
    this.readers = readers;

  }

  @Override
  public List<ZoieMultiReader<BoboSegmentReader>> getIndexReaders() throws IOException {
    return readers;
  }

  @Override
  public Analyzer getAnalyzer() {
    return analyzer;
  }

  @Override
  public void returnIndexReaders(List<ZoieMultiReader<BoboSegmentReader>> r) {
  }

  @Override
  public String getCurrentReaderVersion() {
    return null;
  }

}
