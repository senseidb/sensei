package com.senseidb.search.query;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;

import proj.zoie.api.ZoieSegmentReader;

import com.browseengine.bobo.api.BoboSegmentReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.docidset.OrDocIdSet;
import com.browseengine.bobo.facets.FacetHandler;
import com.senseidb.indexing.DefaultSenseiInterpreter;
import com.senseidb.indexing.MetaType;


public class TimeRetentionFilter extends Filter {

  private final String _column;
  private final int _nDays;
  private final TimeUnit _dataUnit;

  static {
    DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(MetaType.Long);
  }

  public TimeRetentionFilter(String column, int nDays, TimeUnit dataUnit) {
    _column = column;
    _nDays = nDays;
    _dataUnit = dataUnit;
  }

  private DocIdSet buildFilterSet(BoboSegmentReader boboReader) throws IOException {
    FacetHandler<?> facetHandler = boboReader.getFacetHandler(_column);

    if (facetHandler != null) {
      DecimalFormat formatter = new DecimalFormat(
          DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(MetaType.Long));
      BrowseSelection sel = new BrowseSelection(_column);
      long duration = _dataUnit.convert(_nDays, TimeUnit.DAYS);
      long now = _dataUnit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
      long from = now - duration;
      sel.addValue("[" + formatter.format(from) + " TO *]");
      return facetHandler.buildFilter(sel).getDocIdSet(boboReader.getContext(),
        boboReader.getLiveDocs());
    }
    throw new IllegalStateException("no facet handler defined with column: " + _column);
  }

  @SuppressWarnings("unchecked")
  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
    if (context.reader() instanceof ZoieSegmentReader) {
      ZoieSegmentReader<BoboSegmentReader> zoieReader = (ZoieSegmentReader<BoboSegmentReader>) context
          .reader();
      List<DocIdSet> docIdSetList = new ArrayList<DocIdSet>(1);
      docIdSetList.add(buildFilterSet(zoieReader.getDecoratedReader()));
      return new OrDocIdSet(docIdSetList);
    } else {
      throw new IllegalStateException("reader not instance of " + ZoieSegmentReader.class);
    }
  }
}
