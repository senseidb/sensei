package com.senseidb.search.query.filters;

import java.io.IOException;
import java.util.Iterator;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.json.JSONObject;

import com.browseengine.bobo.api.BoboSegmentReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.impl.PathFacetHandler;
import com.browseengine.bobo.query.MatchAllDocIdSetIterator;

public class PathFilterConstructor extends FilterConstructor {
  public static final String FILTER_TYPE = "path";

  @SuppressWarnings("unchecked")
  @Override
  protected Filter doConstructFilter(Object param) throws Exception {
    JSONObject json = (JSONObject) param;

    Iterator<String> iter = json.keys();
    if (!iter.hasNext()) return null;

    final String field = iter.next();
    final String path;
    final int depth;
    final boolean strict;

    Object obj = json.get(field);
    if (obj instanceof JSONObject) {
      path = ((JSONObject) obj).getString(VALUE_PARAM);
      depth = ((JSONObject) obj).optInt(DEPTH_PARAM, 0);
      strict = ((JSONObject) obj).optBoolean(STRICT_PARAM, false);
    } else {
      path = String.valueOf(obj);
      depth = 0;
      strict = false;
    }

    return new Filter() {
      @Override
      public DocIdSet getDocIdSet(AtomicReaderContext context, final Bits acceptDocs) throws IOException {
        if (context.reader() instanceof BoboSegmentReader) {
          final BoboSegmentReader boboReader = (BoboSegmentReader) context.reader();
          FacetHandler<?> facetHandler = boboReader.getFacetHandler(field);
          if (facetHandler != null && facetHandler instanceof PathFacetHandler) {
            BrowseSelection sel = new BrowseSelection(field);
            sel.setValues(new String[] { path });
            sel.setSelectionProperty(PathFacetHandler.SEL_PROP_NAME_DEPTH, String.valueOf(depth));
            sel.setSelectionProperty(PathFacetHandler.SEL_PROP_NAME_STRICT, String.valueOf(strict));
            Filter filter = ((PathFacetHandler) facetHandler).buildFilter(sel);
            if (filter == null) return new DocIdSet() {
              @Override
              public boolean isCacheable() {
                return false;
              }

              @Override
              public DocIdSetIterator iterator() throws IOException {
                return new MatchAllDocIdSetIterator(boboReader, acceptDocs);
              }
            };
            return filter.getDocIdSet(context, acceptDocs);
          }
        }

        throw new UnsupportedOperationException("Path filter is not supported for your field: "
            + field);
      }
    };
  }
}
