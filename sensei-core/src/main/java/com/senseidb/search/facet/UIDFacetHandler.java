package com.senseidb.search.facet;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreDoc;

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.ZoieSegmentReader;

import com.browseengine.bobo.api.BoboSegmentReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.docidset.EmptyDocIdSet;
import com.browseengine.bobo.docidset.IntArrayDocIdSet;
import com.browseengine.bobo.docidset.RandomAccessDocIdSet;
import com.browseengine.bobo.facets.FacetCountCollectorSource;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.filter.EmptyFilter;
import com.browseengine.bobo.facets.filter.RandomAccessFilter;
import com.browseengine.bobo.facets.filter.RandomAccessNotFilter;
import com.browseengine.bobo.sort.DocComparator;
import com.browseengine.bobo.sort.DocComparatorSource;


public class UIDFacetHandler extends FacetHandler<long[]> {
  public UIDFacetHandler(String name) {
    super(name);
  }

  private static class SingleDocRandmAccessDocIdSet extends RandomAccessDocIdSet {
    final int docid;

    SingleDocRandmAccessDocIdSet(int doc) {
      docid = doc;
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
      return new DocIdSetIterator() {
        protected int _doc = -1;

        @Override
        public int advance(int id) throws IOException {
          _doc = id - 1;
          return nextDoc();
        }

        @Override
        public int docID() {
          return _doc;
        }

        @Override
        public int nextDoc() throws IOException {
          if (_doc < docid) {
            return _doc = docid;
          }
          return _doc = DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public long cost() {
          // TODO Auto-generated method stub
          return 0;
        }
      };
    }

    @Override
    public boolean get(int doc) {
      return doc == docid;
    }
  }

  private RandomAccessFilter buildRandomAccessFilter(final long val) throws IOException {
    return new RandomAccessFilter() {

      @Override
      public RandomAccessDocIdSet getRandomAccessDocIdSet(BoboSegmentReader reader)
          throws IOException {
        ZoieSegmentReader<?> zoieReader = (ZoieSegmentReader<?>) (reader.getInnerReader());
        DocIDMapper docidMapper = zoieReader.getDocIDMapper();
        final int docid = docidMapper.getDocID(val);
        if (docid == DocIDMapper.NOT_FOUND) {
          return EmptyDocIdSet.getInstance();
        }

        return new SingleDocRandmAccessDocIdSet(docid);
      }
    };
  }

  private RandomAccessFilter buildRandomAccessFilter(final LongSet valSet) throws IOException {
    return new RandomAccessFilter() {
      @Override
      public RandomAccessDocIdSet getRandomAccessDocIdSet(BoboSegmentReader reader)
          throws IOException {
        ZoieSegmentReader<?> zoieReader = (ZoieSegmentReader<?>) (reader.getInnerReader());
        DocIDMapper docidMapper = zoieReader.getDocIDMapper();

        final IntArrayList docidList = new IntArrayList(valSet.size());

        LongIterator iter = valSet.iterator();

        while (iter.hasNext()) {
          long uid = iter.nextLong();
          int docid = docidMapper.getDocID(uid);
          if (docid != DocIDMapper.NOT_FOUND) {
            docidList.add(docid);
          }
        }

        if (docidList.size() == 0) {
          return EmptyDocIdSet.getInstance();
        }

        if (docidList.size() == 1) {
          int docId = docidList.getInt(0);
          if (!zoieReader.isDeletedInMask(docId)) {
            return new SingleDocRandmAccessDocIdSet(docidList.getInt(0));
          } else {
            return EmptyDocIdSet.getInstance();
          }
        }
        Collections.sort(docidList);
        final IntArrayDocIdSet intArraySet = new IntArrayDocIdSet(docidList.size());
        for (int docId : docidList) {
          if (!zoieReader.isDeletedInMask(docId)) {
            intArraySet.addDoc(docId);
          }
        }
        return new RandomAccessDocIdSet() {
          @Override
          public boolean get(int docid) {
            return docidList.contains(docid);
          }

          @Override
          public DocIdSetIterator iterator() throws IOException {
            return intArraySet.iterator();
          }
        };
      }
    };
  }

  @Override
  public RandomAccessFilter buildRandomAccessFilter(String value, Properties selectionProperty)
      throws IOException {
    try {
      long val = Long.parseLong(value);
      return buildRandomAccessFilter(val);
    } catch (Exception e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public RandomAccessFilter buildRandomAccessAndFilter(String[] vals, Properties prop)
      throws IOException {
    LongSet longSet = new LongOpenHashSet();
    for (String val : vals) {
      try {
        longSet.add(Long.parseLong(val));
      } catch (Exception e) {
        throw new IOException(e.getMessage());
      }
    }
    if (longSet.size() != 1) {
      return EmptyFilter.getInstance();
    } else {
      return buildRandomAccessFilter(longSet.iterator().nextLong());
    }
  }

  @Override
  public RandomAccessFilter buildRandomAccessOrFilter(String[] vals, Properties prop, boolean isNot)
      throws IOException {
    LongSet longSet = new LongOpenHashSet();
    for (String val : vals) {
      try {
        longSet.add(Long.parseLong(val));
      } catch (Exception e) {
        throw new IOException(e.getMessage());
      }
    }
    RandomAccessFilter filter;
    if (longSet.size() == 1) {
      filter = buildRandomAccessFilter(longSet.iterator().nextLong());
    } else {
      filter = buildRandomAccessFilter(longSet);
    }
    if (filter == null) return filter;
    if (isNot) {
      filter = new RandomAccessNotFilter(filter);
    }
    return filter;
  }

  @Override
  public DocComparatorSource getDocComparatorSource() {
    return new DocComparatorSource() {

      @Override
      public DocComparator getComparator(AtomicReader reader, int docbase) throws IOException {
        final UIDFacetHandler uidFacetHandler = UIDFacetHandler.this;
        if (reader instanceof BoboSegmentReader) {
          BoboSegmentReader boboReader = (BoboSegmentReader) reader;
          final long[] uidArray = uidFacetHandler.getFacetData(boboReader);
          return new DocComparator() {

            @Override
            public Comparable<?> value(ScoreDoc doc) {
              int docid = doc.doc;
              return Long.valueOf(uidArray[docid]);
            }

            @Override
            public int compare(ScoreDoc doc1, ScoreDoc doc2) {
              long uid1 = uidArray[doc1.doc];
              long uid2 = uidArray[doc2.doc];
              if (uid1 == uid2) {
                return 0;
              } else {
                if (uid1 < uid2) return -1;
                return 1;
              }
            }
          };
        } else {
          throw new IOException("reader must be instance of: " + BoboSegmentReader.class);
        }
      }
    };
  }

  @Override
  public FacetCountCollectorSource getFacetCountCollectorSource(BrowseSelection sel, FacetSpec fspec) {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public String[] getFieldValues(BoboSegmentReader reader, int id) {
    long[] uidArray = getFacetData(reader);
    return new String[] { String.valueOf(uidArray[id]) };
  }

  @Override
  public Object[] getRawFieldValues(BoboSegmentReader reader, int id) {
    long[] uidArray = getFacetData(reader);
    return new Long[] { uidArray[id] };
  }

  @Override
  public long[] load(BoboSegmentReader reader) throws IOException {
    IndexReader innerReader = reader.getInnerReader();
    if (innerReader instanceof ZoieSegmentReader) {
      ZoieSegmentReader<?> zoieReader = (ZoieSegmentReader<?>) innerReader;
      return zoieReader.getUIDArray();
    } else {
      throw new IOException("inner reader not instance of " + ZoieSegmentReader.class);
    }
  }

}
