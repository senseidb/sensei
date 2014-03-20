package com.senseidb.indexing.activity.deletion;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;

import proj.zoie.api.indexing.AbstractZoieIndexable;

/**
 * This is used to notify the activity engine if the document gets deleted from Zoie by executing the purge filter
 * @author vzhabiuk
 *
 */
public class PurgeFilterWrapper extends Filter {
  private final Filter internal;
  private final DeletionListener deletionListener;

  public PurgeFilterWrapper(Filter internal, DeletionListener deletionListener) {
    this.internal = internal;
    this.deletionListener = deletionListener;
  }

  @Override
  public DocIdSet getDocIdSet(final AtomicReaderContext context, final Bits acceptDocs) throws IOException {
    return new DocIdSet() {
      @Override
      public DocIdSetIterator iterator() throws IOException {
        return new DocIdSetIteratorWrapper(internal.getDocIdSet(context, acceptDocs).iterator()) {
          @Override
          protected void handeDoc(int docid) throws IOException {
            AtomicReader reader = context.reader();
            Document doc = reader.document(docid);
            if (doc == null) {
              return;
            }
            IndexableField field = doc.getField(AbstractZoieIndexable.DOCUMENT_ID_PAYLOAD_FIELD);
            if (field == null) {
              return;
            }
            Number number = field.numericValue();
            if (number == null) {
              return;
            }
            deletionListener.onDelete(context.reader(), number.longValue());
          }

          @Override
          public long cost() {
            // TODO Auto-generated method stub
            return 0;
          }
        };
      }
    };
  }

  public abstract static class DocIdSetIteratorWrapper extends DocIdSetIterator {
    private final DocIdSetIterator iterator;

    public DocIdSetIteratorWrapper(DocIdSetIterator iterator) {
      this.iterator = iterator;
    }

    @Override
    public int docID() {
      return iterator.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int ret = iterator.nextDoc();
      if (ret != DocIdSetIterator.NO_MORE_DOCS) {
        handeDoc(ret);
      }
      return ret;
    }

    @Override
    public int advance(int target) throws IOException {
      int ret = iterator.advance(target);
      if (ret != DocIdSetIterator.NO_MORE_DOCS) {
        handeDoc(ret);
      }
      return ret;
    }

    protected abstract void handeDoc(int ret) throws IOException;
  }

}
