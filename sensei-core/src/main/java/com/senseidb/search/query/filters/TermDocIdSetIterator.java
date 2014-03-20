package com.senseidb.search.query.filters;

import java.io.IOException;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;

public class TermDocIdSetIterator extends DocIdSetIterator {
  private final DocsEnum docsEnum;
  private int doc = -1;

  /**
   * Construct a <code>TermDocIdSetIterator</code>.
   * @param term Term
   * @param reader
   *          IndexReader.
   */
  public TermDocIdSetIterator(Term term, AtomicReader reader) throws IOException {
    docsEnum = reader.termDocsEnum(term);
  }

  @Override
  public int docID() {
    return doc;
  }

  /**
   * Advances to the next document matching the query. <br>
   * The iterator over the matching documents is buffered using
   * {@link TermDocs#read(int[],int[])}.
   *
   * @return the document matching the query or NO_MORE_DOCS if there are no more documents.
   */
  @Override
  public int nextDoc() throws IOException {
    if (docsEnum == null) {
      return NO_MORE_DOCS;
    }
    doc = docsEnum.nextDoc();
    return doc;
  }

  /**
   * Advances to the first match beyond the current whose document number is
   * greater than or equal to a given target. <br>
   * The implementation uses {@link TermDocs#skipTo(int)}.
   *
   * @param target
   *          The target document number.
   * @return the matching document or NO_MORE_DOCS if none exist.
   */
  @Override
  public int advance(int target) throws IOException {
    if (docsEnum == null) {
      return NO_MORE_DOCS;
    }
    doc = docsEnum.advance(target);
    return doc;
  }

  @Override
  public long cost() {
    // TODO Auto-generated method stub
    return 0;
  }

}
