package com.senseidb.search.query;

import java.io.IOException;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ToStringUtils;

public class MatchNoneDocsQuery extends Query {

  private class MatchNoneScorer extends Scorer {
    private final float score;
    private int doc = -1;
    private final AtomicReader reader;

    MatchNoneScorer(AtomicReader r, Weight w) throws IOException {
      super(w);
      score = w.getValueForNormalization();
      reader = r;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return NO_MORE_DOCS;
    }

    @Override
    public float score() {
      return score;
    }

    @Override
    public int advance(int target) throws IOException {
      Bits liveDocs = reader.getLiveDocs();
      for (int i = target; i < reader.maxDoc(); ++i) {
        if (liveDocs != null && !liveDocs.get(i)) {
          continue;
        }
        doc = i;
        return doc;
      }
      return NO_MORE_DOCS;
    }

    @Override
    public int freq() throws IOException {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public long cost() {
      // TODO Auto-generated method stub
      return 0;
    }
  }

  private class MatchNoneDocsWeight extends Weight {
    private float queryWeight;
    private float queryNorm;

    public MatchNoneDocsWeight(IndexSearcher searcher) throws IOException {
      queryWeight = getBoost();
    }

    @Override
    public String toString() {
      return "weight(" + MatchNoneDocsQuery.this + ")";
    }

    @Override
    public Query getQuery() {
      return MatchNoneDocsQuery.this;
    }

    @Override
    public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder, boolean topScorer,
        Bits acceptDocs) throws IOException {
      return new MatchNoneScorer(context.reader(), this);
    }

    @Override
    public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
      // explain query weight
      Explanation queryExpl = new ComplexExplanation(true, queryWeight,
          "MatchNoneDocsQuery, product of:");
      if (getBoost() != 1.0f) {
        queryExpl.addDetail(new Explanation(getBoost(), "boost"));
      }
      queryExpl.addDetail(new Explanation(queryNorm, "queryNorm"));

      return queryExpl;
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return queryWeight * queryWeight;
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      queryNorm = norm;
      queryWeight *= queryNorm;
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) {
    try {
      return new MatchNoneDocsWeight(searcher);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("*:^");
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MatchNoneDocsQuery)) return false;
    MatchNoneDocsQuery other = (MatchNoneDocsQuery) o;
    return this.getBoost() == other.getBoost();
  }

  @Override
  public int hashCode() {
    return Float.floatToIntBits(getBoost()) ^ 0x1AA71190;
  }
}
