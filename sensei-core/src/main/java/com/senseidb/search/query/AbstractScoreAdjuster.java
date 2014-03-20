package com.senseidb.search.query;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;

public abstract class AbstractScoreAdjuster extends Query {
  public class ScoreAdjusterWeight extends Weight {
    Weight _innerWeight;

    public ScoreAdjusterWeight(Weight innerWeight) throws IOException {
      _innerWeight = innerWeight;
    }

    @Override
    public String toString() {
      return "weight(" + AbstractScoreAdjuster.this + ")";
    }

    @Override
    public Query getQuery() {
      return _innerWeight.getQuery();
    }

    @Override
    public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
      Explanation innerExplain = _innerWeight.explain(context, doc);
      return createExplain(innerExplain, context, doc);
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return _innerWeight.getValueForNormalization();
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      _innerWeight.normalize(norm, topLevelBoost);
    }

    @Override
    public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder, boolean topScorer,
        Bits acceptDocs) throws IOException {
      Scorer innerScorer = _innerWeight.scorer(context, scoreDocsInOrder, topScorer, acceptDocs);
      if (innerScorer == null) {
        return null;
      }
      return createScorer(innerScorer, context);
    }
  }

  protected final Query _query;

  public AbstractScoreAdjuster(Query query) {
    _query = query;
  }

  protected abstract Scorer createScorer(Scorer innerScorer, AtomicReaderContext context)
      throws IOException;

  protected Explanation createExplain(Explanation innerExplain, AtomicReaderContext context, int doc)
      throws IOException {
    return innerExplain;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    return new ScoreAdjusterWeight(_query.createWeight(searcher));
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    _query.rewrite(reader);
    return this;
  }

  @Override
  public String toString(String field) {
    return _query.toString(field);
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    _query.extractTerms(terms);
  }
}
