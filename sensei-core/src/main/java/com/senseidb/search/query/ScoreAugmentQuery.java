package com.senseidb.search.query;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.json.JSONException;
import org.json.JSONObject;

import com.browseengine.bobo.api.BoboSegmentReader;

public class ScoreAugmentQuery extends AbstractScoreAdjuster {

  public static interface ScoreAugmentFunction {

    /**
     * Initialize the function object if it has to store any reader-specific data; This will be called when creating the scorer;
     *
     * @param reader
     * @param jsonParams
     * @throws IOException
     *
     * This method initialize the internal score calculator, e.g., let it know what external data can be used, or anything required for computing the score;
     */
    public void initializeReader(BoboSegmentReader reader, JSONObject jsonParams)
        throws IOException;

    /**
     * Initialize the function in Query level, which means a global initialization; Such as accessing the external storage through a network connection, or any data could be reused;
     *
     * @param jsonParams  This JSONObject contains everything needed to initialize the scoreFunction from outside world. Just set it to NULL and ignore it in the method body if you don't want it.
     * @throws JSONException
     */
    public void initializeGlobal(JSONObject jsonParams) throws JSONException;

    /**
     * @return whether the innerscore will be used or not. If innerScore is used, newScore(float rawScore, int docID) will be called; Otherwise newScore(int docID) will be called.
     */
    public boolean useInnerScore();

    /**
     * @param rawScore
     * @param docID
     * @return the modified new score for document with the original innerScore;
     */
    public float newScore(float rawScore, int docID);

    /**
     *
     * @param rawScore
     * @param docID
     * @return the modified new score for document without the original innerScore to save time;
     */
    public float newScore(int docID);

    /**
     * @return the String to explain how the new score is generated;
     */
    public String getExplainString(float rawscore, int docID);

    /**
     * @return a copy of itself with the initialized global data; (Not reader-specific data) If there was no global initialization, just simply return this;
     */
    public ScoreAugmentFunction getCopy();
  }

  private static class AugmentScorer extends Scorer {
    private final ScoreAugmentFunction _func;
    private final Scorer _innerScorer;

    protected AugmentScorer(BoboSegmentReader reader, Scorer innerScorer,
        ScoreAugmentFunction func, JSONObject jsonParms) throws IOException {
      super(innerScorer.getWeight());
      _innerScorer = innerScorer;
      _func = func;
      _func.initializeReader(reader, jsonParms);
    }

    @Override
    public float score() throws IOException {
      return (_func.useInnerScore()) ? _func.newScore(_innerScorer.score(), _innerScorer.docID())
          : _func.newScore(_innerScorer.docID());
    }

    @Override
    public int advance(int target) throws IOException {
      return _innerScorer.advance(target);
    }

    @Override
    public int docID() {
      return _innerScorer.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      return _innerScorer.nextDoc();
    }

    @Override
    public int freq() throws IOException {
      return _innerScorer.freq();
    }

    @Override
    public long cost() {
      return _innerScorer.cost();
    }

  }

  private transient ScoreAugmentFunction _func;
  private transient JSONObject _jsonParam;

  public ScoreAugmentQuery(Query query, ScoreAugmentFunction func, JSONObject jsonParam)
      throws JSONException {
    super(query);
    _func = func;
    _func.initializeGlobal(jsonParam);
    _jsonParam = jsonParam;
    if (_func == null) throw new IllegalArgumentException("augment function cannot be null");
  }

  @Override
  protected Scorer createScorer(Scorer innerScorer, AtomicReaderContext context) throws IOException {
    if (context.reader() instanceof BoboSegmentReader) {
      if (innerScorer != null) {
        return new AugmentScorer((BoboSegmentReader) context.reader(), innerScorer, _func.getCopy(), _jsonParam);
      } else {
        return null;
      }
    } else {
      throw new IllegalStateException("reader not instance of " + BoboSegmentReader.class);
    }
  }
}
