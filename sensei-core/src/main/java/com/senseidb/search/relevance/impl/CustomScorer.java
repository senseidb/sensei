package com.senseidb.search.relevance.impl;

import java.io.IOException;

import org.apache.lucene.search.Scorer;
import org.json.JSONObject;

import com.browseengine.bobo.api.BoboSegmentReader;
import com.senseidb.search.relevance.RuntimeRelevanceFunction;
import com.senseidb.search.relevance.impl.CompilationHelper.DataTable;

public class CustomScorer extends Scorer {
  final Scorer _innerScorer;
  private RuntimeRelevanceFunction _sModifier = null;

  public CustomScorer(Scorer innerScorer, BoboSegmentReader boboReader, CustomMathModel cModel,
      DataTable _dt, JSONObject _valueJson) throws Exception {
    super(innerScorer.getWeight());
    _innerScorer = innerScorer;
    _sModifier = new RuntimeRelevanceFunction(cModel, _dt);
    _sModifier.initializeGlobal(_valueJson);
    _sModifier.initializeReader(boboReader, _valueJson);
  }

  @Override
  public float score() throws IOException {
    return _sModifier.newScore(_innerScorer.score(), docID());
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
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long cost() {
    // TODO Auto-generated method stub
    return 0;
  }

}
