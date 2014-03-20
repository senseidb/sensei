package com.senseidb.search.client.res;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.senseidb.search.client.json.CustomJsonHandler;
import com.senseidb.search.client.json.JsonField;

@CustomJsonHandler(SenseiHitJsonHandler.class)
public class SenseiHit {
  @JsonField("_uid")
  private Long uid;
  @JsonField("_docid")
  private Integer docid;
  @JsonField("_score")
  private Double score;
  @JsonField("_srcdata")
  private String srcdata;
  @JsonField("_grouphitscount")
  private Integer grouphitscount;
  private List<SenseiHit> groupHits = new ArrayList<SenseiHit>();
  private List<FieldValue> storedFields = new ArrayList<FieldValue>();

  @JsonField("_termvectors")
  private final Map<String, List<FieldTerm>> termVectors = new HashMap<String, List<FieldTerm>>();
  private Explanation explanation;
  private final Map<String, List<String>> fieldValues = new HashMap<String, List<String>>();

  @Override
  public String toString() {
    return "\n---------------------------------------------------------------------------------------------------------------\n"
        + "SenseiHit [uid="
        + uid
        + ", docid="
        + docid
        + ", score="
        + score
        + ", srcdata="
        + srcdata
        + ", grouphitscount="
        + grouphitscount
        + ", \n      groupHits="
        + groupHits
        + ", \n     storedFields="
        + storedFields
        + ", \n     termVectors="
        + termVectors
        + ", \n      explanation=" + explanation + ", \n       fieldValues=" + fieldValues + "]";
  }

  public Long getUid() {
    return uid;
  }

  public Integer getDocid() {
    return docid;
  }

  public Double getScore() {
    return score;
  }

  public String getSrcdata() {
    return srcdata;
  }

  public Integer getGrouphitscount() {
    return grouphitscount;
  }

  public List<SenseiHit> getGroupHits() {
    return groupHits;
  }

  public void setUid(Long uid) {
    this.uid = uid;
  }

  public void setDocid(Integer docid) {
    this.docid = docid;
  }

  public void setScore(Double score) {
    this.score = score;
  }

  public void setSrcdata(String srcdata) {
    this.srcdata = srcdata;
  }

  public void setGrouphitscount(Integer grouphitscount) {
    this.grouphitscount = grouphitscount;
  }

  public void setGroupHits(List<SenseiHit> groupHits) {
    this.groupHits = groupHits;
  }

  public List<FieldValue> getStoredFields() {
    return storedFields;
  }

  public void setStoredFields(List<FieldValue> storedFields) {
    this.storedFields = storedFields;
  }

  // getFieldTermFrequencies will be removed in next release
  // Please use getTermVectors instead
  @Deprecated
  public Map<String, List<TermFrequency>> getFieldTermFrequencies() {
    Map<String, List<TermFrequency>> res = new HashMap<String, List<TermFrequency>>();
    Set<Entry<String, List<FieldTerm>>> entries = termVectors.entrySet();
    for (Entry<String, List<FieldTerm>> entry : entries) {
      String field = entry.getKey();
      List<TermFrequency> tf = new ArrayList<TermFrequency>();
      for (FieldTerm term : entry.getValue()) {
        tf.add(new TermFrequency(term.getTerm(), term.getFreq()));
      }
      res.put(field, tf);
    }
    return res;
  }

  public Map<String, List<FieldTerm>> getTermVectors() {
    return termVectors;
  }

  public Explanation getExplanation() {
    return explanation;
  }

  public Map<String, List<String>> getFieldValues() {
    return fieldValues;
  }

}
