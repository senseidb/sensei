package com.senseidb.search.client.res;

import java.util.List;

public class FieldTerm {
  private String term;
  private Integer freq;
  private List<Integer> positions;
  private List<Integer> startOffsets;
  private List<Integer> endOffsets;

  public String getTerm() {
    return term;
  }

  public void setTerm(String term) {
    this.term = term;
  }

  public int getFreq() {
    return freq;
  }

  public void setFreq(int freq) {
    this.freq = freq;
  }

  public List<Integer> getPositions() {
    return positions;
  }

  public void setPositions(List<Integer> positions) {
    this.positions = positions;
  }

  public List<Integer> getStartOffsets() {
    return startOffsets;
  }

  public void setStartOffset(List<Integer> startOffsets) {
    this.startOffsets = startOffsets;
  }

  public List<Integer> getEndOffsets() {
    return endOffsets;
  }

  public void setEndOffsets(List<Integer> endOffsets) {
    this.endOffsets = endOffsets;
  }

  public FieldTerm() {
    // TODO Auto-generated constructor stub
  }

  @Override
  public String toString() {
    return "[term=" + term + ", freq=" + freq + ", positions=" + positions
        + ", startOffsets=" + startOffsets + ", endOffsets=" + endOffsets + "]";
  }
}
