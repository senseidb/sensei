package com.senseidb.search.client.res;

public class SenseiRequestExecStatus {
  private long time;
  private int resultCount;

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public int getResultCount() {
    return resultCount;
  }

  public void setResultCount(int resultCount) {
    this.resultCount = resultCount;
  }

  @Override
  public String toString() {
    return resultCount + " results in " + getTime() + "ms";
  }
}
