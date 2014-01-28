package com.senseidb.search.req;

/**
 * Contains runtime and number of results returned for execution of a request or subset of a request.
 */
public class SenseiRequestExecStats extends SenseiExecStats {

  private final int resultCount;

  public SenseiRequestExecStats(long time, int resultCount) {
    super(time);
    this.resultCount = resultCount;
  }

  public int getResultCount() {
    return resultCount;
  }

  @Override
  public String toString() {
    return resultCount + " results in " + getTime() + "ms";
  }
}
