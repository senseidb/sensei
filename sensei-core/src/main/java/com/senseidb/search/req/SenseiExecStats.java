package com.senseidb.search.req;

import java.io.Serializable;

/**
 * Represents runtime execution status for a request or subset (e.g. a query executed against a single partition or
 * node)
 */
public class SenseiExecStats implements Serializable {
  private final long time;

  public SenseiExecStats(long time) {
    this.time = time;
  }

  public long getTime() {
    return time;
  }

  @Override
  public String toString() {
    return time + "ms";
  }
}
