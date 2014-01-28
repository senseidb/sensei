package com.senseidb.search.req;

import java.io.Serializable;
import java.util.Map;

public interface AbstractSenseiResult<T extends SenseiExecStats> extends Serializable {
  public abstract long getTime();

  public abstract void setTime(long searchTimeMillis);
  public T getExecStats();
  public void addError(SenseiError error);
  public Map<String, T> getPartitionExecStats();
  public void setPartitionExecStats(Map<String, T> partitionExecStats);
}
