package com.senseidb.indexing;

import org.json.JSONObject;

public abstract class DataSourceFilter<D> {

  protected abstract JSONObject doFilter(D data) throws Exception;

  public JSONObject filter(D data) throws Exception {
    JSONObject obj = doFilter(data);
    return obj;
  }
}
