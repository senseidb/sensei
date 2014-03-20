package com.senseidb.search.req;

final public class StringQuery extends SenseiQuery {
  /**
   *
   */
  private static final long serialVersionUID = 1L;

  public StringQuery(String q) {
    super(q.getBytes(utf8Charset));
  }

}
