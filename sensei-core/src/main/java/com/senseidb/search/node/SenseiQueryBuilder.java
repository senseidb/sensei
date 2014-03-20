package com.senseidb.search.node;

import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;

public interface SenseiQueryBuilder {
  Query buildQuery() throws ParseException;

  Filter buildFilter() throws ParseException;
}
