package com.senseidb.search.req;

import java.io.Serializable;

import org.apache.lucene.search.SortField;

public class SerializableSortField implements Serializable {
  /**
   *
   */
  private static final long serialVersionUID = 1L;

  private final String _field;
  private final SortField.Type _type;
  private boolean _reverse = false;

  public SerializableSortField(SortField sortField) {
    _field = sortField.getField();
    _type = sortField.getType();
    _reverse = sortField.getReverse();
  }

  public SortField getSortField() {
    return new SortField(_field, _type, _reverse);
  }

}