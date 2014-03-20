package com.senseidb.search.req.mapred;

public interface SingleFieldAccessor {
  /**
   * Get facet value for the document
   * @param fieldName
   * @param docId
   * @return
   */
  public Object get(int docId);

  /**
   * Get string facet value for the document
   * @param fieldName
   * @param docId
   * @return
   */
  public String getString(int docId);

  /**
   * Get long  facet value for the document
   * @param fieldName
   * @param docId
   * @return
   */
  public long getLong(int docId);

  /**
   * Get double  facet value for the document
   * @param fieldName
   * @param docId
   * @return
   */
  public double getDouble(int docId);

  /**
   * Get short  facet value for the document
   * @param fieldName
   * @param docId
   * @return
   */
  public short getShort(int docId);

  /**
   * Get integer  facet value for the document
   * @param fieldName
   * @param docId
   * @return
   */
  public int getInteger(int docId);

  /**
   * Get float  facet value for the document
   * @param fieldName
   * @param docId
   * @return
   */
  public float getFloat(int docId);

  /**
   * Get array  facet value for the document
   * @param fieldName
   * @param docId
   * @return
   */
  public Object[] getArray(int docId);

  public int getDictionaryId(int docId);
}
