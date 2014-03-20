package com.senseidb.search.req.mapred;

import proj.zoie.api.DocIDMapper;

import com.browseengine.bobo.api.BoboSegmentReader;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.TermValueList;

public interface FieldAccessor {

  public FacetDataCache<?> getValueCache(String name);

  /**
   * Get facet value for the document
   * @param fieldName
   * @param docId
   * @return
   */
  public Object get(String fieldName, int docId);

  /**
   * Get string facet value for the document
   * @param fieldName
   * @param docId
   * @return
   */
  public String getString(String fieldName, int docId);

  /**
   * Get long  facet value for the document
   * @param fieldName
   * @param docId
   * @return
   */
  public long getLong(String fieldName, int docId);

  /**
   * Get double  facet value for the document
   * @param fieldName
   * @param docId
   * @return
   */
  public double getDouble(String fieldName, int docId);

  /**
   * Get short  facet value for the document
   * @param fieldName
   * @param docId
   * @return
   */
  public short getShort(String fieldName, int docId);

  /**
   * Get integer  facet value for the document
   * @param fieldName
   * @param docId
   * @return
   */
  public int getInteger(String fieldName, int docId);

  /**
   * Get float  facet value for the document
   * @param fieldName
   * @param docId
   * @return
   */
  public float getFloat(String fieldName, int docId);

  /**
   * Get array  facet value for the document
   * @param fieldName
   * @param docId
   * @return
   */
  public Object[] getArray(String fieldName, int docId);

  public Object getByUID(String fieldName, long uid);

  public String getStringByUID(String fieldName, long uid);

  public long getLongByUID(String fieldName, long uid);

  public double getDoubleByUID(String fieldName, long uid);

  public short getShortByUID(String fieldName, long uid);

  public int getIntegerByUID(String fieldName, long uid);

  public float getFloatByUID(String fieldName, long uid);

  public Object[] getArrayByUID(String fieldName, long uid);

  public TermValueList<?> getTermValueList(String fieldName);

  /**
   * @param facetName
   * @return
   * @throws IllegalStateException if the facet can not be found
   */
  public FacetHandler<?> getFacetHandler(String facetName);

  public BoboSegmentReader getBoboIndexReader();

  /**
   * Returns the docIdtoUID mapper
   * @return
   */
  public DocIDMapper getMapper();

  public SingleFieldAccessor getSingleFieldAccessor(String facetName);

}