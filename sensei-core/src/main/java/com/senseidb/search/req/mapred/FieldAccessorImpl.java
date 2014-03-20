package com.senseidb.search.req.mapred;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import proj.zoie.api.DocIDMapper;

import com.browseengine.bobo.api.BoboSegmentReader;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.MultiValueFacetDataCache;
import com.browseengine.bobo.facets.data.TermFloatList;
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermNumberList;
import com.browseengine.bobo.facets.data.TermShortList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.search.req.SenseiSystemInfo.SenseiFacetInfo;
import com.senseidb.search.req.mapred.impl.SingleFieldAccessorImpl;

/**
 * This class was designed to avoid polymorphism and to leverage primitive types as much as possible
 * It allows  access to the facetted data
 *
 */
@SuppressWarnings("rawtypes")
public final class FieldAccessorImpl implements FieldAccessor {
  private final Set<String> facets = new HashSet<String>();
  private final BoboSegmentReader boboIndexReader;
  private FacetDataCache lastFacetDataCache;
  private String lastFacetDataCacheName;

  private final Map<String, FacetDataCache> facetDataMap = new HashMap<String, FacetDataCache>();

  private final DocIDMapper mapper;

  public FieldAccessorImpl(Set<SenseiFacetInfo> facetInfos, BoboSegmentReader boboIndexReader,
      DocIDMapper mapper) {
    this.mapper = mapper;
    for (SenseiFacetInfo facetInfo : facetInfos) {
      facets.add(facetInfo.getName());
    }
    this.boboIndexReader = boboIndexReader;
  }

  /*
   * (non-Javadoc)
   * @see com.senseidb.search.req.mapred.FieldAccessor1#getValueCache(java.lang.String)
   */
  @Override
  public final FacetDataCache<?> getValueCache(String name) {
    if (name.equals(lastFacetDataCacheName)) {
      return lastFacetDataCache;
    }
    FacetDataCache ret = facetDataMap.get(name);
    if (ret != null) {
      lastFacetDataCache = ret;
      lastFacetDataCacheName = name;
      return ret;
    }

    Object rawFacetData = boboIndexReader.getFacetData(name);
    if (!(rawFacetData instanceof FacetDataCache)) {
      return null;
    }
    ret = (FacetDataCache) rawFacetData;
    facetDataMap.put(name, ret);
    return ret;
  }

  /*
   * (non-Javadoc)
   * @see com.senseidb.search.req.mapred.FieldAccessor1#get(java.lang.String, int)
   */
  @Override
  public final Object get(String fieldName, int docId) {
    FacetDataCache valueCache = getValueCache(fieldName);
    if (valueCache instanceof MultiValueFacetDataCache) {
      return getArray(fieldName, docId);
    }
    if (valueCache != null) {
      return valueCache.valArray.getRawValue(valueCache.orderArray.get(docId));
    }
    return getFacetHandler(fieldName).getRawFieldValues(boboIndexReader, docId);
  }

  /*
   * (non-Javadoc)
   * @see com.senseidb.search.req.mapred.FieldAccessor1#getString(java.lang.String, int)
   */
  @Override
  public final String getString(String fieldName, int docId) {
    return getFacetHandler(fieldName).getFieldValue(boboIndexReader, docId);
  }

  /*
   * (non-Javadoc)
   * @see com.senseidb.search.req.mapred.FieldAccessor1#getLong(java.lang.String, int)
   */
  @Override
  public final long getLong(String fieldName, int docId) {
    FacetDataCache valueCache = getValueCache(fieldName);
    if (valueCache != null) {
      if (valueCache.valArray instanceof TermLongList) {
        return ((TermLongList) valueCache.valArray).getPrimitiveValue(valueCache.orderArray
            .get(docId));
      } else {
        return (long) ((TermNumberList) valueCache.valArray).getDoubleValue(valueCache.orderArray
            .get(docId));
      }
    } else {
      Object value = getFacetHandler(fieldName).getRawFieldValues(boboIndexReader, docId)[0];
      if (value instanceof Long) {
        return (Long) value;
      }
      if (value instanceof Number) {
        return ((Number) value).longValue();
      }
      if (value instanceof String) {
        return Long.parseLong((String) value);
      }
      throw new UnsupportedOperationException("Class " + value.getClass()
          + " can not be converted to long");
    }
  }

  /*
   * (non-Javadoc)
   * @see com.senseidb.search.req.mapred.FieldAccessor1#getDouble(java.lang.String, int)
   */
  @Override
  public final double getDouble(String fieldName, int docId) {
    FacetDataCache valueCache = getValueCache(fieldName);
    if (valueCache != null) {
      return ((TermNumberList) valueCache.valArray)
          .getDoubleValue(valueCache.orderArray.get(docId));
    } else {
      Object value = getFacetHandler(fieldName).getRawFieldValues(boboIndexReader, docId)[0];
      if (value instanceof Double) {
        return (Double) value;
      }
      if (value instanceof Number) {
        return ((Number) value).doubleValue();
      }
      if (value instanceof String) {
        return Double.parseDouble((String) value);
      }
      throw new UnsupportedOperationException("Class " + value.getClass()
          + " can not be converted to double");
    }
  }

  /*
   * (non-Javadoc)
   * @see com.senseidb.search.req.mapred.FieldAccessor1#getShort(java.lang.String, int)
   */
  @Override
  public final short getShort(String fieldName, int docId) {
    FacetDataCache valueCache = getValueCache(fieldName);
    if (valueCache != null) {
      if (valueCache.valArray instanceof TermShortList) {
        return ((TermShortList) valueCache.valArray).getPrimitiveValue(valueCache.orderArray
            .get(docId));
      } else {
        return (short) ((TermNumberList) valueCache.valArray).getDoubleValue(valueCache.orderArray
            .get(docId));
      }
    } else {
      Object value = getFacetHandler(fieldName).getRawFieldValues(boboIndexReader, docId)[0];
      if (value instanceof Short) {
        return (Short) value;
      }
      if (value instanceof Number) {
        return ((Number) value).shortValue();
      }
      if (value instanceof String) {
        return Short.parseShort((String) value);
      }
      throw new UnsupportedOperationException("Class " + value.getClass()
          + " can not be converted to short");
    }
  }

  /*
   * (non-Javadoc)
   * @see com.senseidb.search.req.mapred.FieldAccessor1#getInteger(java.lang.String, int)
   */
  @Override
  public final int getInteger(String fieldName, int docId) {
    FacetDataCache valueCache = getValueCache(fieldName);
    if (valueCache != null) {
      if (valueCache.valArray instanceof TermIntList) {
        return ((TermIntList) valueCache.valArray).getPrimitiveValue(valueCache.orderArray
            .get(docId));
      } else {
        return (int) ((TermNumberList) valueCache.valArray).getDoubleValue(valueCache.orderArray
            .get(docId));
      }
    } else {
      Object value = getFacetHandler(fieldName).getRawFieldValues(boboIndexReader, docId)[0];
      if (value instanceof Integer) {
        return (Integer) value;
      }
      if (value instanceof Number) {
        return ((Number) value).intValue();
      }
      if (value instanceof String) {
        return Integer.parseInt((String) value);
      }
      throw new UnsupportedOperationException("Class " + value.getClass()
          + " can not be converted to int");
    }
  }

  /*
   * (non-Javadoc)
   * @see com.senseidb.search.req.mapred.FieldAccessor1#getFloat(java.lang.String, int)
   */
  @Override
  public final float getFloat(String fieldName, int docId) {
    FacetDataCache valueCache = getValueCache(fieldName);
    if (valueCache != null) {
      if (valueCache.valArray instanceof TermFloatList) {
        return ((TermFloatList) valueCache.valArray).getPrimitiveValue(valueCache.orderArray
            .get(docId));
      } else {
        return (float) ((TermNumberList) valueCache.valArray).getDoubleValue(valueCache.orderArray
            .get(docId));
      }
    } else {
      Object value = getFacetHandler(fieldName).getRawFieldValues(boboIndexReader, docId)[0];
      if (value instanceof Float) {
        return (Float) value;
      }
      if (value instanceof Number) {
        return ((Number) value).floatValue();
      }
      if (value instanceof String) {
        return Float.parseFloat((String) value);
      }
      throw new UnsupportedOperationException("Class " + value.getClass()
          + " can not be converted to float");
    }
  }

  /*
   * (non-Javadoc)
   * @see com.senseidb.search.req.mapred.FieldAccessor1#getArray(java.lang.String, int)
   */
  @Override
  public final Object[] getArray(String fieldName, int docId) {
    return getFacetHandler(fieldName).getRawFieldValues(boboIndexReader, docId);

  }

  /*
   * (non-Javadoc)
   * @see com.senseidb.search.req.mapred.FieldAccessor1#getByUID(java.lang.String, long)
   */
  @Override
  public final Object getByUID(String fieldName, long uid) {
    return get(fieldName, mapper.getDocID(uid));
  }

  /*
   * (non-Javadoc)
   * @see com.senseidb.search.req.mapred.FieldAccessor1#getStringByUID(java.lang.String, long)
   */
  @Override
  public final String getStringByUID(String fieldName, long uid) {
    return getString(fieldName, mapper.getDocID(uid));
  }

  /*
   * (non-Javadoc)
   * @see com.senseidb.search.req.mapred.FieldAccessor1#getLongByUID(java.lang.String, long)
   */
  @Override
  public final long getLongByUID(String fieldName, long uid) {
    return getLong(fieldName, mapper.getDocID(uid));
  }

  /*
   * (non-Javadoc)
   * @see com.senseidb.search.req.mapred.FieldAccessor1#getDoubleByUID(java.lang.String, long)
   */
  @Override
  public final double getDoubleByUID(String fieldName, long uid) {
    return getDouble(fieldName, mapper.getDocID(uid));
  }

  /*
   * (non-Javadoc)
   * @see com.senseidb.search.req.mapred.FieldAccessor1#getShortByUID(java.lang.String, long)
   */
  @Override
  public final short getShortByUID(String fieldName, long uid) {
    return getShort(fieldName, mapper.getDocID(uid));
  }

  /*
   * (non-Javadoc)
   * @see com.senseidb.search.req.mapred.FieldAccessor1#getIntegerByUID(java.lang.String, long)
   */
  @Override
  public final int getIntegerByUID(String fieldName, long uid) {
    return getInteger(fieldName, mapper.getDocID(uid));
  }

  /*
   * (non-Javadoc)
   * @see com.senseidb.search.req.mapred.FieldAccessor1#getFloatByUID(java.lang.String, long)
   */
  @Override
  public final float getFloatByUID(String fieldName, long uid) {
    return getFloat(fieldName, mapper.getDocID(uid));
  }

  /*
   * (non-Javadoc)
   * @see com.senseidb.search.req.mapred.FieldAccessor1#getArrayByUID(java.lang.String, long)
   */
  @Override
  public final Object[] getArrayByUID(String fieldName, long uid) {
    return getArray(fieldName, mapper.getDocID(uid));
  }

  /*
   * (non-Javadoc)
   * @see com.senseidb.search.req.mapred.FieldAccessor1#getTermValueList(java.lang.String)
   */
  @Override
  public final TermValueList<?> getTermValueList(String fieldName) {
    FacetDataCache valueCache = getValueCache(fieldName);
    if (valueCache == null) {
      return null;
    }
    return valueCache.valArray;
  }

  private String lastFacetHandlerName;
  private FacetHandler lastFacetHandler;

  /*
   * (non-Javadoc)
   * @see com.senseidb.search.req.mapred.FieldAccessor1#getFacetHandler(java.lang.String)
   */
  @Override
  public final FacetHandler<?> getFacetHandler(String facetName) {
    if (!facetName.equals(lastFacetHandlerName)) {
      lastFacetHandler = boboIndexReader.getFacetHandler(facetName);
      lastFacetHandlerName = facetName;
    }
    if (lastFacetHandler == null) {
      throw new IllegalStateException("The facetHandler - " + facetName
          + " is not defined in the schema");
    }
    return lastFacetHandler;
  }

  /*
   * (non-Javadoc)
   * @see com.senseidb.search.req.mapred.FieldAccessor1#getBoboIndexReader()
   */
  @Override
  public BoboSegmentReader getBoboIndexReader() {
    return boboIndexReader;
  }

  /*
   * (non-Javadoc)
   * @see com.senseidb.search.req.mapred.FieldAccessor1#getMapper()
   */
  @Override
  public DocIDMapper getMapper() {
    return mapper;
  }

  private final Map<String, SingleFieldAccessor> singleFieldAccessors = new HashMap<String, SingleFieldAccessor>();

  @Override
  public SingleFieldAccessor getSingleFieldAccessor(String facetName) {
    if (!singleFieldAccessors.containsKey(facetName)) {
      singleFieldAccessors.put(facetName,
        new SingleFieldAccessorImpl((FacetDataCache) boboIndexReader.getFacetData(facetName),
            boboIndexReader.getFacetHandler(facetName), boboIndexReader));
    }
    return singleFieldAccessors.get(facetName);
  }

}
