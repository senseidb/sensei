package com.senseidb.conf;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.indexing.DefaultSenseiInterpreter;
import com.senseidb.indexing.MetaType;

public class SenseiSchema {
  public static final String EVENT_TYPE_FIELD = "type";
  public static final String EVENT_TYPE_UPDATE = "update";
  public static final String EVENT_TYPE_DELETE = "delete";
  public static final String EVENT_TYPE_SKIP = "skip";

  private static Logger logger = Logger.getLogger(SenseiSchema.class);

  private String _uidField;
  private String _srcDataField;
  private boolean _compressSrcData;
  private final List<FacetDefinition> facets = new ArrayList<FacetDefinition>();

  public static class FieldDefinition {
    public Format formatter;
    public boolean isMeta;
    public FieldType fieldType;
    public String fromField;
    public boolean isMulti;
    public boolean isActivity;
    public String delim = ",";
    public Store store;
    public Class<?> type = null;
    public String name;
  }

  public static class FacetDefinition {
    public String name;
    public String type;
    public String column;
    public Boolean dynamic;
    public Map<String, List<String>> params;
    public Set<String> dependSet = new HashSet<String>();

    public static FacetDefinition valueOf(JSONObject facet) {
      try {
        FacetDefinition ret = new FacetDefinition();
        ret.name = facet.getString("name");
        ret.type = facet.getString("type");
        ret.column = facet.optString("column", ret.name);
        JSONArray depends = facet.optJSONArray("depends");
        if (depends != null) {
          for (int i = 0; i < depends.length(); ++i) {
            String dep = depends.getString(i).trim();
            if (!dep.isEmpty()) {
              ret.dependSet.add(dep);
            }
          }
        }

        JSONArray paramList = facet.optJSONArray("params");
        ret.params = SenseiFacetHandlerBuilder.parseParams(paramList);
        return ret;
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  private SenseiSchema() {
  }

  public String getUidField() {
    return _uidField;
  }

  public String getSrcDataField() {
    return _srcDataField;
  }

  public boolean isCompressSrcData() {
    return _compressSrcData;
  }

  public void setCompressSrcData(boolean _compressSrcData) {
    this._compressSrcData = _compressSrcData;
  }

  public Map<String, FieldDefinition> getFieldDefMap() {
    return _fieldDefMap;
  }

  private Map<String, FieldDefinition> _fieldDefMap;
  private static JSONObject schemaObj;

  public static SenseiSchema build(JSONObject schemaObj) throws JSONException,
      ConfigurationException {

    SenseiSchema schema = new SenseiSchema();
    schema.setSchemaObj(schemaObj);
    schema._fieldDefMap = new HashMap<String, FieldDefinition>();
    JSONObject tableElem = schemaObj.optJSONObject("table");
    if (tableElem == null) {
      throw new ConfigurationException("empty schema");
    }

    schema._uidField = tableElem.getString("uid");
    schema._srcDataField = tableElem.optString("src-data-field", "src_data");
    schema._compressSrcData = tableElem.optBoolean("compress-src-data", true);

    JSONArray columns = tableElem.optJSONArray("columns");

    int count = 0;
    if (columns != null) {
      count = columns.length();
    }

    for (int i = 0; i < count; ++i) {

      JSONObject column = columns.getJSONObject(i);
      try {
        String n = column.getString("name");
        String t = column.getString("type");
        String frm = column.optString("from", "");
        String storeString = column.optString("store", "NO");
        storeString = storeString.toUpperCase();

        FieldDefinition fdef = new FieldDefinition();
        fdef.formatter = null;
        fdef.fromField = frm.length() > 0 ? frm : n;
        if (storeString.equals("NO")) {
          fdef.store = Store.NO;
        } else if (storeString.equals("YES")) {
          fdef.store = Store.YES;
        } else {
          throw new ConfigurationException("Invalid indexing parameter specification");
        }
        fdef.isMeta = true;

        fdef.isMulti = column.optBoolean("multi");
        fdef.isActivity = column.optBoolean("activity");
        fdef.name = n;
        String delimString = column.optString("delimiter");
        if (delimString != null && delimString.trim().length() > 0) {
          fdef.delim = delimString;
        }

        schema._fieldDefMap.put(n, fdef);

        if (t.equals("int")) {
          MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(int.class);
          String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
          fdef.formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.US));
          fdef.type = int.class;
        } else if (t.equals("short")) {
          MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(short.class);
          String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
          fdef.formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.US));
          fdef.type = int.class;
        } else if (t.equals("long")) {
          MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(long.class);
          String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
          fdef.formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.US));
          fdef.type = long.class;
        } else if (t.equals("float")) {
          MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(float.class);
          String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
          fdef.formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.US));
          fdef.type = double.class;
        } else if (t.equals("double")) {
          MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(double.class);
          String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
          fdef.formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.US));
          fdef.type = double.class;
        } else if (t.equals("char")) {
          fdef.formatter = null;
          fdef.type = char.class;
        } else if (t.equals("string")) {
          fdef.formatter = null;
          fdef.type = String.class;
        } else if (t.equals("boolean")) {
          fdef.formatter = null;
          fdef.type = boolean.class;
        } else if (t.equals("date")) {
          String f = "";
          try {
            f = column.optString("format");
          } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
          }
          if (f.isEmpty()) throw new ConfigurationException("Date format cannot be empty.");

          fdef.formatter = new SimpleDateFormat(f);
          fdef.type = Date.class;
        } else if (t.equals("text")) {
          fdef.isMeta = false;
          FieldType fieldType = new FieldType();
          String idxString = column.optString("index", "ANALYZED");
          idxString = idxString.toUpperCase();
          if (idxString.equals("NO")) {
            fieldType.setIndexed(false);
            fieldType.setTokenized(false);
            fieldType.setOmitNorms(true);
          } else if (idxString.equals("ANALYZED") || idxString.equals("TOKENIZED")) {
            fieldType.setIndexed(true);
            fieldType.setTokenized(true);
            fieldType.setOmitNorms(false);
          } else if (idxString.equals("NOT_ANALYZED") || idxString.equals("UN_TOKENIZED")) {
            fieldType.setIndexed(true);
            fieldType.setTokenized(false);
            fieldType.setOmitNorms(false);
          } else if (idxString.equals("NOT_ANALYZED_NO_NORMS") || idxString.equals("NO_NORMS")) {
            fieldType.setIndexed(true);
            fieldType.setTokenized(false);
            fieldType.setOmitNorms(true);
          } else if (idxString.equals("ANALYZED_NO_NORMS")) {
            fieldType.setIndexed(true);
            fieldType.setTokenized(true);
            fieldType.setOmitNorms(true);
          } else {
            throw new ConfigurationException("Invalid indexing parameter specification");
          }

          if (storeString.equals("NO")) {
            fieldType.setStored(false);
          } else if (storeString.equals("YES")) {
            fieldType.setStored(true);
          } else {
            throw new ConfigurationException("Invalid indexing parameter specification");
          }

          String tvString = column.optString("termvector", "NO");
          tvString = tvString.toUpperCase();
          if (tvString.equals("NO")) {
            fieldType.setStoreTermVectors(false);
            fieldType.setStoreTermVectorOffsets(false);
            fieldType.setStoreTermVectorPositions(false);
          } else if (tvString.equals("YES")) {
            fieldType.setStoreTermVectors(true);
            fieldType.setStoreTermVectorOffsets(false);
            fieldType.setStoreTermVectorPositions(false);
          } else if (tvString.equals("WITH_POSITIONS")) {
            fieldType.setStoreTermVectors(true);
            fieldType.setStoreTermVectorOffsets(false);
            fieldType.setStoreTermVectorPositions(true);
          } else if (tvString.equals("WITH_OFFSETS")) {
            fieldType.setStoreTermVectors(true);
            fieldType.setStoreTermVectorOffsets(true);
            fieldType.setStoreTermVectorPositions(false);
          } else if (tvString.equals("WITH_POSITIONS_OFFSETS")) {
            fieldType.setStoreTermVectors(true);
            fieldType.setStoreTermVectorOffsets(true);
            fieldType.setStoreTermVectorPositions(true);
          } else {
            throw new ConfigurationException("Invalid indexing parameter specification");
          }
          fdef.fieldType = fieldType;
        }
      } catch (Exception e) {
        throw new ConfigurationException("Error parsing schema: " + column, e);
      }
    }
    JSONArray facetsList = schemaObj.optJSONArray("facets");
    if (facetsList != null) {
      for (int i = 0; i < facetsList.length(); i++) {
        JSONObject facet = facetsList.optJSONObject(i);
        if (facet != null) {
          schema.facets.add(FacetDefinition.valueOf(facet));
        }
      }
    }
    return schema;
  }

  public List<FacetDefinition> getFacets() {
    return facets;
  }

  public JSONObject getSchemaObj() {
    return schemaObj;
  }

  public void setSchemaObj(JSONObject schemaObj) {
    SenseiSchema.schemaObj = schemaObj;
  }

}
