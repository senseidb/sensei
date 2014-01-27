package com.senseidb.servlet;

import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_COUNT;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_DYNAMIC_INIT;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_DYNAMIC_TYPE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_DYNAMIC_TYPE_BOOL;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_DYNAMIC_TYPE_BYTEARRAY;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_DYNAMIC_TYPE_DOUBLE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_DYNAMIC_TYPE_INT;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_DYNAMIC_TYPE_LONG;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_DYNAMIC_TYPE_STRING;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_DYNAMIC_VAL;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_FACET;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_FACET_EXPAND;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_FACET_MAX;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_FACET_MINHIT;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_FACET_ORDER;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_FACET_ORDER_HITS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_FACET_ORDER_VAL;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_FETCH_STORED;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_FETCH_TERMVECTOR;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_FIELDS_TO_FETCH;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_GROUP_BY;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_MAX_PER_GROUP;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_OFFSET;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_PARTITIONS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_QUERY;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_QUERY_PARAM;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_ERRORS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_ERROR_CODE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_ERROR_MESSAGE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_ERROR_TYPE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_FACETS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_FACET_INFO_COUNT;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_FACET_INFO_SELECTED;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_FACET_INFO_VALUE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HITS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HITS_EXPL_DESC;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HITS_EXPL_DETAILS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HITS_EXPL_VALUE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_DOCID;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_EXPLANATION;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_GROUPHITS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_GROUPHITSCOUNT;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_GROUPFIELD;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_GROUPVALUE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_SCORE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_SRC_DATA;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_STORED_FIELDS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_STORED_FIELDS_NAME;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_STORED_FIELDS_VALUE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_TERMVECTORS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_UID;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_NUMGROUPS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_NUMHITS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_PARSEDQUERY;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_SELECT_LIST;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_TID;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_TIME;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_TOTALDOCS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_ROUTE_PARAM;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SELECT;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SELECT_NOT;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SELECT_OP;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SELECT_OP_AND;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SELECT_OP_OR;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SELECT_PROP;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SELECT_VAL;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SHOW_EXPLAIN;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SORT;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SORT_DESC;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SORT_DOC;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SORT_DOC_REVERSE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SORT_SCORE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SORT_SCORE_REVERSE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_CLUSTERINFO;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_CLUSTERINFO_ADMINLINK;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_CLUSTERINFO_ID;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_CLUSTERINFO_NODELINK;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_CLUSTERINFO_PARTITIONS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_FACETS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_FACETS_NAME;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_FACETS_PROPS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_FACETS_RUNTIME;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_LASTMODIFIED;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_NUMDOCS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_SCHEMA;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_VERSION;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.senseidb.search.req.ErrorType;
import com.senseidb.util.Pair;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.DataConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.search.SortField;
import org.json.JSONException;
import org.json.JSONObject;

import proj.zoie.api.indexing.AbstractZoieIndexable;

import com.browseengine.bobo.api.BrowseFacet;
import com.browseengine.bobo.api.BrowseHit.BoboTerm;
import com.browseengine.bobo.api.BrowseHit.SerializableExplanation;
import com.browseengine.bobo.api.BrowseHit.SerializableField;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.BrowseSelection.ValueOperation;
import com.browseengine.bobo.api.FacetAccessible;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.api.FacetSpec.FacetSortSpec;
import com.browseengine.bobo.facets.DefaultFacetHandlerInitializerParam;
import com.senseidb.conf.SenseiFacetHandlerBuilder;
import com.senseidb.search.req.SenseiError;
import com.senseidb.search.req.SenseiHit;
import com.senseidb.search.req.SenseiJSONQuery;
import com.senseidb.search.req.SenseiQuery;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.search.req.SenseiSystemInfo;
import com.senseidb.util.JSONUtil.FastJSONObject;
import com.senseidb.util.RequestConverter;

public class DefaultSenseiJSONServlet extends AbstractSenseiRestServlet {

  private static final String PARAM_RESULT_MAP_REDUCE = "mapReduceResult";

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  private static Logger logger = Logger.getLogger(DefaultSenseiJSONServlet.class);

  public static void writeJSONExpl(JsonGenerator jsonGenerator, SerializableExplanation expl) throws IOException {
    jsonGenerator.writeStartObject();
    jsonGenerator.writeNumberField(PARAM_RESULT_HITS_EXPL_VALUE, expl.getValue());
    String descr = expl.getDescription();
    jsonGenerator.writeStringField(PARAM_RESULT_HITS_EXPL_DESC, descr == null ? "" : descr);
    SerializableExplanation[] details = expl.getDetails();
    if (details != null)
    {
      jsonGenerator.writeArrayFieldStart(PARAM_RESULT_HITS_EXPL_DETAILS);
      for (SerializableExplanation detail : details) {
        if (detail != null) {
          writeJSONExpl(jsonGenerator, detail);
        }
      }
      jsonGenerator.writeEndArray();
    }
    jsonGenerator.writeEndObject();
  }

  public static void writeJSONFacets(JsonGenerator jsonGenerator, Map<String, FacetAccessible> facetValueMap,
                                     SenseiRequest req) throws IOException {
    jsonGenerator.writeObjectFieldStart(PARAM_RESULT_FACETS);
    if (facetValueMap != null)
    {
      Set<Entry<String, FacetAccessible>> entrySet = facetValueMap.entrySet();

      for (Entry<String, FacetAccessible> entry : entrySet) {
        String fieldname = entry.getKey();
        jsonGenerator.writeArrayFieldStart(fieldname);

        BrowseSelection sel = req.getSelection(fieldname);
        HashSet<String> selectedVals = new HashSet<String>();
        if (sel != null) {
          String[] vals = sel.getValues();
          if (vals != null && vals.length > 0) {
            selectedVals.addAll(Arrays.asList(vals));
          }
        }

        FacetAccessible facetAccessible = entry.getValue();
        List<BrowseFacet> facetList = facetAccessible.getFacets();

        ArrayList<Pair<BrowseFacet, Boolean>> allFacets = new ArrayList<Pair<BrowseFacet, Boolean>>(facetList.size());

        for (BrowseFacet f : facetList) {
          String fval = f.getValue();
          if (fval != null && fval.length() > 0)
          {
            allFacets.add(new Pair<BrowseFacet, Boolean>(f, selectedVals.remove(fval)));
          }
        }

        if (selectedVals.size() > 0) {
          // selected vals did not make it in top n
          for (String selectedVal : selectedVals) {
            if (selectedVal != null && selectedVal.length() > 0) {
              BrowseFacet selectedFacetVal = facetAccessible.getFacet(selectedVal);

              if (selectedFacetVal != null) {
                allFacets.add(new Pair<BrowseFacet, Boolean>(selectedFacetVal, true));
              } else {
                allFacets.add(new Pair<BrowseFacet, Boolean>(new BrowseFacet(selectedVal, 0), false));
              }
            }
          }

          FacetSpec fspec = req.getFacetSpec(fieldname);
          assert fspec != null;
          sortFacets(fieldname, allFacets, fspec);
        }

        for (Pair<BrowseFacet, Boolean> facetSelectedPair : allFacets) {
          jsonGenerator.writeStartObject();
          jsonGenerator.writeNumberField(PARAM_RESULT_FACET_INFO_COUNT,
            facetSelectedPair.getFirst().getFacetValueHitCount());
          jsonGenerator.writeStringField(PARAM_RESULT_FACET_INFO_VALUE, facetSelectedPair.getFirst().getValue());
          jsonGenerator.writeBooleanField(PARAM_RESULT_FACET_INFO_SELECTED, facetSelectedPair.getSecond());
          jsonGenerator.writeEndObject();
        }

        jsonGenerator.writeEndArray();
      }
    }
    jsonGenerator.writeEndObject();
  }

  private static void sortFacets(String fieldName, ArrayList<Pair<BrowseFacet, Boolean>> facets, FacetSpec fspec) {
    FacetSortSpec sortSpec = fspec.getOrderBy();
    if (FacetSortSpec.OrderHitsDesc.equals(sortSpec)) {
      Collections.sort(facets, new Comparator<Pair<BrowseFacet, Boolean>>() {
        @Override
        public int compare(Pair<BrowseFacet, Boolean> o1, Pair<BrowseFacet, Boolean> o2) {
          try {
            int c1 = o1.getFirst().getFacetValueHitCount();
            int c2 = o2.getFirst().getFacetValueHitCount();
            int val = c2 - c1;
            if (val == 0) {
              String s1 = o1.getFirst().getValue();
              String s2 = o2.getFirst().getValue();
              val = s1.compareTo(s2);
            }
            return val;
          } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return 0;
          }
        }
      });
    } else if (FacetSortSpec.OrderValueAsc.equals(sortSpec)) {
      Collections.sort(facets, new Comparator<Pair<BrowseFacet, Boolean>>() {
        @Override
        public int compare(Pair<BrowseFacet, Boolean> o1, Pair<BrowseFacet, Boolean> o2) {
          try {
            String s1 = o1.getFirst().getValue();
            String s2 = o2.getFirst().getValue();
            return s1.compareTo(s2);
          } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return 0;
          }
        }
      });
    } else {
      throw new IllegalStateException(fieldName + " sorting is not supported");
    }
  }

  @Override
  protected void writeResult(HttpServletRequest httpReq, OutputStream ostream, SenseiRequest req,
                             SenseiResult res) throws IOException {
    JsonGenerator jsonGenerator = JSON_FACTORY.createGenerator(ostream);
    jsonGenerator.setCodec(OBJECT_MAPPER);

    initJsonp(httpReq, jsonGenerator);

    writeJSONResult(jsonGenerator, req, res);

    endJsonp(httpReq, jsonGenerator);
    jsonGenerator.flush();
  }

  private boolean initJsonp(HttpServletRequest httpReq, JsonGenerator jsonGenerator) throws IOException {
    String callback = httpReq.getParameter("callback");
    if (callback != null) {
      jsonGenerator.writeRaw(callback);
      jsonGenerator.writeRaw('(');
      return true;
    }
    return false;
  }

  private boolean endJsonp(HttpServletRequest httpReq, JsonGenerator jsonGenerator) throws IOException {
    if (httpReq.getParameter("callback") != null) {
      jsonGenerator.writeRaw(')');
      return true;
    }
    return false;
  }

  public static void writeJSONHits(JsonGenerator jsonGenerator, SenseiRequest req, SenseiHit[] hits) throws IOException {
    Set<String> selectSet = req.getSelectSet();

    jsonGenerator.writeStartArray();
    for (SenseiHit hit : hits) {
      Map<String, String[]> fieldMap = hit.getFieldValues();

      jsonGenerator.writeStartObject();
      if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_UID)) {
        jsonGenerator.writeNumberField(PARAM_RESULT_HIT_UID, hit.getUID());
      }
      if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_DOCID)) {
        jsonGenerator.writeNumberField(PARAM_RESULT_HIT_DOCID, hit.getDocid());
      }
      if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_SCORE)) {
        jsonGenerator.writeNumberField(PARAM_RESULT_HIT_SCORE, hit.getScore());
      }
      if ((selectSet == null || selectSet.contains(PARAM_RESULT_HIT_GROUPFIELD)) && hit.getGroupField() != null) {
        jsonGenerator.writeStringField(PARAM_RESULT_HIT_GROUPFIELD, hit.getGroupField());
      }
      if ((selectSet == null || selectSet.contains(PARAM_RESULT_HIT_GROUPVALUE)) && hit.getGroupValue() != null) {
        jsonGenerator.writeStringField(PARAM_RESULT_HIT_GROUPVALUE, hit.getGroupValue());
      }
      if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_GROUPHITSCOUNT)) {
        jsonGenerator.writeNumberField(PARAM_RESULT_HIT_GROUPHITSCOUNT, hit.getGroupHitsCount());
      }
      if (hit.getGroupHits() != null && hit.getGroupHits().length > 0) {
        jsonGenerator.writeFieldName(PARAM_RESULT_HIT_GROUPHITS);
        writeJSONHits(jsonGenerator, req, hit.getSenseiGroupHits());
      }

      // get fetchStored even if request does not have it because it could be set at the
      // federated broker level
      if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_SRC_DATA) || 
          req.isFetchStoredFields() || hit.getSrcData() != null)
      {
        jsonGenerator.writeStringField(PARAM_RESULT_HIT_SRC_DATA, hit.getSrcData());
      }
      if (fieldMap != null) {
        Set<Entry<String, String[]>> entries = fieldMap.entrySet();
        for (Entry<String, String[]> entry : entries) {
          String key = entry.getKey();
          if (key.equals(PARAM_RESULT_HIT_UID)) {
            // UID is already set.
            continue;
          }
          if (key.equals(SenseiFacetHandlerBuilder.SUM_GROUP_BY_FACET_NAME)) {
            // UID is already set.
            continue;
          }
          String[] vals = entry.getValue();

          if (selectSet == null || selectSet.contains(key)) {
            if (vals != null) {
              jsonGenerator.writeObjectField(key, vals);
            }
          }
        }
      }

      List<SerializableField> fields = hit.getStoredFields();
      if (fields != null) {
        if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_STORED_FIELDS)) {
          jsonGenerator.writeArrayFieldStart(PARAM_RESULT_HIT_STORED_FIELDS);
          for (SerializableField field : fields) {
            if (!field.name().equals(AbstractZoieIndexable.DOCUMENT_STORE_FIELD) &&
                (req.getStoredFieldsToFetch() != null
                 && !req.getStoredFieldsToFetch().contains(field.name()))) {
              continue;
            }
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField(PARAM_RESULT_HIT_STORED_FIELDS_NAME, field.name());
            jsonGenerator.writeStringField(PARAM_RESULT_HIT_STORED_FIELDS_VALUE, field.stringValue());
            jsonGenerator.writeEndObject();
          }
          jsonGenerator.writeEndArray();
        }
      }

      Map<String, List<BoboTerm>> tvMap = hit.getTermVectorMap();
      if (tvMap != null && tvMap.size() > 0) {
        if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_TERMVECTORS)) {
          jsonGenerator.writeObjectFieldStart(PARAM_RESULT_HIT_TERMVECTORS);

          Set<Entry<String, List<BoboTerm>>> entries = tvMap.entrySet();
          for (Entry<String, List<BoboTerm>> entry : entries) {
            jsonGenerator.writeArrayFieldStart(entry.getKey());
            for (BoboTerm term : entry.getValue()) {
              jsonGenerator.writeStringField("term", term.term);
              jsonGenerator.writeNumberField("freq", term.freq);
              jsonGenerator.writeObjectField("positions", term.positions);
              jsonGenerator.writeObjectField("startOffsets", term.startOffsets);
              jsonGenerator.writeObjectField("endOffsets", term.endOffsets);
            }
            jsonGenerator.writeEndArray();
          }

          jsonGenerator.writeEndObject();
        }
      }

      SerializableExplanation expl = hit.getExplanation();
      if (expl != null) {
        if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_EXPLANATION)) {
          jsonGenerator.writeFieldName(PARAM_RESULT_HIT_EXPLANATION);
          writeJSONExpl(jsonGenerator, expl);
        }
      }

      jsonGenerator.writeEndObject();
    }
    jsonGenerator.writeEndArray();
  }

  public static void writeJSONResult(JsonGenerator jsonGenerator, SenseiRequest req, SenseiResult res) throws IOException {
    jsonGenerator.writeStartObject();

    jsonGenerator.writeNumberField(PARAM_RESULT_TID, res.getTid());
    jsonGenerator.writeNumberField(PARAM_RESULT_TOTALDOCS, res.getTotalDocsLong());
    jsonGenerator.writeNumberField(PARAM_RESULT_NUMHITS, res.getNumHitsLong());
    jsonGenerator.writeNumberField(PARAM_RESULT_NUMGROUPS, res.getNumGroupsLong());
    jsonGenerator.writeStringField(PARAM_RESULT_PARSEDQUERY, res.getParsedQuery());
    
    SenseiHit[] hits = res.getSenseiHits();
    jsonGenerator.writeFieldName(PARAM_RESULT_HITS);
    writeJSONHits(jsonGenerator, req, hits);

    List<String> selectList = req.getSelectList();
    if (selectList != null) {
      jsonGenerator.writeArrayFieldStart(PARAM_RESULT_SELECT_LIST);
      for (String col: selectList) {
        jsonGenerator.writeString(col);
      }
      jsonGenerator.writeEndArray();
    }

    jsonGenerator.writeNumberField(PARAM_RESULT_TIME, res.getTime());

    writeJSONFacets(jsonGenerator, res.getFacetMap(), req);

    List<SenseiError> senseiErrors = res.getErrors();

    String mapReduceResultString = null;
    if (req.getMapReduceFunction() != null && res.getMapReduceResult() != null) {
      JSONObject mapReduceResult = null;
      try {
        mapReduceResult = req.getMapReduceFunction().render(
        res.getMapReduceResult().getReduceResult());
        if (mapReduceResult != null) {
          if (!(mapReduceResult instanceof FastJSONObject) && mapReduceResult != null) {
            mapReduceResult = new FastJSONObject(mapReduceResult.toString());
          }
          mapReduceResultString = mapReduceResult.toString();
        }      
      } catch (Exception e) {
        senseiErrors = new ArrayList<SenseiError>(senseiErrors);
        senseiErrors.add(new SenseiError(e.getMessage(), ErrorType.JsonParsingError));
      }
    }
    if (mapReduceResultString != null) {
      jsonGenerator.writeFieldName(PARAM_RESULT_MAP_REDUCE);
      jsonGenerator.writeRawValue(mapReduceResultString);
    }
    writeJSONErrors(jsonGenerator, senseiErrors);

    jsonGenerator.writeEndObject();
    jsonGenerator.flush();
  }

  private static void writeJSONErrors(JsonGenerator jsonGenerator, List<SenseiError> errors) throws IOException {
    jsonGenerator.writeArrayFieldStart(PARAM_RESULT_ERRORS);
    for (SenseiError error : errors) {
      jsonGenerator.writeStartObject();
      jsonGenerator.writeStringField(PARAM_RESULT_ERROR_MESSAGE, error.getMessage());
      jsonGenerator.writeStringField(PARAM_RESULT_ERROR_TYPE, error.getErrorType().name());
      jsonGenerator.writeNumberField(PARAM_RESULT_ERROR_CODE, error.getErrorCode());
      jsonGenerator.writeEndObject();
    }
    jsonGenerator.writeEndArray();

    if (errors.size() > 0) {
      jsonGenerator.writeNumberField(PARAM_RESULT_ERROR_CODE, errors.get(0).getErrorCode());
    } else {
      jsonGenerator.writeNumberField(PARAM_RESULT_ERROR_CODE, 0);
    }
  }

  private static SenseiQuery buildSenseiQuery(DataConfiguration params) {
    SenseiQuery sq;
    String query = params.getString(PARAM_QUERY, null);

    JSONObject qjson = new FastJSONObject();
    if (query != null && query.length() > 0) {
      try {
        qjson.put("query", query);
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
      }
    }

    try {
      String[] qparams = params.getStringArray(PARAM_QUERY_PARAM);
      for (String qparam : qparams) {
        qparam = qparam.trim();
        if (qparam.length() == 0) continue;
        String[] parts = qparam.split(":", 2);
        if (parts.length == 2) {
          qjson.put(parts[0], parts[1]);
        }
      }
    } catch (JSONException jse) {
      logger.error(jse.getMessage(), jse);
    }

    sq = new SenseiJSONQuery(qjson);
    return sq;
  }

  @Override
  protected SenseiRequest buildSenseiRequest(DataConfiguration params) throws Exception {
    return convertSenseiRequest(params);
  }

  public static SenseiRequest convertSenseiRequest(DataConfiguration params) {
    SenseiRequest senseiReq = new SenseiRequest();

    convertScalarParams(senseiReq, params);
    convertSenseiQuery(senseiReq, params);
    convertSortParam(senseiReq, params);
    convertSelectParam(senseiReq, params);
    convertFacetParam(senseiReq, params);
    convertInitParams(senseiReq, params);
    convertPartitionParams(senseiReq, params);

    return senseiReq;
  }

  public static void convertSenseiQuery(SenseiRequest senseiReq, DataConfiguration params) {
    senseiReq.setQuery(buildSenseiQuery(params));
  }

  public static void convertScalarParams(SenseiRequest senseiReq, DataConfiguration params) {
    senseiReq.setOffset(params.getInt(PARAM_OFFSET, 0));
    senseiReq.setCount(params.getInt(PARAM_COUNT, 10));
    senseiReq.setShowExplanation(params.getBoolean(PARAM_SHOW_EXPLAIN, false));
    senseiReq.setFetchStoredFields(params.getBoolean(PARAM_FETCH_STORED, false));

    String[] fetchTVs = params.getStringArray(PARAM_FETCH_TERMVECTOR);
    if (fetchTVs != null && fetchTVs.length > 0) {
      HashSet<String> tvsToFetch = new HashSet<String>(Arrays.asList(fetchTVs));
      tvsToFetch.remove("");
      if (tvsToFetch.size() > 0) senseiReq.setTermVectorsToFetch(tvsToFetch);
    }

    String[] fetchSFs = params.getStringArray(PARAM_FIELDS_TO_FETCH);
    if (fetchSFs != null && fetchSFs.length > 0) {
      HashSet<String> sfToFetch = new HashSet<String>(Arrays.asList(fetchSFs));
      sfToFetch.remove("");
      if (sfToFetch.size() > 0) senseiReq.setStoredFieldsToFetch(sfToFetch);
    }

    String groupBy = params.getString(PARAM_GROUP_BY, null);
    if (groupBy != null && groupBy.length() != 0) senseiReq.setGroupBy(StringUtils.split(groupBy,
      ','));
    senseiReq.setMaxPerGroup(params.getInt(PARAM_MAX_PER_GROUP, 0));
    String routeParam = params.getString(PARAM_ROUTE_PARAM);
    if (routeParam != null && routeParam.length() != 0) senseiReq.setRouteParam(routeParam);
  }

  @SuppressWarnings("unchecked")
  public static void convertPartitionParams(SenseiRequest senseiReq, DataConfiguration params) {
    if (params.containsKey(PARAM_PARTITIONS)) {
      List<Integer> partitions = params.getList(Integer.class, PARAM_PARTITIONS);
      senseiReq.setPartitions(new HashSet<Integer>(partitions));
    }
  }

  @SuppressWarnings("unchecked")
  public static void convertInitParams(SenseiRequest senseiReq, DataConfiguration params) {
    Map<String, Configuration> facetParamMap = RequestConverter.parseParamConf(params,
      PARAM_DYNAMIC_INIT);
    Set<Entry<String, Configuration>> facetEntries = facetParamMap.entrySet();

    for (Entry<String, Configuration> facetEntry : facetEntries) {
      String facetName = facetEntry.getKey();
      Configuration facetConf = facetEntry.getValue();

      DefaultFacetHandlerInitializerParam facetParams = new DefaultFacetHandlerInitializerParam();

      Iterator<String> paramsIter = facetConf.getKeys();

      while (paramsIter.hasNext()) {
        String paramName = paramsIter.next();
        Configuration paramConf = (Configuration) facetConf.getProperty(paramName);

        String type = paramConf.getString(PARAM_DYNAMIC_TYPE);
        List<String> vals = paramConf.getList(PARAM_DYNAMIC_VAL);

        try {
          String[] attrVals = vals.toArray(new String[0]);

          if (attrVals.length == 0 || attrVals[0].length() == 0) {
            logger.warn(String.format("init param has no values: facet: %s, type: %s", facetName,
              type));
            continue;
          }

          // TODO: smarter dispatching, factory, generics
          if (type.equalsIgnoreCase(PARAM_DYNAMIC_TYPE_BOOL)) {
            createBooleanInitParam(facetParams, paramName, attrVals);
          } else if (type.equalsIgnoreCase(PARAM_DYNAMIC_TYPE_STRING)) {
            createStringInitParam(facetParams, paramName, attrVals);
          } else if (type.equalsIgnoreCase(PARAM_DYNAMIC_TYPE_INT)) {
            createIntInitParam(facetParams, paramName, attrVals);
          } else if (type.equalsIgnoreCase(PARAM_DYNAMIC_TYPE_BYTEARRAY)) {
            createByteArrayInitParam(facetParams, paramName, paramConf.getString(PARAM_DYNAMIC_VAL));
          } else if (type.equalsIgnoreCase(PARAM_DYNAMIC_TYPE_LONG)) {
            createLongInitParam(facetParams, paramName, attrVals);
          } else if (type.equalsIgnoreCase(PARAM_DYNAMIC_TYPE_DOUBLE)) {
            createDoubleInitParam(facetParams, paramName, attrVals);
          } else {
            logger.warn(String.format("Unknown init param name: %s, type %s, for facet: %s",
              paramName, type, facetName));
            continue;
          }

        } catch (Exception e) {
          logger.warn(String.format("Failed to parse init param name: %s, type %s, for facet: %s",
            paramName, type, facetName));
        }
      }

      senseiReq.setFacetHandlerInitializerParam(facetName, facetParams);
    }
  }

  private static void createBooleanInitParam(DefaultFacetHandlerInitializerParam facetParams,
      String name, String[] paramVals) {
    boolean[] vals = new boolean[paramVals.length];
    int i = 0;
    for (String paramVal : paramVals) {
      vals[i++] = Boolean.parseBoolean(paramVal);
    }

    facetParams.putBooleanParam(name, vals);
  }

  private static void createStringInitParam(DefaultFacetHandlerInitializerParam facetParams,
      String name, String[] paramVals) {
    facetParams.putStringParam(name, Arrays.asList(paramVals));
  }

  private static void createIntInitParam(DefaultFacetHandlerInitializerParam facetParams,
      String name, String[] paramVals) {
    int[] vals = new int[paramVals.length];
    int i = 0;
    for (String paramVal : paramVals) {
      vals[i++] = Integer.parseInt(paramVal);
    }

    facetParams.putIntParam(name, vals);
  }

  private static void createByteArrayInitParam(DefaultFacetHandlerInitializerParam facetParams,
      String name, String paramVal) throws UnsupportedEncodingException {
    byte[] val = paramVal.getBytes("UTF-8");
    facetParams.putByteArrayParam(name, val);
  }

  private static void createLongInitParam(DefaultFacetHandlerInitializerParam facetParams,
      String name, String[] paramVals) {
    long[] vals = new long[paramVals.length];
    int i = 0;
    for (String paramVal : paramVals) {
      vals[i++] = Long.parseLong(paramVal);
    }

    facetParams.putLongParam(name, vals);
  }

  private static void createDoubleInitParam(DefaultFacetHandlerInitializerParam facetParams,
      String name, String[] paramVals) {
    double[] vals = new double[paramVals.length];
    int i = 0;
    for (String paramVal : paramVals) {
      vals[i++] = Double.parseDouble(paramVal);
    }

    facetParams.putDoubleParam(name, vals);
  }

  public static void convertSortParam(SenseiRequest senseiReq, DataConfiguration params) {
    String[] sortStrings = params.getStringArray(PARAM_SORT);

    if (sortStrings != null && sortStrings.length > 0) {
      ArrayList<SortField> sortFieldList = new ArrayList<SortField>(sortStrings.length);

      for (String sortString : sortStrings) {
        sortString = sortString.trim();
        if (sortString.length() == 0) continue;
        SortField sf;
        String[] parts = sortString.split(":");
        if (parts.length == 2) {
          boolean reverse = PARAM_SORT_DESC.equals(parts[1]);
          sf = new SortField(parts[0], SortField.Type.CUSTOM, reverse);
        } else if (parts.length == 1) {
          if (PARAM_SORT_SCORE.equals(parts[0])) {
            sf = SenseiRequest.FIELD_SCORE;
          } else if (PARAM_SORT_SCORE_REVERSE.equals(parts[0])) {
            sf = SenseiRequest.FIELD_SCORE_REVERSE;
          } else if (PARAM_SORT_DOC.equals(parts[0])) {
            sf = SenseiRequest.FIELD_DOC;
          } else if (PARAM_SORT_DOC_REVERSE.equals(parts[0])) {
            sf = SenseiRequest.FIELD_DOC_REVERSE;
          } else {
            sf = new SortField(parts[0], SortField.Type.CUSTOM, false);
          }
        } else {
          throw new IllegalArgumentException("invalid sort string: " + sortString);
        }

        if (sf.getType() != SortField.Type.DOC && sf.getType() != SortField.Type.SCORE
            && (sf.getField() == null || sf.getField().isEmpty())) // Empty field name.
        continue;

        sortFieldList.add(sf);
      }

      senseiReq.setSort(sortFieldList.toArray(new SortField[sortFieldList.size()]));
    }
  }

  public static void convertFacetParam(SenseiRequest senseiReq, DataConfiguration params) {
    Map<String, Configuration> facetParamMap = RequestConverter.parseParamConf(params, PARAM_FACET);
    Set<Entry<String, Configuration>> entries = facetParamMap.entrySet();

    for (Entry<String, Configuration> entry : entries) {
      String name = entry.getKey();
      Configuration conf = entry.getValue();
      FacetSpec fspec = new FacetSpec();

      fspec.setExpandSelection(conf.getBoolean(PARAM_FACET_EXPAND, false));
      fspec.setMaxCount(conf.getInt(PARAM_FACET_MAX, 10));
      fspec.setMinHitCount(conf.getInt(PARAM_FACET_MINHIT, 1));

      FacetSpec.FacetSortSpec orderBy;
      String orderString = conf.getString(PARAM_FACET_ORDER, PARAM_FACET_ORDER_HITS);
      if (PARAM_FACET_ORDER_HITS.equals(orderString)) {
        orderBy = FacetSpec.FacetSortSpec.OrderHitsDesc;
      } else if (PARAM_FACET_ORDER_VAL.equals(orderString)) {
        orderBy = FacetSpec.FacetSortSpec.OrderValueAsc;
      } else {
        throw new IllegalArgumentException("invalid order string: " + orderString);
      }
      fspec.setOrderBy(orderBy);
      senseiReq.setFacetSpec(name, fspec);
    }
  }

  public static void convertSelectParam(SenseiRequest senseiReq, DataConfiguration params) {
    Map<String, Configuration> selectParamMap = RequestConverter.parseParamConf(params,
      PARAM_SELECT);
    Set<Entry<String, Configuration>> entries = selectParamMap.entrySet();

    for (Entry<String, Configuration> entry : entries) {
      String name = entry.getKey();
      Configuration conf = entry.getValue();

      BrowseSelection sel = new BrowseSelection(name);

      String[] vals = conf.getStringArray(PARAM_SELECT_VAL);
      for (String val : vals) {
        if (val.trim().length() > 0) {
          sel.addValue(val);
        }
      }

      vals = conf.getStringArray(PARAM_SELECT_NOT);
      for (String val : vals) {
        if (val.trim().length() > 0) {
          sel.addNotValue(val);
        }
      }

      String op = conf.getString(PARAM_SELECT_OP, PARAM_SELECT_OP_OR);

      ValueOperation valOp;
      if (PARAM_SELECT_OP_OR.equals(op)) {
        valOp = ValueOperation.ValueOperationOr;
      } else if (PARAM_SELECT_OP_AND.equals(op)) {
        valOp = ValueOperation.ValueOperationAnd;
      } else {
        throw new IllegalArgumentException("invalid selection operation: " + op);
      }
      sel.setSelectionOperation(valOp);

      String[] selectPropStrings = conf.getStringArray(PARAM_SELECT_PROP);
      if (selectPropStrings != null && selectPropStrings.length > 0) {
        Map<String, String> prop = new HashMap<String, String>();
        for (String selProp : selectPropStrings) {
          if (selProp.trim().length() == 0) continue;

          String[] parts = selProp.split(":");
          if (parts.length == 2) {
            prop.put(parts[0], parts[1]);
          } else {
            throw new IllegalArgumentException("invalid prop string: " + selProp);
          }
        }
        sel.setSelectionProperties(prop);
      }

      senseiReq.addSelection(sel);
    }
  }

  private static final JsonFactory JSON_FACTORY = new JsonFactory();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  protected void buildResultString(HttpServletRequest httpReq, OutputStream ostream, SenseiSystemInfo info)
    throws IOException {
    JSONObject jsonObj = new FastJSONObject();

    JsonGenerator jsonGenerator = JSON_FACTORY.createGenerator(ostream);
    jsonGenerator.setCodec(OBJECT_MAPPER);
    initJsonp(httpReq, jsonGenerator);

    jsonGenerator.writeStartObject();

    jsonGenerator.writeNumberField(PARAM_SYSINFO_NUMDOCS, info.getNumDocs());
    jsonGenerator.writeNumberField(PARAM_SYSINFO_LASTMODIFIED, info.getLastModified());
    jsonGenerator.writeStringField(PARAM_SYSINFO_VERSION, info.getVersion());
    
    if (info.getSchema() != null && info.getSchema().length() != 0) {
      // TODO don't do this!
      // validate... original behavior reads value into JSONObject then reserializes which would throw an exception
      try {
        JsonParser parser = JSON_FACTORY.createParser(info.getSchema());
        while (parser.nextToken() != null);
        jsonGenerator.writeFieldName(PARAM_SYSINFO_SCHEMA);
        jsonGenerator.writeRawValue(info.getSchema());
      } catch (JsonParseException e) {
        writeJSONErrors(jsonGenerator, Collections.singletonList(
          new SenseiError(e.getMessage(), ErrorType.JsonParsingError)));
      }
    }

    jsonGenerator.writeArrayFieldStart(PARAM_SYSINFO_FACETS);
    Set<SenseiSystemInfo.SenseiFacetInfo> facets = info.getFacetInfos();
    if (facets != null) {
        for (SenseiSystemInfo.SenseiFacetInfo facet : facets) {
          jsonGenerator.writeStartObject();
          jsonGenerator.writeStringField(PARAM_SYSINFO_FACETS_NAME, facet.getName());
          jsonGenerator.writeBooleanField(PARAM_SYSINFO_FACETS_RUNTIME, facet.isRunTime());
          jsonGenerator.writeObjectField(PARAM_SYSINFO_FACETS_PROPS, facet.getProps());
          jsonGenerator.writeEndObject();
        }
    }
    jsonGenerator.writeEndArray();

    jsonGenerator.writeArrayFieldStart(PARAM_SYSINFO_CLUSTERINFO);
    List<SenseiSystemInfo.SenseiNodeInfo> clusterInfo = info.getClusterInfo();
    if (clusterInfo != null) {
      for (SenseiSystemInfo.SenseiNodeInfo nodeInfo : clusterInfo) {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeNumberField(PARAM_SYSINFO_CLUSTERINFO_ID, nodeInfo.getId());
        jsonGenerator.writeObjectField(PARAM_SYSINFO_CLUSTERINFO_PARTITIONS, nodeInfo.getPartitions());
        jsonGenerator.writeStringField(PARAM_SYSINFO_CLUSTERINFO_NODELINK, nodeInfo.getNodeLink());
        jsonGenerator.writeStringField(PARAM_SYSINFO_CLUSTERINFO_ADMINLINK, nodeInfo.getAdminLink());
        jsonGenerator.writeEndObject();
      }
    }
    jsonGenerator.writeEndArray();

    jsonGenerator.writeEndObject();
    endJsonp(httpReq, jsonGenerator);
    jsonGenerator.flush();
  }
}
