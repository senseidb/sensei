package com.senseidb.search.query.filters;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.TermRangeFilter;
import org.apache.lucene.util.Bits;
import org.json.JSONObject;

import com.browseengine.bobo.api.BoboSegmentReader;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.filter.EmptyFilter;
import com.browseengine.bobo.facets.filter.FacetRangeFilter;
import com.browseengine.bobo.facets.filter.RandomAccessFilter;
import com.browseengine.bobo.query.MatchAllDocIdSetIterator;
import com.senseidb.indexing.DefaultSenseiInterpreter;
import com.senseidb.indexing.MetaType;

public class RangeFilterConstructor extends FilterConstructor {
  public static final String FILTER_TYPE = "range";

  @SuppressWarnings("unchecked")
  @Override
  protected Filter doConstructFilter(Object obj) throws Exception {
    final JSONObject json = (JSONObject) obj;

    Iterator<String> iter = json.keys();
    if (!iter.hasNext()) return null;

    final String field = iter.next();

    JSONObject jsonObj = json.getJSONObject(field);

    final String from, to;
    final boolean include_lower, include_upper;
    final boolean noOptimize = jsonObj.optBoolean(NOOPTIMIZE_PARAM, false);
    final String type;
    final String dateFormat;

    String gt = jsonObj.optString(GT_PARAM, null);
    String gte = jsonObj.optString(GTE_PARAM, null);
    String lt = jsonObj.optString(LT_PARAM, null);
    String lte = jsonObj.optString(LTE_PARAM, null);

    type = jsonObj.optString(RANGE_FIELD_TYPE, null);
    dateFormat = jsonObj.optString(RANGE_DATE_FORMAT, null);

    if (gt != null) {
      from = gt;
      include_lower = false;
    } else if (gte != null) {
      from = gte;
      include_lower = true;
    } else {
      from = jsonObj.optString(FROM_PARAM, null);
      include_lower = jsonObj.optBoolean(INCLUDE_LOWER_PARAM, true);
    }

    if (lt != null) {
      to = lt;
      include_upper = false;
    } else if (lte != null) {
      to = lte;
      include_upper = true;
    } else {
      to = jsonObj.optString(TO_PARAM, null);
      include_upper = jsonObj.optBoolean(INCLUDE_UPPER_PARAM, true);
    }

    return new Filter() {
      @SuppressWarnings("rawtypes")
      @Override
      public DocIdSet getDocIdSet(final AtomicReaderContext context, final Bits acceptDocs) throws IOException {
        String fromPadded = from, toPadded = to;
        if (!noOptimize) {
          if (context.reader() instanceof BoboSegmentReader) {
            BoboSegmentReader boboReader = (BoboSegmentReader) context.reader();
            FacetHandler facetHandler = boboReader.getFacetHandler(field);
            if (facetHandler != null) {
              StringBuilder sb = new StringBuilder();
              if (include_lower && from != null) {
                sb.append("[");
              } else {
                sb.append("(");
              }

              if (from == null) {
                sb.append("*");
              } else {
                sb.append(from);
              }
              sb.append(" TO ");
              if (to == null) {
                sb.append("*");
              } else {
                sb.append(to);
              }

              if (include_upper && to != null) {
                sb.append("]");
              } else {
                sb.append(")");
              }
              RandomAccessFilter filter = null;
              Object facetData = boboReader.getFacetData(field);
              // If it's Sensei's standard facet handler
              if (facetData instanceof FacetDataCache) {
                filter = new FacetRangeFilter(facetHandler, sb.toString());
              } else {
                filter = facetHandler.buildRandomAccessFilter(sb.toString(), null);
              }
              return filter.getDocIdSet(context, acceptDocs);
            }
          }
        }

        if (type == null) {
          return EmptyFilter.getInstance().getDocIdSet(context, acceptDocs);
        }

        if ("int".equals(type)) {
          MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(int.class);
          String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
          DecimalFormat formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(
              Locale.US));
          fromPadded = formatter.format(Integer.parseInt(from));
          toPadded = formatter.format(Integer.parseInt(to));
        } else if ("short".equals(type)) {
          MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(short.class);
          String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
          DecimalFormat formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(
              Locale.US));
          fromPadded = formatter.format(Short.parseShort(from));
          toPadded = formatter.format(Short.parseShort(to));
        } else if ("long".equals(type)) {
          MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(long.class);
          String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
          DecimalFormat formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(
              Locale.US));
          fromPadded = formatter.format(Long.parseLong(from));
          toPadded = formatter.format(Long.parseLong(to));
        } else if ("date".equals(type)) {
          if (dateFormat == null) throw new IllegalArgumentException(
              "Date format cannot be empty in filter json when type is date: " + json);
          else {
            SimpleDateFormat formatter = new SimpleDateFormat(dateFormat);
            fromPadded = formatter.format(new Date(Long.parseLong(from)));
            toPadded = formatter.format(new Date(Long.parseLong(to)));
          }
        } else if ("float".equals(type)) {
          MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(float.class);
          String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
          DecimalFormat formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(
              Locale.US));
          fromPadded = formatter.format(Float.parseFloat(from));
          toPadded = formatter.format(Float.parseFloat(to));
        } else if ("double".equals(type)) {
          MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(double.class);
          String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
          DecimalFormat formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(
              Locale.US));
          fromPadded = formatter.format(Double.parseDouble(from));
          toPadded = formatter.format(Double.parseDouble(to));
        }

        if (fromPadded == null || fromPadded.length() == 0) if (toPadded == null
            || toPadded.length() == 0) return new DocIdSet() {
          @Override
          public boolean isCacheable() {
            return false;
          }

          @Override
          public DocIdSetIterator iterator() throws IOException {
            return new MatchAllDocIdSetIterator(context.reader(), acceptDocs);
          }
        };
        else {
          return TermRangeFilter.newStringRange(field, fromPadded, toPadded, false, include_upper)
              .getDocIdSet(context, acceptDocs);
        }
        else if (toPadded == null || toPadded.length() == 0) {
          return TermRangeFilter.newStringRange(field, fromPadded, toPadded, include_lower, false)
              .getDocIdSet(context, acceptDocs);
        }

        return TermRangeFilter.newStringRange(field, fromPadded, toPadded, include_lower,
          include_upper).getDocIdSet(context, acceptDocs);
      }
    };
  }
}
