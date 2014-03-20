package com.senseidb.search.req.mapred.functions.groupby;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import com.browseengine.bobo.api.BoboSegmentReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.facets.FacetCountCollector;
import com.browseengine.bobo.facets.FacetCountCollectorSource;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.TermNumberList;
import com.browseengine.bobo.facets.filter.RandomAccessFilter;
import com.browseengine.bobo.facets.impl.DefaultFacetCountCollector;
import com.browseengine.bobo.sort.DocComparatorSource;

public class SumGroupByFacetHandler extends FacetHandler<FacetDataCache<?>> {
  private static final String[] EMPTY = new String[0];
  private String _groupByFacet;
  private String _valueFacet;
  private final String _name;

  public SumGroupByFacetHandler(String name) {
    super(name);
    this._name = name;

  }

  public SumGroupByFacetHandler(String name, String groupByFacet, String valueFacet) {
    super(name, asSet(groupByFacet, valueFacet));
    this._name = name;
    this._groupByFacet = groupByFacet;
    this._valueFacet = valueFacet;
  }

  public static Set<String> asSet(String... vals) {
    Set<String> ret = new HashSet<String>(vals.length);
    for (String str : vals) {
      ret.add(str);
    }
    return ret;
  }

  @Override
  public RandomAccessFilter buildRandomAccessFilter(String value, Properties selectionProperty)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FacetCountCollectorSource getFacetCountCollectorSource(final BrowseSelection sel,
      final FacetSpec fspec) {
    return new FacetCountCollectorSource() {
      @SuppressWarnings("unchecked")
      @Override
      public FacetCountCollector getFacetCountCollector(BoboSegmentReader reader, int docBase) {
        String dimension = fspec.getProperties().get("dimension");
        String metric = fspec.getProperties().get("metric");
        if (dimension == null) {
          dimension = SumGroupByFacetHandler.this._groupByFacet;
        }
        if (metric == null) {
          metric = SumGroupByFacetHandler.this._valueFacet;
        }
        FacetDataCache<?> groupByCache = (FacetDataCache<?>) reader.getFacetData(dimension);
        final FacetDataCache<?> sumOverDataCache = (FacetDataCache<?>) reader.getFacetData(metric);
        final TermNumberList<Number> valList = (TermNumberList<Number>) sumOverDataCache.valArray;
        return new DefaultFacetCountCollector(_name, groupByCache, docBase, sel, fspec) {
          @Override
          public void collectAll() {
            for (int docid = 0; docid < _array.size(); docid++)
              collect(docid);
          }

          @Override
          public void collect(int docid) {
            _count.add(
              _array.get(docid),
              _count.get(_array.get(docid))
                  + (int) valList.getDoubleValue(sumOverDataCache.orderArray.get(docid)));
          }
        };

      }
    };
  }

  @Override
  public String[] getFieldValues(BoboSegmentReader reader, int id) {
    return EMPTY;
  }

  @Override
  public DocComparatorSource getDocComparatorSource() {
    return null;
  }

  @Override
  public FacetDataCache<?> load(BoboSegmentReader reader) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }
}
