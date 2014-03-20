package com.senseidb.search.node;

import java.io.IOException;
import java.util.List;

import proj.zoie.api.ZoieSegmentReader;
import proj.zoie.impl.indexing.AbstractIndexReaderDecorator;

import com.browseengine.bobo.api.BoboSegmentReader;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.RuntimeFacetHandlerFactory;

public class SenseiIndexReaderDecorator extends AbstractIndexReaderDecorator<BoboSegmentReader> {
  private final List<FacetHandler<?>> _facetHandlers;
  private final List<RuntimeFacetHandlerFactory<?, ?>> _facetHandlerFactories;

  public SenseiIndexReaderDecorator(List<FacetHandler<?>> facetHandlers,
      List<RuntimeFacetHandlerFactory<?, ?>> facetHandlerFactories) {
    _facetHandlers = facetHandlers;
    _facetHandlerFactories = facetHandlerFactories;
  }

  public SenseiIndexReaderDecorator() {
    this(null, null);
  }

  public List<FacetHandler<?>> getFacetHandlerList() {
    return _facetHandlers;
  }

  public List<RuntimeFacetHandlerFactory<?, ?>> getFacetHandlerFactories() {
    return _facetHandlerFactories;
  }

  @Override
  public BoboSegmentReader decorate(ZoieSegmentReader<BoboSegmentReader> zoieReader)
      throws IOException {
    BoboSegmentReader boboReader = null;
    if (zoieReader != null) {
      boboReader = BoboSegmentReader
          .getInstance(zoieReader, _facetHandlers, _facetHandlerFactories);
    }
    return boboReader;
  }

  @Override
  public BoboSegmentReader redecorate(BoboSegmentReader reader,
      ZoieSegmentReader<BoboSegmentReader> newReader) throws IOException {
    return reader.copy(newReader);
  }

}
