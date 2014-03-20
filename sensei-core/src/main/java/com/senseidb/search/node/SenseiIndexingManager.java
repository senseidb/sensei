package com.senseidb.search.node;

import java.util.Map;

import proj.zoie.api.DataProvider;
import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieException;

import com.browseengine.bobo.api.BoboSegmentReader;

public interface SenseiIndexingManager<D> {
  void initialize(Map<Integer, Zoie<BoboSegmentReader, D>> zoieSystemMap) throws Exception;

  void start() throws Exception;

  void shutdown();

  DataProvider<D> getDataProvider();

  void syncWithVersion(long timeToWait, String version) throws ZoieException;
}
