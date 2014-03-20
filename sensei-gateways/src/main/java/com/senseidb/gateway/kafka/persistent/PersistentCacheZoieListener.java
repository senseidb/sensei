package com.senseidb.gateway.kafka.persistent;

import proj.zoie.api.indexing.IndexingEventListener;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

public class PersistentCacheZoieListener implements IndexingEventListener {
  private final PersistentCache persistentCache;
  private static Counter versionUpdateCount = Metrics.newCounter(new MetricName(
      PersistentKafkaStreamDataProvider.class, "versionUpdateCount"));

  public PersistentCacheZoieListener(PersistentCache persistentCache) {
    this.persistentCache = persistentCache;
  }

  @Override
  public void handleIndexingEvent(IndexingEvent evt) {

  }

  @Override
  public void handleUpdatedDiskVersion(String version) {
    versionUpdateCount.inc();
    persistentCache.updateDiskVersion(version);

  }
}