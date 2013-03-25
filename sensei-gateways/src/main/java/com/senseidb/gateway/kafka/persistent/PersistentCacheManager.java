package com.senseidb.gateway.kafka.persistent;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.json.JSONObject;

import proj.zoie.api.Zoie;

import com.browseengine.bobo.facets.FacetHandler;
import com.senseidb.conf.SenseiSchema;
import com.senseidb.indexing.ShardingStrategy;
import com.senseidb.indexing.activity.PurgeUnusedActivitiesJob;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.node.SenseiCore;
import com.senseidb.search.node.SenseiZoieFactory;
import com.senseidb.search.plugin.PluggableSearchEngine;
import com.senseidb.util.Pair;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

/**
 * 
 * This class was created to overcome the Kafka's lack of offset management support on the cluster level
 * It uses a small persistent cache inside the Sensei
 * @author vzhabiuk
 *
 */
public class PersistentCacheManager implements PluggableSearchEngine {
  private static final Logger log = Logger.getLogger(PersistentCacheManager.class);
  private String indexDirectory;
  private int nodeId;
  private ShardingStrategy shardingStrategy;
  private Map<Integer, PersistentCache> persistentCaches = new HashMap<Integer, PersistentCache>();
  private Comparator<String> versionComparator;
  private int maxPartition;
  private static Timer timer = Metrics.newTimer(new MetricName(PersistentKafkaStreamDataProvider.class, "cachePersistTime"), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
  private static Counter numberOfBatchesCounter = Metrics.newCounter(new MetricName(PersistentKafkaStreamDataProvider.class, "numberOfBatches"));
  @Override
  public void init(String indexDirectory, int nodeId, SenseiSchema senseiSchema, Comparator<String> versionComparator,
      SenseiPluginRegistry pluginRegistry, ShardingStrategy shardingStrategy) {
        this.indexDirectory = indexDirectory;
        this.nodeId = nodeId;
        this.versionComparator = versionComparator;
        this.shardingStrategy = shardingStrategy;
        maxPartition = pluginRegistry.getConfiguration().getInt("sensei.index.manager.default.maxpartition.id") + 1;
    
  }

  @Override
  public String getVersion() {
    return null;
  }

  @Override
  public JSONObject acceptEvent(JSONObject event, String version) {
    return event;
  }

  @Override
  public boolean acceptEventsForAllPartitions() {
    return false;
  }

  @Override
  public Set<String> getFieldNames() {
    return Collections.EMPTY_SET;
  }

  @Override
  public Set<String> getFacetNames() {
    return Collections.EMPTY_SET;
  }

  @Override
  public List<FacetHandler<?>> createFacetHandlers() {
    return Collections.EMPTY_LIST;
  }

  @Override
  public void onDelete(IndexReader indexReader, long... uids) {
    
  }

  @Override
  public void start(SenseiCore senseiCore) {
    persistentCaches = new HashMap<Integer, PersistentCache>(senseiCore.getPartitions().length);
    for (int partition : senseiCore.getPartitions()) {
      Zoie zoie = (Zoie)senseiCore.getIndexReaderFactory(partition);
      PersistentCache persistentCache = new PersistentCache(getPath(indexDirectory, nodeId,partition), versionComparator);
      PersistentCache.registerAsListener(persistentCache, zoie);
      persistentCaches.put(partition, persistentCache);
    }
  }

  public static File getPath(String indexDirectory, int nodeId, int partition) {
    return new File(SenseiZoieFactory.getPath(new File(indexDirectory), nodeId, partition), "cache");
  }
  public  void addEvent(JSONObject event, String version) {
    try {
    int partition = shardingStrategy.caculateShard(maxPartition, event);
    persistentCaches.get(partition).acceptEvent(event, version);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public void commitPengingEvents() {
    log.info("Flushing pending kafka events to the persistent cache");
    long time = System.currentTimeMillis();
    int numberOfBatches = 0;
    
    for (PersistentCache persistentCache : persistentCaches.values()) {
      persistentCache.commitPengingEvents();
      numberOfBatches += persistentCache.numberOfAvailableBatches();
    }
    numberOfBatchesCounter.clear();
    numberOfBatchesCounter.inc(numberOfBatches);
    timer.update(System.currentTimeMillis() - time, TimeUnit.MILLISECONDS);
}

  public synchronized List<Pair<String, String>> getEventsNotAvailableInZoie(String zoiePersistentVersion) {
    try {
    List<Pair<String, String>> events = new ArrayList<Pair<String, String>>();
    for (PersistentCache persistentCache : persistentCaches.values()) {
      events.addAll(persistentCache.getEventsNotAvailableInZoie(zoiePersistentVersion));
    }
    Collections.sort( events, new Comparator<Pair<String, String>>() {
      @Override
      public int compare(Pair<String, String> o1, Pair<String, String> o2) {
        return versionComparator.compare(o1.getFirst(), o1.getFirst());
      }
    });
    List<Pair<String, String>> ret = new ArrayList<Pair<String, String>> ();
      for (Pair<String, String> pair : events) {
        if (versionComparator.compare(zoiePersistentVersion, pair.getFirst()) <= 0) {
          ret.add(pair);
        }
      }
    return ret;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
  @Override
  public void stop() {
    
  }

  public Map<Integer, PersistentCache> getPersistentCaches() {
    return persistentCaches;
  }

}
