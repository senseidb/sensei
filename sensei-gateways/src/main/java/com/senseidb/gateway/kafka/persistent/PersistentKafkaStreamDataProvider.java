package com.senseidb.gateway.kafka.persistent;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import proj.zoie.api.DataConsumer.DataEvent;

import com.senseidb.gateway.kafka.DataPacket;
import com.senseidb.gateway.kafka.KafkaStreamDataProvider;
import com.senseidb.indexing.DataSourceFilter;
import com.senseidb.indexing.activity.PurgeUnusedActivitiesJob;
import com.senseidb.util.Pair;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

public class PersistentKafkaStreamDataProvider extends KafkaStreamDataProvider  {
  private static final Logger log = Logger.getLogger(PersistentKafkaStreamDataProvider.class);
  private final PersistentCacheManager cacheManager;
  private int batchSize;
  private AtomicInteger currentBatchCounter = new AtomicInteger(0);
  private final long startingOffset;
  private AtomicLong versionCounter = new AtomicLong();
  private volatile Iterator<Pair<String, String>> eventsFromPersistentCache;
  
  
  public PersistentKafkaStreamDataProvider(Comparator<String> versionComparator,String zookeeperUrl,int soTimeout,int batchSize,
      String consumerGroupId,String topic,long startingOffset,DataSourceFilter<DataPacket> dataConverter, PersistentCacheManager cacheManager) {
    super(versionComparator, zookeeperUrl, soTimeout, batchSize, consumerGroupId, topic, startingOffset, dataConverter);
    this.startingOffset = startingOffset;
    versionCounter.set(startingOffset);
    this.cacheManager = cacheManager;
    this.batchSize = batchSize;
  }

  @Override
  public void start() {
    List<Pair<String, String>> eventsNotAvailableInZoie = cacheManager
        .getEventsNotAvailableInZoie(getStringVersionRepresentation(startingOffset));
    eventsFromPersistentCache = eventsNotAvailableInZoie.iterator();
    super.start();
  }

  @Override
  public DataEvent<JSONObject> next() {
    try {
      if (eventsFromPersistentCache != null && eventsFromPersistentCache.hasNext()) {
        Pair<String, String> next = eventsFromPersistentCache.next();
        return new DataEvent<JSONObject>(new JSONObject(next.getSecond()), next.getFirst());
      }
      DataEvent<JSONObject> next = super.next();
      if (next != null) {
        cacheManager.addEvent(next.getData(), next.getVersion());
        if (batchSize <= currentBatchCounter.incrementAndGet()) {
          cacheManager.commitPengingEvents();
          super.commit();
          currentBatchCounter.set(0);
        }
      }
      return next;
    } catch (Exception ex) {
      log.error(ex.getMessage(), ex);
      return null;
    }
  }
  @Override
  public long getNextVersion() {
    return versionCounter.incrementAndGet();
  }

 
}
