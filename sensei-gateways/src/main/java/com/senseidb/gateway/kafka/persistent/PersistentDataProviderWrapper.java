package com.senseidb.gateway.kafka.persistent;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import proj.zoie.api.DataConsumer.DataEvent;

import com.senseidb.gateway.kafka.KafkaStreamDataProvider;
import com.senseidb.util.Pair;

public class PersistentDataProviderWrapper extends KafkaStreamDataProvider {
  private static final Logger log = Logger.getLogger(PersistentDataProviderWrapper.class);
  private final PersistentCacheManager cacheManager;
  private final int batchSize;
  private final AtomicInteger atomicInteger = new AtomicInteger(0);
  private final long startingOffset;
  private Iterator<Pair<String, String>> eventsFromPersistentCache;
  private final KafkaStreamDataProvider wrapped;
  public PersistentDataProviderWrapper(KafkaStreamDataProvider wrapped, long startingOffset,
      int batchSize, PersistentCacheManager cacheManager) {
    this.wrapped = wrapped;
    this.startingOffset = startingOffset;
    this.cacheManager = cacheManager;
    this.batchSize = batchSize;
  }

  @Override
  public void start() {
    wrapped.start();
    List<Pair<String, String>> eventsNotAvailableInZoie = cacheManager
        .getEventsNotAvailableInZoie(wrapped.getStringVersionRepresentation(startingOffset));
    eventsFromPersistentCache = eventsNotAvailableInZoie.iterator();

  }

  @Override
  public DataEvent<JSONObject> next() {
    try {
      if (eventsFromPersistentCache.hasNext()) {
        Pair<String, String> next = eventsFromPersistentCache.next();
        return new DataEvent<JSONObject>(new JSONObject(next.getSecond()), next.getFirst());
      }
      DataEvent<JSONObject> next = wrapped.next();
      if (next != null) {
        cacheManager.addEvent(next.getData(), next.getVersion());
        if (batchSize <= atomicInteger.incrementAndGet()) {
          cacheManager.commitPengingEvents();
          wrapped.commit();
          atomicInteger.set(0);
        }
      }
      return next;
    } catch (Exception ex) {
      log.error(ex.getMessage(), ex);
      return null;
    }
  }

  @Override
  public void stop() {
    wrapped.stop();
  }

  @Override
  public void setBatchSize(int batchSize) {
    wrapped.setBatchSize(batchSize);
  }

  @Override
  public void setMaxEventsPerMinute(long maxEventsPerMinute) {
    wrapped.setMaxEventsPerMinute(maxEventsPerMinute);
  }

  @Override
  public void setRetryTime(int retryTime) {
    wrapped.setRetryTime(retryTime);
  }

}
