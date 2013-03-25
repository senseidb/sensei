package com.senseidb.gateway.kafka.persistent;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.springframework.util.Assert;

import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.hourglass.impl.Hourglass;
import proj.zoie.hourglass.impl.HourglassListener;
import proj.zoie.impl.indexing.ZoieSystem;

import com.senseidb.util.Pair;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

public class PersistentCache {
  private static final Logger log = Logger.getLogger(PersistentCache.class);
  private final File indexDir;
  private final Comparator<String> versionComparator;
  private String currentPersistentVersion = null;
  private EventBatch eventBatch = new EventBatch();
  
  public PersistentCache(File indexDir, Comparator<String> versionComparator) {
    this.indexDir = indexDir;
    indexDir.mkdirs();
    this.versionComparator = versionComparator;
  }

  public synchronized void acceptEvent(JSONObject event, String version) {
    eventBatch.update(event, version);
  }

  public synchronized void commitPengingEvents() {
    eventBatch.flusToDisk(indexDir);
    eventBatch = new EventBatch();
  }

  public synchronized List<Pair<String, String>> getEventsNotAvailableInZoie(String zoiePersistentVersion) {
    try {      
      Collection<String> availableBatches = EventBatch.getAvailableBatches(indexDir);
      List<Pair<String, String>> ret = new ArrayList<Pair<String, String>>();
      List<String> batches = new ArrayList<String>(availableBatches);
      Collections.sort(batches, new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
          return versionComparator.compare(EventBatch.getVersionInfo(o1).getFirst(), EventBatch.getVersionInfo(o2).getFirst());
        }
      });
      for (String batch : batches) {
        EventBatch eventBatch = EventBatch.recreateFromDisk(indexDir, batch);
        for (Pair<String, String> pair : eventBatch.getEvents()) {
          if (versionComparator.compare(currentPersistentVersion, pair.getFirst()) <= 0) {
            ret.add(pair);
          }
        }
      }
      return ret;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public synchronized void updateDiskVersion(String version) {
    log.debug("Updating the disk version to " + version);
    if (currentPersistentVersion == null || versionComparator.compare(currentPersistentVersion, version) < 0) {
      currentPersistentVersion = version;
      purgeObsoleteFiles();
    }
  }

  public void purgeObsoleteFiles() {
    Collection<String> obsoleteFiles = EventBatch.getObsoleteFiles(EventBatch.getAvailableBatches(indexDir), versionComparator,
        currentPersistentVersion);
    for (String fileName : obsoleteFiles) {
      Assert.state(FileUtils.deleteQuietly(new File(indexDir, fileName)), "couldn't delete the file - " + fileName);
    }
  }

  public static void registerAsListener(final PersistentCache persistentCache, Zoie zoie) {
    if (zoie instanceof Hourglass) {
      Hourglass hourglass = (Hourglass) zoie;
      hourglass.addHourglassListener(new HourglassListener() {
        @Override
        public void onNewZoie(Zoie newZoie) {
          ((ZoieSystem) newZoie).addIndexingEventListener(new PersistentCacheZoieListener(persistentCache));
        }
        @Override
        public void onRetiredZoie(Zoie oldZoie) {
        }
        @Override
        public void onIndexReaderCleanUp(ZoieIndexReader indexReader) {
        }
      });
    } else {
      ((ZoieSystem) zoie).addIndexingEventListener(new PersistentCacheZoieListener(persistentCache));
    }
  }
  public int numberOfAvailableBatches() {
    return eventBatch.getAvailableBatches(indexDir).size();
  }
}
