package com.senseidb.gateway.test;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.commons.configuration.MapConfiguration;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.impl.indexing.ZoieConfig;

import com.senseidb.gateway.kafka.KafkaStreamDataProvider;
import com.senseidb.gateway.kafka.persistent.PersistentCache;
import com.senseidb.gateway.kafka.persistent.PersistentCacheManager;
import com.senseidb.gateway.kafka.persistent.PersistentDataProviderWrapper;
import com.senseidb.indexing.ShardingStrategy;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.util.Pair;
import com.senseidb.util.SingleNodeStarter;

public class PersistentKafkaGatewayTest extends TestCase {

  private PersistentCache cache;
  private File indexDir;

  @Before
  public void setUp() throws Exception {
    indexDir = new File("kafkaIndex");
    SingleNodeStarter.rmrf(indexDir);
    indexDir.mkdir();
  }

  @After
  protected void tearDown() throws Exception {
    indexDir = new File("kafkaIndex");
    SingleNodeStarter.rmrf(indexDir);
  }

  @Test
  public void test1PersistentStoreWithOneSavedBatch() throws Exception {

    MockDataProvider mockDataProvider = new MockDataProvider(BaseGatewayTestUtil.dataList);
    PersistentCacheManager cacheManager = createpersistentCacheManager();
    PersistentDataProviderWrapper persistentDataProviderWrapper = new PersistentDataProviderWrapper(
        mockDataProvider, 0, 3, cacheManager);
    persistentDataProviderWrapper.start();
    for (int i = 0; i < 4; i++) {
      persistentDataProviderWrapper.next();
    }
    TestCase.assertEquals(3, mockDataProvider.commitSnapshot);
    PersistentCacheManager secondManager = createpersistentCacheManager();
    List<Pair<String, String>> eventsNotAvailableInZoie = secondManager
        .getEventsNotAvailableInZoie("0");
    assertEquals(3, eventsNotAvailableInZoie.size());
    eventsNotAvailableInZoie = secondManager.getEventsNotAvailableInZoie("2");
    assertEquals(2, eventsNotAvailableInZoie.size());

  }

  @Test
  public void test2TestPersistentStoreWithThreeSavedBatch() throws Exception {

    MockDataProvider mockDataProvider = new MockDataProvider(BaseGatewayTestUtil.dataList);
    PersistentCacheManager cacheManager = createpersistentCacheManager();
    PersistentDataProviderWrapper persistentDataProviderWrapper = new PersistentDataProviderWrapper(
        mockDataProvider, 0, 3, cacheManager);
    persistentDataProviderWrapper.start();
    for (int i = 0; i < 10; i++) {
      persistentDataProviderWrapper.next();
    }
    TestCase.assertEquals(9, mockDataProvider.commitSnapshot);
    PersistentCacheManager newManager = createpersistentCacheManager();
    List<Pair<String, String>> eventsNotAvailableInZoie = newManager
        .getEventsNotAvailableInZoie("0");
    assertEquals(9, eventsNotAvailableInZoie.size());
    eventsNotAvailableInZoie = newManager.getEventsNotAvailableInZoie("4");
    assertEquals(6, eventsNotAvailableInZoie.size());

  }

  @Test
  public void test3TestPersistentStoreWithAndUpdatedZoie() throws Exception {

    MockDataProvider mockDataProvider = new MockDataProvider(BaseGatewayTestUtil.dataList);
    PersistentCacheManager cacheManager = createpersistentCacheManager();
    PersistentDataProviderWrapper persistentDataProviderWrapper = new PersistentDataProviderWrapper(
        mockDataProvider, 0, 3, cacheManager);
    persistentDataProviderWrapper.start();
    for (int i = 0; i < 10; i++) {
      persistentDataProviderWrapper.next();
    }
    TestCase.assertEquals(9, mockDataProvider.commitSnapshot);
    PersistentCacheManager newManager = createpersistentCacheManager();
    List<Pair<String, String>> eventsNotAvailableInZoie = newManager
        .getEventsNotAvailableInZoie("0");
    assertEquals(9, eventsNotAvailableInZoie.size());
    newManager.getPersistentCaches().get(0).updateDiskVersion("8");
    newManager = createpersistentCacheManager();
    eventsNotAvailableInZoie = newManager.getEventsNotAvailableInZoie("0");
    assertEquals(3, eventsNotAvailableInZoie.size());

  }

  public PersistentCacheManager createpersistentCacheManager() {
    PersistentCacheManager cacheManager = new PersistentCacheManager();
    Map map = new HashMap();
    map.put("sensei.index.manager.default.maxpartition.id", 0);
    cacheManager.init(indexDir.getAbsolutePath(), 1, null, ZoieConfig.DEFAULT_VERSION_COMPARATOR,
      SenseiPluginRegistry.build(new MapConfiguration(map)), new ShardingStrategy() {
        @Override
        public int caculateShard(int maxShardId, JSONObject dataObj) throws JSONException {
          return 0;
        }
      });
    cache = new PersistentCache(indexDir, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    cacheManager.getPersistentCaches().put(0, cache);
    return cacheManager;
  }

  public static class MockDataProvider extends KafkaStreamDataProvider {
    private int numberOfEventsFetched;
    public int commitSnapshot;
    private Iterator<JSONObject> iterator;

    public MockDataProvider(List<JSONObject> messages) {
      iterator = messages.iterator();
    }

    @Override
    public DataEvent<JSONObject> next() {
      numberOfEventsFetched++;
      return new DataEvent<JSONObject>(iterator.next(),
          getStringVersionRepresentation(numberOfEventsFetched));
    }

    @Override
    public void commit() {
      commitSnapshot = numberOfEventsFetched;
    }

    @Override
    public void start() {
    }
  }

}
