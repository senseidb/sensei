package com.senseidb.gateway.test;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import junit.framework.Assert;
import junit.framework.TestCase;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import proj.zoie.impl.indexing.ZoieConfig;

import com.senseidb.gateway.kafka.DefaultJsonDataSourceFilter;
import com.senseidb.gateway.kafka.persistent.PersistentCache;
import com.senseidb.gateway.kafka.persistent.PersistentCacheManager;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.util.Pair;
import com.senseidb.util.SingleNodeStarter;
@Ignore("Takes too much time to execute and might be  fragile")
public class KafkaIntegrationTest  {
  private static File kafkaServerFile = new File("src/test/resources/configs/kafka-server.properties");
  private static KafkaServer kafkaServer;
  private static String indexDirectory;
  private static File ConfDir1;
  @BeforeClass
  public static void init() throws Exception {
    File indexDir = new File("sensei-kafka-test"); 
    SingleNodeStarter.rmrf(indexDir);
    indexDir.mkdir();
    ConfDir1 = new File(KafkaIntegrationTest.class.getClassLoader().getResource("kafka-sensei-conf").toURI());
    
    Properties props = new Properties();
    props.put("zk.connect", "localhost:2181");
    props.put("serializer.class", "kafka.serializer.DefaultEncoder");
    SingleNodeStarter.start(ConfDir1, 0);
    Properties kafkaProps = new Properties();
    kafkaProps.load(new FileReader(kafkaServerFile));
    
    File kafkaLogFile = new File(kafkaProps.getProperty("log.dir"));
    FileUtils.deleteDirectory(kafkaLogFile);
    
    KafkaConfig kafkaConfig = new KafkaConfig(kafkaProps);
    kafkaServer = new KafkaServer(kafkaConfig);
    
    kafkaServer.startup();
    ProducerConfig producerConfig = new ProducerConfig(props);
    Producer<String,Message> kafkaProducer = new Producer<String,Message>(producerConfig);
    SenseiPluginRegistry senseiPluginRegistry = SenseiPluginRegistry.build(new PropertiesConfiguration(new File(ConfDir1, "sensei.properties")));
    indexDirectory = senseiPluginRegistry.getConfiguration().getString("sensei.index.directory");
    String topic = senseiPluginRegistry.getConfiguration().getString("sensei.gateway.kafka.topic");
    List<ProducerData<String, Message>> msgList = new ArrayList<ProducerData<String, Message>>();
    for (String json : FileUtils.readLines(new File("src/test/resources/kafka-sensei-conf/data/cars.json"))){
      Message m = new Message(json.getBytes(DefaultJsonDataSourceFilter.UTF8));
      ProducerData<String,Message> msg = new ProducerData<String,Message>(topic,m);
      msgList.add(msg);
    }
    kafkaProducer.send(msgList);
    SingleNodeStarter.waitTillServerStarts(15000);
  }
  @AfterClass
  public static void afterClass() {
    SingleNodeStarter.shutdown();
    SingleNodeStarter.rmrf(new File(indexDirectory));
  }
  @Test
  public void test1DataFromPersistentCache() {
    PersistentCache persistentCache = new PersistentCache(PersistentCacheManager.getPath(indexDirectory, 1, 0), ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    List<Pair<String, String>> eventsNotAvailableInZoie = persistentCache.getEventsNotAvailableInZoie("0");
    Assert.assertEquals(500, eventsNotAvailableInZoie.size());
    
    
  }
  @Test
  public void test2RestartTheCacheWithDataFromPersistentCache() throws Exception{
    File tmpDir = new File("tmp");
    try {
    File directory = PersistentCacheManager.getPath(indexDirectory, 1, 0);
    tmpDir.mkdirs();
    FileUtils.copyDirectory(directory, tmpDir);    
    SingleNodeStarter.shutdown();
    //Thread.sleep(2000);
    SingleNodeStarter.rmrf(new File(indexDirectory));
    directory = PersistentCacheManager.getPath(indexDirectory, 1, 0);
    directory.mkdirs();
    FileUtils.copyDirectory(tmpDir, directory);  
    
    
    SingleNodeStarter.start(ConfDir1, 500);
    } finally {
      SingleNodeStarter.rmrf(tmpDir);
      
    }
  }
  
}
