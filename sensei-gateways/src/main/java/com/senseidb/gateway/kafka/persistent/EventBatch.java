package com.senseidb.gateway.kafka.persistent;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.springframework.util.Assert;


import com.senseidb.util.Pair;

public class EventBatch {
  private static final Logger log = Logger.getLogger(EventBatch.class);
  private String minimumVersion;
  private String maximumVersion;
  private List<Pair<String, String>> events = new ArrayList<Pair<String, String>>();
  public void update(JSONObject event, String version) {
    if (minimumVersion == null) {
      minimumVersion = version;
    }
    if (version.contains("-")) {
      throw new IllegalStateException("The version shouldn't contain '-' symbol");
    }
    maximumVersion = version;
    events.add(new Pair<String, String>(version, event.toString()));
  }
  public void flusToDisk(File directory) {
    File file = new File(directory, minimumVersion + "-" + maximumVersion + ".cache");
    DataOutputStream outputStream = null;
    try {
      outputStream = new DataOutputStream(new GZIPOutputStream(new FileOutputStream(file)));
      outputStream.writeInt(events.size());
      for (Pair<String, String> eventData : events) {
        outputStream.writeUTF(eventData.getFirst());
        outputStream.writeUTF(eventData.getSecond());
      }
    } catch (Exception ex) {
      log.error("Error while flushing cache to file", ex);
      throw new RuntimeException(ex);
    } finally {
      IOUtils.closeQuietly(outputStream);
    }
  }
  public static Pair<String, String> getVersionInfo(String fileName) {
    if (!fileName.endsWith(".cache") || !fileName.contains("-")) {
      throw new IllegalStateException("the name of the cache chunk is incorrect = " + fileName);
    }
    String[] versions = fileName.substring(0, fileName.length() - ".cache".length()).split("-");
    String firstVersion = versions[0];
    String secondVersion = versions[1];
    return new Pair<String, String>(firstVersion, secondVersion);
    
  }
  public static EventBatch recreateFromDisk(File directory, String name) {
    Pair<String, String> versionInfo = getVersionInfo(name);
    EventBatch eventBatch = new EventBatch();
    eventBatch.minimumVersion = versionInfo.getFirst();
    eventBatch.maximumVersion = versionInfo.getSecond();
    DataInputStream inputStream = null;
    
    try {
      inputStream = new DataInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(new File(directory, name)))));
      int count = inputStream.readInt();
      for (int i = 0; i < count; i++) {
        String version = inputStream.readUTF();
        String event = inputStream.readUTF();
        eventBatch.events.add(new Pair<String, String>(version, event));
      }
    } catch (Exception ex) {
      log.error("Error while flushing cache to file", ex);
      throw new RuntimeException(ex);
    } finally {
      IOUtils.closeQuietly(inputStream);
    }
    Assert.state(eventBatch.minimumVersion.equals(eventBatch.events.get(0).getFirst()), "The first version in the fileName to the first version inside the file" + name);
    Assert.state(eventBatch.maximumVersion.equals(eventBatch.events.get(eventBatch.events.size() - 1).getFirst()), "The first version in the fileName to the first version inside the file" + name);
    return eventBatch;
  }
  public static Collection<String> getAvailableBatches(File directory) {
    String[] list = directory.list(new FilenameFilter() {      
      @Override
      public boolean accept(File dir, String name) {        
        return name.endsWith(".cache") && name.contains("-");
      }
    });    
    if (list == null) {
      return Collections.EMPTY_LIST;
    }
    return Arrays.asList(list);
  }
  public static Collection<String> getObsoleteFiles(Collection<String> fileNames, Comparator<String> versionComparator, String currentVersion) {
    Collection<String> ret = new ArrayList<String>();
    for (String fileName : fileNames) {
      Pair<String, String> versionInfo = getVersionInfo( fileName);
      if (versionComparator.compare(currentVersion, versionInfo.getSecond()) > 0) {
        ret.add(fileName);
      }
    }
    return ret;     
  }
  public static Collection<String> getRelevantFiles(Collection<String> fileNames, Comparator<String> versionComparator, String currentVersion) {
    Collection<String> ret = new ArrayList<String>();
    for (String fileName : fileNames) {
      Pair<String, String> versionInfo = getVersionInfo( fileName);
      if (versionComparator.compare(currentVersion, versionInfo.getSecond()) < 0) {
        ret.add(versionInfo.getSecond());
      }
    }
    return ret;     
  }
  public List<Pair<String, String>> getEvents() {
    return events;
  }
}
