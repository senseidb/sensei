package com.senseidb.perf;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.search.client.SenseiServiceProxy;
import com.senseidb.search.client.json.JsonSerializer;
import com.senseidb.search.client.req.SenseiClientRequest;
import com.senseidb.search.node.SenseiServer;

public class TestRunner {
  private static int generatedUid = 0;
  private static ArrayList<JSONObject> jsons;
  private static int size;
  private static int readUid = 0;
  private static boolean queryThreadStop = false;

  public static void main(String[] args) throws Exception {
    org.apache.log4j.PropertyConfigurator.configure("conf-perf/log4j-perf.properties");
    SenseiServer.main(new String[] { "conf-perf" });
    List<String> linesFromFile = FileUtils.readLines(new File("data/cars.json"));
    jsons = new ArrayList<JSONObject>();
    for (String line : linesFromFile) {
      if (line == null || !line.contains("{")) {
        continue;
      }
      jsons.add(new JSONObject(line));
    }
    size = jsons.size();
    System.out.println(size + " lines in car.json");
    Runnable injection = new Runnable() {
      @Override
      public void run() {
        while (putNextDoc()) {
        }
      };
    };

    Thread thread = new Thread(injection);
    thread.start();

    Thread[] queryThreads = new Thread[1];
    final SenseiServiceProxy proxy = new SenseiServiceProxy("localhost", 8080);
    Runnable query = new Runnable() {

      @Override
      public void run() {
        int count = 0;
        long cost = 0;
        boolean init = false;
        while (!queryThreadStop) {
          long start = System.currentTimeMillis();
          String sendPostRaw = proxy.sendPostRaw(
            proxy.getSearchUrl(),
            ((JSONObject) JsonSerializer.serialize(SenseiClientRequest.builder()
                .addSort(com.senseidb.search.client.req.Sort.desc("mileage")).build())).toString());
          cost += System.currentTimeMillis() - start;
          ++count;
          try {
            int numhits = new JSONObject(sendPostRaw).getInt("totaldocs");
            if (numhits > 0) {
              init = true;
            }
            if (init && numhits == 0) {
              System.out.println("!!!!numhits is 0");
            }

            if (count % 100 == 0) {
              System.out.println("Avg latency: " + (double) cost / count);
            }
            Thread.sleep(200);
          } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
        System.out.println("Final Avg latency: " + (double) cost / count);

      }
    };

    System.out.println("Start test while data is being injected.");
    for (int i = 0; i < queryThreads.length; i++) {
      queryThreads[i] = new Thread(query);
      queryThreads[i].start();
    }

    thread.join();
    queryThreadStop = true;
    System.out.println("Injection is done.");
    Thread.sleep(15000);

    System.out.println("Start test while no data is being injected.");
    queryThreadStop = false;
    for (int i = 0; i < queryThreads.length; i++) {
      queryThreads[i] = new Thread(query);
      queryThreads[i].start();
    }
    Thread.sleep(120 * 1000);
    queryThreadStop = true;
    System.out.println("Quit...");
  }

  public static boolean putNextDoc() {
    if (readUid == size) {
      readUid = 0;
    }
    if (generatedUid == 1000000) {
      return false;
    }
    JSONObject newEvent = clone(jsons.get(readUid++));
    try {
      newEvent.put("id", generatedUid++);
      PerfFileDataProvider.queue.put(newEvent);
    } catch (Exception e) {
      System.out.println("Error " + e.getMessage());
    }
    return true;
  }

  private static JSONObject clone(JSONObject obj) {
    JSONObject ret = new JSONObject();
    for (String key : JSONObject.getNames(obj)) {
      try {
        ret.put(key, obj.opt(key));
      } catch (JSONException ex) {
        throw new RuntimeException(ex);
      }
    }
    return ret;
  }
}
