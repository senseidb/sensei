package com.senseidb.search.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.search.client.json.JsonDeserializer;
import com.senseidb.search.client.json.JsonSerializer;
import com.senseidb.search.client.req.SenseiClientRequest;
import com.senseidb.search.client.res.SenseiResult;

public class SenseiServiceProxy {

  private String host;
  private int port;
  private final String url;

  public SenseiServiceProxy(String host, int port) {
    this.host = host;
    this.port = port;
    this.url = null;
  }

  public SenseiServiceProxy(String url) {
    this.url = url;

  }

  public SenseiResult sendSearchRequest(SenseiClientRequest request) {
    try {
      String requestStr = JsonSerializer.serialize(request).toString();
      String output = sendPostRaw(getSearchUrl(), requestStr);
      return JsonDeserializer.deserialize(SenseiResult.class, jsonResponse(output));
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public SenseiResult sendBQL(String bql) {
    try {
      String output = sendBQLRaw(bql);
      return JsonDeserializer.deserialize(SenseiResult.class, jsonResponse(output));
    } catch (Exception ex) {
      throw new RuntimeException("Exception in POST to " + url, ex);
    }
  }

  public String sendBQLRaw(String bql) {
    try {
      JSONObject bqlJson = new JSONObject().put("bql", bql);

      String output = sendPostRaw(getSearchUrl(), bqlJson.toString());
      return output;
    } catch (Exception ex) {
      throw new RuntimeException("Exception in POST to " + url, ex);
    }
  }

  public Map<Long, JSONObject> sendGetRequest(long... uids) throws IOException, JSONException {
    return sendGetRequest(true, true, uids);
  }

  public Map<Long, JSONObject> sendGetRequest(boolean includeValue, boolean includeFields,
                                              long... uids) throws IOException, JSONException {
    Map<Long, JSONObject> ret = new LinkedHashMap<Long, JSONObject>(uids.length);
    String response = sendPostRaw(getStoreGetUrl(includeValue, includeFields),
      new JSONArray(uids).toString());
    if (response == null || response.length() == 0) {
      return ret;
    }
    JSONObject responseJson = new JSONObject(response);

    @SuppressWarnings("rawtypes")
    Iterator keys = responseJson.keys();
    while (keys.hasNext()) {
      String key = (String) keys.next();
      ret.put(Long.parseLong(key), responseJson.optJSONObject(key));
    }

    return ret;
  }

  public String getSearchUrl() {
    if (url != null) return url;
    return "http://" + host + ":" + port + "/sensei";
  }

  public String getStoreGetUrl() {
    return getStoreGetUrl(false, false);
  }

  public String getStoreGetUrl(boolean includeValue, boolean includeFields) {
    String baseUrl;
    if (url != null) {
      baseUrl = url + "/get";
    } else {
      baseUrl = "http://" + host + ":" + port + "/sensei/get";
    }
    if (includeValue && includeFields) {
      return baseUrl + "?value=true&fields=true";
    } else if(includeValue) {
      return baseUrl + "?value=true";
    } else if (includeFields) {
      return baseUrl + "?fields=true";
    } else {
      return baseUrl;
    }
  }

  private JSONObject jsonResponse(String output) throws JSONException {
    return new JSONObject(output);
  }

  byte[] drain(InputStream inputStream) throws IOException {
    try {
      byte[] buf = new byte[1024];
      int len;
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      while ((len = inputStream.read(buf)) > 0) {
        byteArrayOutputStream.write(buf, 0, len);
      }
      return byteArrayOutputStream.toByteArray();
    } finally {
      inputStream.close();
    }
  }

  public String sendPostRaw(String urlStr, String requestStr) {
    return this.sendPostRaw(urlStr, requestStr, null);
  }

  public String sendPostRaw(String urlStr, String requestStr, Map<String, String> headers) {
    HttpURLConnection conn = null;
    try {
      URL url = new URL(urlStr);
      conn = (HttpURLConnection) url.openConnection();
      conn.setDoOutput(true);
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Accept-Encoding", "gzip");

      String string = requestStr;
      byte[] requestBytes = string.getBytes("UTF-8");
      conn.setRequestProperty("Content-Length", String.valueOf(requestBytes.length));
      conn.setRequestProperty("http.keepAlive", String.valueOf(true));
      conn.setRequestProperty("default", String.valueOf(true));

      if (headers != null && headers.size() > 0) {
        Set<Entry<String, String>> entries = headers.entrySet();
        for (Entry<String, String> entry : entries) {
          conn.setRequestProperty(entry.getKey(), entry.getValue());
        }
      }

      OutputStream os = new BufferedOutputStream(conn.getOutputStream());
      os.write(requestBytes);
      os.flush();
      os.close();
      int responseCode = conn.getResponseCode();

      if (responseCode != HttpURLConnection.HTTP_OK) {
        throw new IOException("Failed to " + url + " : HTTP error code : " + responseCode);
      }
      byte[] bytes = drain(new GZIPInputStream(new BufferedInputStream(conn.getInputStream())));

      String output = new String(bytes, "UTF-8");
      return output;
    } catch (Exception ex) {
      throw new RuntimeException("Exception in POST to " + url, ex);
    } finally {
      if (conn != null) conn.disconnect();
    }
  }

}
