package com.senseidb.test.client;

import java.util.Arrays;
import java.util.Collections;

import com.senseidb.search.client.SenseiServiceProxy;
import com.senseidb.search.client.json.JsonSerializer;
import com.senseidb.search.client.req.Operator;
import com.senseidb.search.client.req.Selection;
import com.senseidb.search.client.req.SenseiClientRequest;
import com.senseidb.search.client.res.SenseiResult;

public class SendRawQuery {
  public static void main(String[] args) throws Exception {
    SenseiClientRequest senseiRequest = SenseiClientRequest
        .builder()
        .paging(10, 0)
        .fetchStored(true)
        .addSelection(
          Selection.terms("color", Arrays.asList("red", "blue"), Collections.<String> emptyList(),
            Operator.or)).build();
    String requestStr = JsonSerializer.serialize(senseiRequest).toString();
    System.out.println(requestStr);
    SenseiResult senseiResult = new SenseiServiceProxy("localhost", 8081).sendBQL("SELECT *");
    System.out.println(senseiResult.toString());
  }
}
