package com.senseidb.search.req;

import java.util.List;

import org.json.JSONObject;

/**
 * Used to modify/validate Json request before it would be sent to Sensei's broker
 *
 */
public interface RequestPostProcessor {
      List<SenseiError> process(JSONObject senseiRequest);
}
