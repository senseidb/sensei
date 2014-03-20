package com.senseidb.indexing.activity;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.json.JSONObject;

import com.senseidb.conf.SenseiSchema;
import com.senseidb.conf.SenseiSchema.FieldDefinition;

public class DefaultActivityFilter extends BaseActivityFilter {

  private volatile HashSet<String> cachedActivities;

  @Override
  public boolean acceptEventsForAllPartitions() {
    return false;
  }

  @Override
  public ActivityFilteredResult filter(JSONObject event, SenseiSchema senseiSchema) {
    Map<String, Object> columnValues = new HashMap<String, Object>();
    for (String activityField : getActivities(senseiSchema)) {
      Object obj = event.opt(activityField);
      if (obj != null) {
        event.remove(activityField);
        columnValues.put(activityField, obj);
      }
    }
    ActivityFilteredResult activityFilteredResult = new ActivityFilteredResult(event, columnValues);
    return activityFilteredResult;
  }

  private Set<String> getActivities(SenseiSchema senseiSchema) {
    if (cachedActivities == null) {
      cachedActivities = new HashSet<String>();
      for (String fieldName : senseiSchema.getFieldDefMap().keySet()) {
        FieldDefinition fieldDefinition = senseiSchema.getFieldDefMap().get(fieldName);
        if (fieldDefinition.isActivity) {
          cachedActivities.add(fieldName);
        }
      }
    }
    return cachedActivities;
  }

}
