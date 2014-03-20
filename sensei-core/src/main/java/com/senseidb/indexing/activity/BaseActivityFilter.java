package com.senseidb.indexing.activity;

import java.util.Map;

import org.json.JSONObject;

import com.senseidb.conf.SenseiSchema;
import com.senseidb.plugin.SenseiPlugin;
import com.senseidb.plugin.SenseiPluginRegistry;

public abstract class BaseActivityFilter implements SenseiPlugin {
  protected Map<String, String> config;
  protected SenseiPluginRegistry pluginRegistry;

  @Override
  public final void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
    this.config = config;
    this.pluginRegistry = pluginRegistry;
  }

  public abstract ActivityFilteredResult filter(JSONObject event, SenseiSchema senseiSchema);

  public boolean acceptEventsForAllPartitions() {
    return false;
  }

  public static class ActivityFilteredResult {
    private JSONObject filteredObject;
    private final Map<String, Object> activityValues;

    public ActivityFilteredResult(JSONObject filteredObject,
        Map<String, Object> activityValues) {
      super();
      this.filteredObject = filteredObject;
      this.activityValues = activityValues;
    }

    public JSONObject getFilteredObject() {
      return filteredObject;
    }

    public void setFilteredObject(JSONObject filteredObject) {
      this.filteredObject = filteredObject;
    }

    public Map<String, Object> getActivityValues() {
      return activityValues;
    }
  }

  @Override
  public void start() {

  }

  @Override
  public void stop() {

  }
}
