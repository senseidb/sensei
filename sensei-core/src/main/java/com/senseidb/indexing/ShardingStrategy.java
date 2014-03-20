package com.senseidb.indexing;

import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.plugin.SenseiPluginFactory;
import com.senseidb.plugin.SenseiPluginRegistry;

public interface ShardingStrategy {
  int caculateShard(int maxShardId, JSONObject dataObj) throws JSONException;

  public static class FieldModShardingStrategy implements ShardingStrategy {
    public static class Factory implements SenseiPluginFactory<FieldModShardingStrategy> {
      @Override
      public FieldModShardingStrategy getBean(Map<String, String> initProperties,
          String fullPrefix, SenseiPluginRegistry pluginRegistry) {
        return new FieldModShardingStrategy(initProperties.get("field"));
      }
    }

    protected String _field;

    public FieldModShardingStrategy(String field) {
      _field = field;
    }

    @Override
    public int caculateShard(int maxShardId, JSONObject dataObj) throws JSONException {
      long uid = Long.parseLong(dataObj.getString(_field));
      return (int) (Math.abs(uid) % maxShardId);
    }
  }
}
