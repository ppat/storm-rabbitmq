package io.latent.storm.rabbitmq.config;

import java.util.Map;

public class ConfigUtils
{
  public static String getFromMap(String key, Map<String, Object> map) {
    return map.get(key).toString();
  }
  
  public static String getFromMap(String key, Map<String, Object> map, String defaultValue) {
    Object value = map.get(key);
    if (value==null) return defaultValue;
    return map.get(key).toString();
  }

  public static int getFromMapAsInt(String key, Map<String, Object> map) {
    return Integer.valueOf(map.get(key).toString());
  }
  
  public static int getFromMapAsInt(String key, Map<String, Object> map, int defaultValue) {
    Object value = map.get(key);
    if (value==null) return defaultValue;
    return Integer.valueOf(map.get(key).toString());
  }

  public static boolean getFromMapAsBoolean(String key, Map<String, Object> map) {
    return Boolean.valueOf(map.get(key).toString());
  }
  
  public static boolean getFromMapAsBoolean(String key, Map<String, Object> map, boolean defaultValue) {
    Object value = map.get(key);
    if (value==null) return defaultValue;
    return Boolean.valueOf(map.get(key).toString());
  }

  public static void addToMap(String key, Map<String, Object> map, String value) {
    map.put(key, value);
  }

  public static void addToMap(String key, Map<String, Object> map, int value) {
    map.put(key, Integer.toString(value));
  }

  public static void addToMap(String key, Map<String, Object> map, boolean value) {
    map.put(key, Boolean.toString(value));
  }
}
