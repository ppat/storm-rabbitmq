package io.latent.storm.rabbitmq.config;

import java.util.Map;

public class ConfigUtils
{
  public static String getFromMap(String key, String suffix, Map<String, Object> map) {
    return map.get(key + "." + suffix).toString();
  }

  public static int getFromMapAsInt(String key, String suffix, Map<String, Object> map) {
    return Integer.valueOf(map.get(key + "." + suffix).toString());
  }

  public static boolean getFromMapAsBoolean(String key, String suffix, Map<String, Object> map) {
    return Boolean.valueOf(map.get(key + "." + suffix).toString());
  }

  public static void addToMap(String key, String suffix, Map<String, Object> map, String value) {
    map.put(key + "." + suffix, value);
  }

  public static void addToMap(String key, String suffix, Map<String, Object> map, int value) {
    map.put(key + "." + suffix, Integer.toString(value));
  }

  public static void addToMap(String key, String suffix, Map<String, Object> map, boolean value) {
    map.put(key + "." + suffix, Boolean.toString(value));
  }
}
