package ru.kiryam.storm.rabbitmq.config;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ProducerConfig implements Serializable
{
  private final ConnectionConfig connectionConfig;
  private final String exchangeName;
  private final String routingKey;
  private final String contentType;
  private final String contentEncoding;
  private final boolean persistent;

  public ProducerConfig(ConnectionConfig connectionConfig,
                        String exchangeName,
                        String routingKey,
                        String contentType,
                        String contentEncoding,
                        boolean persistent)
  {
    this.connectionConfig = connectionConfig;
    this.exchangeName = exchangeName;
    this.routingKey = routingKey;
    this.contentType = contentType;
    this.contentEncoding = contentEncoding;
    this.persistent = persistent;
  }

  public ConnectionConfig getConnectionConfig()
  {
    return connectionConfig;
  }

  public String getExchangeName()
  {
    return exchangeName;
  }

  public String getRoutingKey()
  {
    return routingKey;
  }

  public String getContentType()
  {
    return contentType;
  }

  public String getContentEncoding()
  {
    return contentEncoding;
  }

  public boolean isPersistent()
  {
    return persistent;
  }

  public static ProducerConfig getFromStormConfig(Map<String, Object> stormConfig) {
    ConnectionConfig connectionConfig = ConnectionConfig.getFromStormConfig(stormConfig);
    return new ProducerConfig(connectionConfig,
                              ConfigUtils.getFromMap("rabbitmq.exchangeName", stormConfig),
                              ConfigUtils.getFromMap("rabbitmq.routingKey", stormConfig),
                              ConfigUtils.getFromMap("rabbitmq.contentType", stormConfig),
                              ConfigUtils.getFromMap("rabbitmq.contentEncoding", stormConfig),
                              ConfigUtils.getFromMapAsBoolean("rabbitmq.persistent", stormConfig));
  }

  public Map<String, Object> asMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.putAll(connectionConfig.asMap());
    ConfigUtils.addToMap("rabbitmq.exchangeName", map, exchangeName);
    ConfigUtils.addToMap("rabbitmq.routingKey", map, routingKey);
    ConfigUtils.addToMap("rabbitmq.contentType", map, contentType);
    ConfigUtils.addToMap("rabbitmq.contentEncoding", map, contentEncoding);
    ConfigUtils.addToMap("rabbitmq.persistent", map, persistent);
    return map;
  }
}
