package io.latent.storm.rabbitmq.config;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static io.latent.storm.rabbitmq.config.ConfigUtils.*;

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
                              getFromMap("rabbitmq.exchangeName", stormConfig),
                              getFromMap("rabbitmq.routingKey", stormConfig),
                              getFromMap("rabbitmq.contentType", stormConfig),
                              getFromMap("rabbitmq.contentEncoding", stormConfig),
                              getFromMapAsBoolean("rabbitmq.persistent", stormConfig));
  }

  public Map<String, Object> asMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.putAll(connectionConfig.asMap());
    addToMap("rabbitmq.exchangeName", map, exchangeName);
    addToMap("rabbitmq.routingKey", map, routingKey);
    addToMap("rabbitmq.contentType", map, contentType);
    addToMap("rabbitmq.contentEncoding", map, contentEncoding);
    addToMap("rabbitmq.persistent", map, persistent);
    return map;
  }
}
