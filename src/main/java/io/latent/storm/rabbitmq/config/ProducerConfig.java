package io.latent.storm.rabbitmq.config;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static io.latent.storm.rabbitmq.config.ConfigUtils.*;

public class ProducerConfig implements Serializable
{
  private final ConnectionConfig connectionConfig;
  private final String exchangeName;
  private final String contentType;
  private final String contentEncoding;
  private final boolean persistent;

  public ProducerConfig(ConnectionConfig connectionConfig,
                        String exchangeName,
                        String contentType,
                        String contentEncoding,
                        boolean persistent)
  {
    this.connectionConfig = connectionConfig;
    this.exchangeName = exchangeName;
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

  public static ProducerConfig getFromStormConfig(String keyPrefix, Map<String, Object> stormConfig) {
    ConnectionConfig connectionConfig = ConnectionConfig.getFromStormConfig(keyPrefix, stormConfig);
    return new ProducerConfig(connectionConfig,
                              getFromMap(keyPrefix, "exchangeName", stormConfig),
                              getFromMap(keyPrefix, "contentType", stormConfig),
                              getFromMap(keyPrefix, "contentEncoding", stormConfig),
                              getFromMapAsBoolean(keyPrefix, "persistent", stormConfig));
  }

  public Map<String, Object> asMap(String keyPrefix) {
    Map<String, Object> map = new HashMap<String, Object>();
    map.putAll(connectionConfig.asMap(keyPrefix));
    addToMap(keyPrefix, "exchangeName", map, exchangeName);
    addToMap(keyPrefix, "contentType", map, contentType);
    addToMap(keyPrefix, "contentEncoding", map, contentEncoding);
    addToMap(keyPrefix, "persistent", map, persistent);
    return map;
  }
}
