package io.latent.storm.rabbitmq.config;

import java.io.Serializable;
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

  public static ProducerConfig getFromStormConfig(String key, Map<String, Object> stormConfig) {
    ConnectionConfig connectionConfig = ConnectionConfig.getFromStormConfig(key, stormConfig);
    return new ProducerConfig(connectionConfig,
                              getFromMap(key, "exchangeName", stormConfig),
                              getFromMap(key, "contentType", stormConfig),
                              getFromMap(key, "contentEncoding", stormConfig),
                              getFromMapAsBoolean(key, "persistent", stormConfig));
  }

  public void addToStormConfig(String key, Map<String, Object> map) {
    connectionConfig.addToStormConfig(key, map);
    addToMap(key, "exchangeName", map, exchangeName);
    addToMap(key, "contentType", map, contentType);
    addToMap(key, "contentEncoding", map, contentEncoding);
    addToMap(key, "persistent", map, persistent);
  }
}
