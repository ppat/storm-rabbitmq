package io.latent.storm.rabbitmq.config;

import java.io.Serializable;
import java.util.Map;

import static io.latent.storm.rabbitmq.config.ConfigUtils.*;

public class ConsumerConfig implements Serializable {
  private final ConnectionConfig connectionConfig;
  private final int prefetchCount;
  private final String queueName;
  private final boolean requeueOnFail;
  private final boolean autoAck;

  public ConsumerConfig(ConnectionConfig connectionConfig,
                        int prefetchCount,
                        String queueName,
                        boolean requeueOnFail,
                        boolean autoAck) {
    if (requeueOnFail && autoAck) throw new IllegalArgumentException("Cannot requeue on fail when auto acking is enabled");
    if (connectionConfig == null || prefetchCount < 1) {
      throw new IllegalArgumentException("Invalid configuration");
    }

    this.connectionConfig = connectionConfig;
    this.prefetchCount = prefetchCount;
    this.queueName = queueName;
    this.requeueOnFail = requeueOnFail;
    this.autoAck = autoAck;
  }

  public ConnectionConfig getConnectionConfig() {
    return connectionConfig;
  }

  public int getPrefetchCount() {
    return prefetchCount;
  }

  public String getQueueName() {
    return queueName;
  }

  public boolean isRequeueOnFail() {
    return requeueOnFail;
  }

  public boolean isAutoAck() {
    return autoAck;
  }

  public static ConsumerConfig getFromStormConfig(String key, Map<String, Object> stormConfig) {
    ConnectionConfig connectionConfig = ConnectionConfig.getFromStormConfig(key, stormConfig);
    return new ConsumerConfig(connectionConfig,
                              getFromMapAsInt(key, "prefetchCount", stormConfig),
                              getFromMap(key, "queueName", stormConfig),
                              getFromMapAsBoolean(key, "requeueOnFail", stormConfig),
                              getFromMapAsBoolean(key, "autoAck", stormConfig));
  }

  public void addToStormConfig(String key, Map<String, Object> map) {
    connectionConfig.addToStormConfig(key, map);
    addToMap(key, "prefetchCount", map, prefetchCount);
    addToMap(key, "queueName", map, queueName);
    addToMap(key, "requeueOnFail", map, requeueOnFail);
    addToMap(key, "autoAck", map, autoAck);
  }
}

