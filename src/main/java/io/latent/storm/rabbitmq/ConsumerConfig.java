package io.latent.storm.rabbitmq;

import backtype.storm.spout.Scheme;

import java.io.Serializable;

public class ConsumerConfig implements Serializable {
  private final ConnectionConfig connectionConfig;
  private final int prefetchCount;
  private final String queueName;
  private final boolean requeueOnFail;
  private final boolean autoAck;
  private final Declarator declarator;
  private final Scheme scheme;

  public ConsumerConfig(ConnectionConfig connectionConfig,
                        int prefetchCount,
                        String queueName,
                        Scheme scheme) {
    this(connectionConfig, prefetchCount, queueName, true, false, new Declarator.NoOp(), scheme);
  }

  public ConsumerConfig(ConnectionConfig connectionConfig,
                        int prefetchCount,
                        String queueName,
                        boolean requeueOnFail,
                        boolean autoAck,
                        Declarator declarator,
                        Scheme scheme) {
    this.connectionConfig = connectionConfig;
    this.prefetchCount = prefetchCount;
    this.queueName = queueName;
    this.requeueOnFail = requeueOnFail;
    this.autoAck = autoAck;
    this.declarator = declarator;
    this.scheme = scheme;
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

  public Declarator getDeclarator() {
    return declarator;
  }

  public Scheme getScheme() {
    return scheme;
  }
}
