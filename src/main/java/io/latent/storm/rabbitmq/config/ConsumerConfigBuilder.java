package io.latent.storm.rabbitmq.config;

public final class ConsumerConfigBuilder
{
  private ConnectionConfig connectionConfig;
  private int prefetchCount;
  private String queueName;
  private boolean requeueOnFail;

  public ConsumerConfigBuilder()
  {
  }

  public ConsumerConfigBuilder connection(ConnectionConfig connection) {
    this.connectionConfig = connection;
    return this;
  }

  public ConsumerConfigBuilder prefetch(int prefetch) {
    this.prefetchCount = prefetch;
    return this;
  }

  public ConsumerConfigBuilder queue(String queue) {
    this.queueName = queue;
    return this;
  }

  public ConsumerConfigBuilder requeueOnFail() {
    this.requeueOnFail = true;
    return this;
  }

  public ConsumerConfig build() {
    return new ConsumerConfig(connectionConfig, prefetchCount, queueName, requeueOnFail);
  }
}
