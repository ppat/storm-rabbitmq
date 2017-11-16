package io.latent.storm.rabbitmq.config;

public class ProducerConfigBuilder
{
  private ConnectionConfig connectionConfig;
  private String exchangeName;
  private String routingKey;
  private String contentType;
  private String contentEncoding;
  private boolean persistent = false;

  public ProducerConfigBuilder()
  {
  }

  public ProducerConfigBuilder connection(ConnectionConfig connection) {
    this.connectionConfig = connection;
    return this;
  }

  public ProducerConfigBuilder exchange(String exchange) {
    this.exchangeName = exchange;
    return this;
  }

  public ProducerConfigBuilder routingKey(String routingKey) {
    this.routingKey = routingKey;
    return this;
  }

  public ProducerConfigBuilder contentType(String contentType) {
    this.contentType = contentType;
    return this;
  }

  public ProducerConfigBuilder contentEncoding(String contentEncoding) {
    this.contentEncoding = contentEncoding;
    return this;
  }

  public ProducerConfigBuilder persistent() {
    this.persistent = true;
    return this;
  }

  public ProducerConfig build()
  {
    return new ProducerConfig(connectionConfig, exchangeName, routingKey, contentType, contentEncoding, persistent);
  }
}
