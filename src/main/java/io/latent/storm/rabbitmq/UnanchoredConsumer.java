package io.latent.storm.rabbitmq;

import io.latent.storm.rabbitmq.config.ConnectionConfig;

public class UnanchoredConsumer extends RabbitMQConsumer
{
  public UnanchoredConsumer(ConnectionConfig connectionConfig,
                            int prefetchCount,
                            String queueName,
                            boolean requeueOnFail,
                            Declarator declarator,
                            ErrorReporter errorReporter)
  {
    super(connectionConfig, prefetchCount, queueName, requeueOnFail, declarator, errorReporter);
  }

  @Override
  public void ack(Long msgId)
  {
  }

  @Override
  public void fail(Long msgId)
  {
  }

  @Override
  public void failWithRedelivery(Long msgId)
  {
  }

  @Override
  public void deadLetter(Long msgId)
  {
  }

  @Override
  protected boolean isAutoAcking()
  {
    return true;
  }
}
