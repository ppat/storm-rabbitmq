package io.latent.storm.rabbitmq;

import backtype.storm.tuple.Tuple;
import io.latent.storm.rabbitmq.config.ProducerConfig;

import java.util.Map;

public abstract class TupleToMessageNonDynamic extends TupleToMessage
{
  private String exchangeName;
  private String routingKey;
  private String contentType;
  private String contentEncoding;
  private boolean persistent;

  @Override
  void prepare(@SuppressWarnings("rawtypes") Map stormConfig)
  {
    ProducerConfig producerConfig = ProducerConfig.getFromStormConfig(stormConfig);
    exchangeName = producerConfig.getExchangeName();
    routingKey = producerConfig.getRoutingKey();
    contentType = producerConfig.getContentType();
    contentEncoding = producerConfig.getContentEncoding();
    persistent = producerConfig.isPersistent();
  }

  @Override
  String determineExchangeName(Tuple input)
  {
    return exchangeName;
  }

  @Override
  String determineRoutingKey(Tuple input)
  {
    return routingKey;
  }

  @Override
  String specifyContentType(Tuple input)
  {
    return contentType;
  }

  @Override
  String specifyContentEncoding(Tuple input)
  {
    return contentEncoding;
  }

  @Override
  boolean specifyMessagePersistence(Tuple input)
  {
    return persistent;
  }
}
