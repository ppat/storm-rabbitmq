package io.latent.storm.rabbitmq;

import backtype.storm.spout.SpoutOutputCollector;

import java.util.List;

/**
 * A RabbitMQ spout that emits an unanchored tuple stream on default stream. Should be used when Storm's guaranteed message
 * processing is not needed. Messages will be removed from RabbitMQ queue as soon as it's delivered to storm and will not be
 * retried on any errors during processing.
 *
 * @author peter@latent.io
 */
public class UnanchordRabbitMQSpout extends RabbitMQSpout {

  public UnanchordRabbitMQSpout(String configKey, MessageScheme scheme)
  {
    super(configKey, scheme);
  }

  public UnanchordRabbitMQSpout(String configKey, MessageScheme scheme, Declarator declarator) {
    super(configKey, scheme, declarator);
  }

  @Override
  public void ack(Object msgId) {
    // no op
  }

  @Override
  public void fail(Object msgId) {
    // no op
  }

  @Override
  protected List<Integer> emit(List<Object> tuple,
                               Message message,
                               SpoutOutputCollector spoutOutputCollector) {
    // don't anchor with msgId
    return spoutOutputCollector.emit(tuple);
  }
}
