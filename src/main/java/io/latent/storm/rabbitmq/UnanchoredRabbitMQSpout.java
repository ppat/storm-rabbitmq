package io.latent.storm.rabbitmq;

import io.latent.storm.rabbitmq.config.ConsumerConfig;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SpoutOutputCollector;

import java.util.List;

/**
 * A RabbitMQ spout that emits an unanchored tuple stream on default stream. Should be used when Storm's guaranteed message
 * processing is not needed. Messages will be removed from RabbitMQ queue as soon as it's delivered to storm and will not be
 * retried on any errors during processing.
 *
 * @author peter@latent.io
 */
public class UnanchoredRabbitMQSpout extends RabbitMQSpout {
  public UnanchoredRabbitMQSpout(Scheme scheme) {
    super(scheme);
  }

  public UnanchoredRabbitMQSpout(Scheme scheme,
                                 Declarator declarator) {
    super(scheme, declarator);
  }

  @Override
  protected RabbitMQConsumer loadConsumer(Declarator declarator,
                                          ErrorReporter reporter,
                                          ConsumerConfig config) {
    return new UnanchoredConsumer(config.getConnectionConfig(),
                                  config.getPrefetchCount(),
                                  config.getQueueName(),
                                  config.isRequeueOnFail(),
                                  declarator,
                                  reporter);
  }

  @Override
  public void ack(Object msgId) { /* no op */ }

  @Override
  public void fail(Object msgId) { /* no op */ }

  @Override
  protected List<Integer> emit(List<Object> tuple,
                               Message message,
                               SpoutOutputCollector spoutOutputCollector) {
    // don't anchor with msgId
    return spoutOutputCollector.emit(tuple);
  }
}
