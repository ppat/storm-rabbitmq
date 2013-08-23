package io.latent.storm.rabbitmq;

import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import io.latent.storm.rabbitmq.config.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A simple RabbitMQ spout that emits an anchored tuple stream (on the default stream). This can be used with
 * Storm's guaranteed message processing.
 * 
 * @author peter@latent.io
 */
public class RabbitMQSpout extends BaseRichSpout {
  private final MessageScheme scheme;
  private final Declarator declarator;

  private Logger logger;
  private RabbitMQConsumer consumer;
  private SpoutOutputCollector collector;

  public RabbitMQSpout(MessageScheme scheme) {
    this(scheme, new Declarator.NoOp());
  }

  public RabbitMQSpout(Scheme scheme) {
    this(MessageScheme.Builder.from(scheme), new Declarator.NoOp());
  }

  public RabbitMQSpout(Scheme scheme, Declarator declarator) {
    this(MessageScheme.Builder.from(scheme), declarator);
  }

  public RabbitMQSpout(MessageScheme scheme, Declarator declarator) {
    this.scheme = scheme;
    this.declarator = declarator;
  }

  @Override
  public void open(final Map config,
                   final TopologyContext context,
                   final SpoutOutputCollector spoutOutputCollector) {
    ConsumerConfig consumerConfig = ConsumerConfig.getFromStormConfig(config);
    ErrorReporter reporter = new ErrorReporter() {
      @Override
      public void reportError(Throwable error) {
        spoutOutputCollector.reportError(error);
      }
    };
    consumer = loadConsumer(declarator, reporter, consumerConfig);
    scheme.open(config, context);
    consumer.open();
    logger = LoggerFactory.getLogger(RabbitMQSpout.class);
    collector = spoutOutputCollector;
  }

  protected RabbitMQConsumer loadConsumer(Declarator declarator,
                                          ErrorReporter reporter,
                                          ConsumerConfig config) {
    return new RabbitMQConsumer(config.getConnectionConfig(),
                                config.getPrefetchCount(),
                                config.getQueueName(),
                                config.isRequeueOnFail(),
                                declarator,
                                reporter);
  }

  @Override
  public void close() {
    consumer.close();
    scheme.close();
    super.close();
  }

  @Override
  public void nextTuple() {
    Message message = consumer.nextMessage();
    if (message != Message.NONE) {
      List<Object> tuple = extractTuple(message);
      if (!tuple.isEmpty()) {
        emit(tuple, message, collector);
      }
    }
  }

  protected List<Integer> emit(List<Object> tuple,
                               Message message,
                               SpoutOutputCollector spoutOutputCollector) {
    return spoutOutputCollector.emit(tuple, getDeliveryTag(message));
  }

  private List<Object> extractTuple(Message message) {
    long deliveryTag = getDeliveryTag(message);
    try {
      List<Object> tuple = scheme.deserialize(message.getBody());
      if (tuple != null && !tuple.isEmpty()) {
        return tuple;
      }
      logger.warn("Deserialization error for msgId " + deliveryTag);
    } catch (Exception e) {
      logger.warn("Deserialization error for msgId " + deliveryTag, e);
      collector.reportError(e);
    }
    // get the malformed message out of the way by dead-lettering (if dead-lettering is configured) and move on
    consumer.deadLetter(deliveryTag);
    return Collections.emptyList();
  }

  @Override
  public void ack(Object msgId) {
    if (msgId instanceof Long) consumer.ack((Long) msgId);
  }

  @Override
  public void fail(Object msgId) {
    if (msgId instanceof Long) consumer.fail((Long) msgId);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(scheme.getOutputFields());
  }

  protected long getDeliveryTag(Message message) {
    return ((Message.DeliveredMessage) message).getDeliveryTag();
  }
}
