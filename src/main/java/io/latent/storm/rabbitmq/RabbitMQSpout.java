package io.latent.storm.rabbitmq;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import io.latent.storm.rabbitmq.config.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A simple RabbitMQ spout that emits an anchored tuple stream (on the default stream). This can be used with
 * Storm's guaranteed message processing.
 * 
 * @author peter@latent.io
 */
public class RabbitMQSpout implements IRichSpout {
  private final String configKey;
  private final MessageScheme scheme;
  private final Declarator declarator;

  private ConsumerConfig consumerConfig;

  private Logger logger;
  private RabbitMQConsumer consumer;
  private boolean active;
  private SpoutOutputCollector collector;

  public RabbitMQSpout(String configKey, MessageScheme scheme) {
    this(configKey, scheme, new Declarator.NoOp());
  }

  public RabbitMQSpout(String configKey, MessageScheme scheme, Declarator declarator) {
    this.configKey = configKey;
    this.scheme = scheme;
    this.declarator = declarator;
  }

  @Override
  public void open(final Map config,
                   final TopologyContext context,
                   final SpoutOutputCollector spoutOutputCollector) {
    consumerConfig = ConsumerConfig.getFromStormConfig(configKey, config);
    ErrorReporter reporter = new ErrorReporter() {
      @Override
      public void reportError(Throwable error) {
        collector.reportError(error);
      }
    };
    consumer = new RabbitMQConsumer(consumerConfig.getConnectionConfig(),
                                    consumerConfig.getPrefetchCount(),
                                    consumerConfig.getQueueName(),
                                    consumerConfig.isRequeueOnFail(),
                                    consumerConfig.isAutoAck(),
                                    declarator,
                                    reporter);
    scheme.open(config, context);
    consumer.open();
    logger = LoggerFactory.getLogger(RabbitMQSpout.class);
    collector = spoutOutputCollector;
  }

  @Override
  public void close() {
    consumer.close();
    scheme.close();
  }

  @Override
  public void activate() {
    active = true;
    logger.info(String.format("rabbitmq spout for %s activated", consumerConfig.getQueueName()));
  }

  @Override
  public void deactivate() {
    active = false;
    logger.info(String.format("rabbitmq spout for %s deactivated", consumerConfig.getQueueName()));
  }

  @Override
  public void nextTuple() {
    if (active) {
      Message message = consumer.nextMessage();
      if (message != Message.NONE) {
        List<Object> tuple = extractTuple(message);
        if (!tuple.isEmpty()) {
          emit(tuple, message, collector);
        }
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
      logger.warn("Deserialization error for message " + deliveryTag);
    } catch (Exception e) {
      logger.warn("Deserialization error for message " + deliveryTag, e);
    }
    // get the malformed message out of the way by dead-lettering (if dead-lettering is configured) and move on
    consumer.deadLetter(deliveryTag);
    return Collections.emptyList();
  }

  @Override
  public void ack(Object msgId) {
    consumer.ack((Long) msgId);
  }

  @Override
  public void fail(Object msgId) {
    consumer.fail((Long) msgId);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(scheme.getOutputFields());
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return new HashMap<String, Object>();
  }

  protected long getDeliveryTag(Message message) {
    return ((Message.DeliveredMessage) message).getDeliveryTag();
  }
}
