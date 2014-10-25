package io.latent.storm.rabbitmq;

import io.latent.storm.rabbitmq.config.ConsumerConfig;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

/**
 * A simple RabbitMQ spout that emits an anchored tuple stream (on the default stream). This can be used with
 * Storm's guaranteed message processing.
 * 
 * @author peter@latent.io
 */
public class RabbitMQSpout extends BaseRichSpout {
  private final MessageScheme scheme;
  private final Declarator declarator;

  private transient Logger logger;
  private transient RabbitMQConsumer consumer;
  private transient SpoutOutputCollector collector;
  private transient Map topoConfig;
  private transient TopologyContext topoContext;
  private transient ConsumerConfig consConfig;
  private transient ErrorReporter reporter;
  
  
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
    consConfig = ConsumerConfig.getFromStormConfig(config);
    reporter = new ErrorReporter() {
      @Override
      public void reportError(Throwable error) {
        spoutOutputCollector.reportError(error);
      }
    };
    topoConfig = config;
    topoContext = context;
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
  public void activate() {
    logger.info("activating spout!");
    consumer = loadConsumer(declarator, reporter, consConfig);
    scheme.open(topoConfig, topoContext);
    consumer.open();
    super.activate();
  }
  
  @Override
  public void deactivate() {
    logger.info("deactivating spout!");
    consumer.close();
    scheme.close();
    consumer = null;
    super.deactivate();
  }

  @Override
  public void close() {
    consumer.close();
    scheme.close();
    super.close();
  }

  @Override
  public void nextTuple() {
    Message message;
    while ((message = consumer.nextMessage()) != Message.NONE) {
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
      List<Object> tuple = scheme.deserialize(message);
      if (tuple != null && !tuple.isEmpty()) {
        return tuple;
      }
      String errorMsg = "Deserialization error for msgId " + deliveryTag;
      logger.warn(errorMsg);
      collector.reportError(new Exception(errorMsg));
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
