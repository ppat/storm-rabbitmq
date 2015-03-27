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
import backtype.storm.utils.Utils;

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

  private boolean active;
  private final String streamId;
  private int _nextTupleCallsWithoutMessage=0;

  public RabbitMQSpout(Scheme scheme) {
    this(MessageScheme.Builder.from(scheme), new Declarator.NoOp(),null);
  }
  
  public RabbitMQSpout(Scheme scheme, String streamId){
      this(MessageScheme.Builder.from(scheme), new Declarator.NoOp(), streamId);
  }

  public RabbitMQSpout(Scheme scheme, Declarator declarator) {
    this(MessageScheme.Builder.from(scheme), declarator,null);
  }

  public RabbitMQSpout(MessageScheme scheme, Declarator declarator) {
    this(scheme,declarator,null);
  }
  
  public RabbitMQSpout(Scheme scheme, Declarator declarator, String streamId){
      this(MessageScheme.Builder.from(scheme), declarator, streamId);
  }
  
  public RabbitMQSpout(MessageScheme scheme, Declarator declarator, String streamId){
      this.scheme =scheme;
      this.declarator =declarator;
      this.streamId = streamId;
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
    active = true;
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
    if (!active) return;
    final Message message= consumer.nextMessage();
    if (emptyMessage(message)) {
    	Utils.sleep(waitTime());
    } else {
      final List<Object> tuple = extractTuple(message);
      if (!tuple.isEmpty()) {
        emit(tuple, message, collector);
      }
    }
  }

private boolean emptyMessage(final Message message) {
	final boolean emptyMessage = message  == Message.NONE;
	if (!emptyMessage) {
		resetNextTupleCalls();
	}
	return emptyMessage;
}

private void resetNextTupleCalls() {
	_nextTupleCallsWithoutMessage=0;
}

  private long waitTime() {
	return 50*_nextTupleCallsWithoutMessage++;
}

protected List<Integer> emit(List<Object> tuple,
                               Message message,
                               SpoutOutputCollector spoutOutputCollector) {
    return streamId == null ? spoutOutputCollector.emit(tuple, getDeliveryTag(message)) : 
      spoutOutputCollector.emit(streamId, tuple, getDeliveryTag(message));
  }

  private List<Object> extractTuple(Message message) {
    final long deliveryTag = getDeliveryTag(message);
    try {
      final List<Object> tuple = scheme.deserialize(message);
      if (tuple != null && !tuple.isEmpty()) {
        return tuple;
      }
      final String errorMsg = "Deserialization error for msgId " + deliveryTag;
      logger.warn(errorMsg);
      collector.reportError(new Exception(errorMsg));
    } catch (final Exception e) {
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
    if(streamId == null){
      outputFieldsDeclarer.declare(scheme.getOutputFields());
    }else{
      outputFieldsDeclarer.declareStream(streamId, scheme.getOutputFields());
    }
  }

  @Override
  public void deactivate()
  {
    super.deactivate();
    active = false;
  }

  @Override
  public void activate()
  {
    super.activate();
    active = true;
  }

  protected long getDeliveryTag(Message message) {
    return ((Message.DeliveredMessage) message).getDeliveryTag();
  }
}
