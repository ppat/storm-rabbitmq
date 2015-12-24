package com.narendra.trident.rabbitmq;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.RotatingMap;
import io.latent.storm.rabbitmq.Declarator;
import io.latent.storm.rabbitmq.ErrorReporter;
import io.latent.storm.rabbitmq.Message;
import io.latent.storm.rabbitmq.MessageScheme;
import io.latent.storm.rabbitmq.RabbitMQConsumer;
import io.latent.storm.rabbitmq.config.ConsumerConfig;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout.Emitter;
import storm.trident.topology.TransactionAttempt;

/**
 * The RabbitMQTridentEmitter class listens for incoming messages and stores them in a blocking
 * queue. On each invocation of emit, the queued messages are emitted as a batch.
 *
 */

@SuppressWarnings("rawtypes")
public class RabbitMQTridentEmitter implements Emitter<RabbitMQBatch> {

  private final Logger LOG;

  private int maxBatchSize = 3;
  private MessageScheme scheme = null;
  private transient RabbitMQConsumer consumer;
  private boolean active;
  private String streamId;

  private RotatingMap<Long, List<Message>> batchMessageMap = null; // Maps transaction Ids

  private long rotateTimeMillis;

  private long lastRotate;

  public static final String MAX_BATCH_SIZE_CONF = "topology.spout.max.batch.size";

  @SuppressWarnings("unchecked")
  public RabbitMQTridentEmitter(MessageScheme scheme, final TopologyContext context,
      Declarator declarator, String streamId, Map consumerMap) {

    batchMessageMap = new RotatingMap<Long, List<Message>>(3);
    ConsumerConfig consumerConfig = ConsumerConfig.getFromStormConfig(consumerMap);
    ErrorReporter reporter = new ErrorReporter() {
      @Override
      public void reportError(Throwable error) {

      }
    };
    this.scheme = scheme;
    consumer = loadConsumer(declarator, reporter, consumerConfig);
    scheme.open(consumerMap, context);
    consumer.open();
    maxBatchSize = Integer.parseInt(consumerMap.get(MAX_BATCH_SIZE_CONF).toString());
    LOG = LoggerFactory.getLogger(RabbitMQTridentEmitter.class);
    active = true;
    rotateTimeMillis =
        1000L * Integer.parseInt(consumerMap.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS).toString());
    lastRotate = System.currentTimeMillis();
  }



  protected RabbitMQConsumer loadConsumer(Declarator declarator, ErrorReporter reporter,
      ConsumerConfig config) {
    return new RabbitMQConsumer(config.getConnectionConfig(), config.getPrefetchCount(),
        config.getQueueName(), config.isRequeueOnFail(), declarator, reporter);
  }


  @Override
  public void success(TransactionAttempt tx) {

    @SuppressWarnings("unchecked")
    List<Message> messages = (List<Message>) batchMessageMap.remove(tx.getTransactionId());

    if (messages != null) {
      if (!messages.isEmpty()) {
        LOG.debug("Success for batch with transaction id " + tx.getTransactionId() + "/"
            + tx.getAttemptId() + "StreamId " + streamId);
      }

      for (Message msg : messages) {
        Long messageId = Long.MIN_VALUE;
        try {
          messageId = getDeliveryTag(msg);
          // acking is important
          if (messageId instanceof Long)
            consumer.ack((Long) messageId);
          LOG.trace("Acknowledged message " + messageId);
        } catch (Exception e) {
          LOG.warn("Failed to acknowledge message " + messageId, e);
        }
      }
    } else {
      LOG.warn("No messages found in batch with transaction id " + tx.getTransactionId() + "/"
          + tx.getAttemptId());
    }
  }



  @Override
  public void close() {
    try {
      LOG.info("Closing Consumer connection.");
      consumer.close();
      scheme.close();
    } catch (Exception e) {
      LOG.warn("Error closing Consumer connection.", e);
    }
  }

  @Override
  public void emitBatch(TransactionAttempt tx, RabbitMQBatch coordinatorMeta,
      TridentCollector collector) {


    long now = System.currentTimeMillis();
    if (now - lastRotate > rotateTimeMillis) {
      Map<Long, List<Message>> failed = batchMessageMap.rotate();
      for (Long id : failed.keySet()) {
        LOG.warn("TIMED OUT batch with transaction id " + id + " for " + streamId);
        fail(id, failed.get(id));
      }
      lastRotate = now;
    }


    if (batchMessageMap.containsKey(tx.getTransactionId())) {
      LOG.warn("FAILED duplicate batch with transaction id " + tx.getTransactionId() + "/"
          + tx.getAttemptId() + " for " + streamId);
      fail(tx.getTransactionId(), batchMessageMap.get(tx.getTransactionId()));
    }
    
    if (!active)
      return;
    int emitted = 0;
    Message message;

    List<Message> batchMessages = new ArrayList<Message>();

    while (emitted < maxBatchSize && (message = consumer.nextMessage()) != Message.NONE) {
      List<Object> tuple = extractTuple(message, collector);
      if (!tuple.isEmpty()) {
        batchMessages.add(message);
        collector.emit(tuple);
        emitted += 1;
      }

    }



    if (!batchMessages.isEmpty()) {
      LOG.debug("Emitting batch with transaction id " + tx.getTransactionId() + "/"
          + tx.getAttemptId() + " and size " + batchMessages.size() + " for " + streamId);
    } else {
      LOG.trace("No items to acknowledge for batch with transaction id " + tx.getTransactionId()
          + "/" + tx.getAttemptId() + " for " + streamId);
    }
    batchMessageMap.put(tx.getTransactionId(), batchMessages);
  }


  /**
   * Fail a batch with the given transaction id. This is called when a batch is timed out, or a new
   * batch with a matching transaction id is emitted. Note that the current implementation does
   * nothing - i.e. it discards messages that have been failed.
   * 
   * @param transactionId The transaction id of the failed batch
   * @param messages The list of messages to fail.
   */
  private void fail(Long transactionId, List<Message> messages) {
    LOG.debug("Failure for batch with transaction id " + transactionId + " for " + streamId);
    if (messages != null) {
      for (Message msg : messages) {
        try {
          Long msgId = getDeliveryTag(msg);
          if (msgId instanceof Long)
            consumer.fail((Long) msgId);
          LOG.trace("Failed message " + msgId);
        } catch (Exception e) {
          LOG.warn("Could not identify failed message ", e);
        }
      }
    } else {
      LOG.warn("Failed batch has no messages with transaction id " + transactionId);
    }
  }



  private List<Object> extractTuple(Message message, TridentCollector collector) {
    try {
      List<Object> tuple = scheme.deserialize(message);
      if (tuple != null && !tuple.isEmpty()) {
        return tuple;
      }
    } catch (Exception e) {
      collector.reportError(e);
    }
    return Collections.emptyList();
  }

  protected long getDeliveryTag(Message message) {
    return ((Message.DeliveredMessage) message).getDeliveryTag();
  }


}
