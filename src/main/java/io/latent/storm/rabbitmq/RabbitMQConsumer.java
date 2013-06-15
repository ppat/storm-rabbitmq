package io.latent.storm.rabbitmq;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * An abstraction on RabbitMQ client API to encapsulate interaction with RabbitMQ and de-couple Storm API from RabbitMQ API.
 *
 * @author peter@latent.io
 */
public class RabbitMQConsumer {
  private final ConnectionFactory connectionFactory;
  private final int prefetchCount;
  private final String queueName;
  private final boolean requeueOnFail;
  private final boolean autoAck;
  private final Declarator declarator;
  private final ErrorReporter reporter;
  private final Logger logger;

  private Connection connection;
  private Channel channel;
  private QueueingConsumer consumer;
  private String consumerTag;

  public RabbitMQConsumer(ConnectionConfig connectionConfig,
                          int prefetchCount,
                          String queueName,
                          boolean requeueOnFail,
                          boolean autoAck,
                          Declarator declarator,
                          ErrorReporter errorReporter) {
    this.connectionFactory = connectionConfig.asConnectionFactory();
    this.prefetchCount = prefetchCount;
    this.queueName = queueName;
    this.requeueOnFail = requeueOnFail;
    this.autoAck = autoAck;
    this.declarator = declarator;

    this.reporter = errorReporter;
    this.logger = LoggerFactory.getLogger(RabbitMQConsumer.class);
  }

  public Message nextMessage() {
    reinitIfNecessary();
    try {
      return Message.forDelivery(consumer.nextDelivery());
    } catch (ShutdownSignalException sse) {
      logger.error("shutdown signal received while attempting to get next message", sse);
      reporter.reportError(sse);
      reset();
      return Message.NONE;
    } catch (InterruptedException ie) {
      /* nothing to do. timed out waiting for message */
      logger.debug("interruepted while waiting for message", ie);
      return Message.NONE;
    } catch (ConsumerCancelledException cce) {
      /* if the queue on the broker was deleted or node in the cluster containing the queue failed */
      logger.error("consumer got cancelled while attempting to get next message", cce);
      reporter.reportError(cce);
      reset();
      return Message.NONE;
    }
  }

  public void ack(Long msgId) {
    if (autoAck) return;
    try {
      channel.basicAck(msgId, false);
    } catch (Exception e) {
      logger.error("failed to ack for delivery-tag: " + msgId, e);
      reporter.reportError(e);
    }
  }

  public void fail(Long msgId) {
    if (requeueOnFail)
      failWithRedelivery(msgId);
    else
      deadLetter(msgId);
  }

  public void failWithRedelivery(Long msgId) {
    if (autoAck) {
      logger.warn(String.format("auto-acking enabled so failed message (delivery-tag=%d) will not be retried", msgId));
      return;
    }
    try {
      channel.basicReject(msgId, true);
    } catch (Exception e) {
      logger.error("failed basicReject with redelivery for delivery-tag: " + msgId, e);
      reporter.reportError(e);
    }
  }

  public void deadLetter(Long msgId) {
    if (autoAck) return;
    try {
      channel.basicReject(msgId, false);
    } catch (Exception e) {
      logger.error("failed basicReject with dead-lettering (when configured) for delivery-tag: " + msgId, e);
      reporter.reportError(e);
    }
  }

  public void open() {
    try {
      connection = createConnection();
      channel = connection.createChannel();
      if (prefetchCount > 0) {
        logger.info("setting basic.qos / prefetch count to " + prefetchCount + " for " + queueName);
        channel.basicQos(prefetchCount);
      }
      // run any declaration prior to queue consumption
      declarator.execute(channel);

      consumer = new QueueingConsumer(channel);
      consumerTag = channel.basicConsume(queueName, autoAck, consumer);
    } catch (Exception e) {
      logger.error("could not open listener on queue " + queueName);
      reporter.reportError(e);
      reset();
    }
  }

  public void close() {
    try {
      if (channel != null && channel.isOpen()) {
        if (consumerTag != null) channel.basicCancel(consumerTag);
        channel.close();
      }
    } catch (Exception e) {
      logger.debug("error closing channel and/or cancelling consumer", e);
    }
    try {
      logger.info("closing connection to rabbitmq: " + connection);
      connection.close();
    } catch (Exception e) {
      logger.debug("error closing connection", e);
    }
    consumer = null;
    consumerTag = null;
    channel = null;
    connection = null;
  }

  private void reset() {
    consumerTag = null;
  }

  private void reinitIfNecessary() {
    if (consumerTag == null || consumer == null) {
      close();
      open();
    }
  }

  private Connection createConnection() throws IOException {
    Connection connection = connectionFactory.newConnection();
    connection.addShutdownListener(new ShutdownListener() {
      @Override
      public void shutdownCompleted(ShutdownSignalException cause) {
        logger.error("shutdown signal received", cause);
        reporter.reportError(cause);
        reset();
      }
    });
    logger.info("connected to rabbitmq: " + connection + " for " + queueName);
    return connection;
  }
}
