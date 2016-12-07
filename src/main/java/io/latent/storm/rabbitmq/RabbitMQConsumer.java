package io.latent.storm.rabbitmq;

import io.latent.storm.rabbitmq.config.ConnectionConfig;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * An abstraction on RabbitMQ client API to encapsulate interaction with RabbitMQ and de-couple Storm API from RabbitMQ API.
 *
 * @author peter@latent.io
 */
public class RabbitMQConsumer implements Serializable {
  public static final long MS_WAIT_FOR_MESSAGE = 1L;

  private final ConnectionFactory connectionFactory;
  private final Address[] highAvailabilityHosts;
  private final int prefetchCount;
  private final String queueName;
  private final boolean requeueOnFail;
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
                          Declarator declarator,
                          ErrorReporter errorReporter) {
    this.connectionFactory = connectionConfig.asConnectionFactory();
    this.highAvailabilityHosts = connectionConfig.getHighAvailabilityHosts().toAddresses();
    this.prefetchCount = prefetchCount;
    this.queueName = queueName;
    this.requeueOnFail = requeueOnFail;
    this.declarator = declarator;

    this.reporter = errorReporter;
    this.logger = LoggerFactory.getLogger(RabbitMQConsumer.class);
  }

  public Message nextMessage() {
    reinitIfNecessary();
    if (consumerTag == null || consumer == null) return Message.NONE;
    try {
      return Message.forDelivery(consumer.nextDelivery(MS_WAIT_FOR_MESSAGE));
    } catch (ShutdownSignalException sse) {
      reset();
      logger.error("shutdown signal received while attempting to get next message", sse);
      reporter.reportError(sse);
      return Message.NONE;
    } catch (InterruptedException ie) {
      /* nothing to do. timed out waiting for message */
      logger.debug("interruepted while waiting for message", ie);
      return Message.NONE;
    } catch (ConsumerCancelledException cce) {
      /* if the queue on the broker was deleted or node in the cluster containing the queue failed */
      reset();
      logger.error("consumer got cancelled while attempting to get next message", cce);
      reporter.reportError(cce);
      return Message.NONE;
    }
  }

  public void ack(Long msgId) {
    reinitIfNecessary();
    try {
      channel.basicAck(msgId, false);
    } catch (ShutdownSignalException sse) {
      reset();
      logger.error("shutdown signal received while attempting to ack message", sse);
      reporter.reportError(sse);
    } catch (Exception e) {
      logger.error("could not ack for msgId: " + msgId, e);
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
    reinitIfNecessary();
    try {
      channel.basicReject(msgId, true);
    } catch (ShutdownSignalException sse) {
      reset();
      logger.error("shutdown signal received while attempting to fail with redelivery", sse);
      reporter.reportError(sse);
    } catch (Exception e) {
      logger.error("could not fail with redelivery for msgId: " + msgId, e);
      reporter.reportError(e);
    }
  }

  public void deadLetter(Long msgId) {
    reinitIfNecessary();
    try {
      channel.basicReject(msgId, false);
    } catch (ShutdownSignalException sse) {
      reset();
      logger.error("shutdown signal received while attempting to fail with no redelivery", sse);
      reporter.reportError(sse);
    } catch (Exception e) {
      logger.error("could not fail with dead-lettering (when configured) for msgId: " + msgId, e);
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
      consumerTag = channel.basicConsume(queueName, isAutoAcking(), consumer);
    } catch (Exception e) {
      reset();
      logger.error("could not open listener on queue " + queueName);
      reporter.reportError(e);
    }
  }

  protected boolean isAutoAcking() {
    return false;
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

  private Connection createConnection() throws IOException, TimeoutException {
    Connection connection = highAvailabilityHosts == null || highAvailabilityHosts.length == 0 
          ? connectionFactory.newConnection() 
          : connectionFactory.newConnection(highAvailabilityHosts);
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
