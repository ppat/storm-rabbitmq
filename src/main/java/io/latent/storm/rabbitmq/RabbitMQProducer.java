package io.latent.storm.rabbitmq;

import com.rabbitmq.client.*;
import io.latent.storm.rabbitmq.config.ConnectionConfig;
import org.apache.storm.topology.ReportedFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class RabbitMQProducer implements Serializable {
  private final Declarator declarator;

  private transient Logger logger;

  private transient ConnectionConfig connectionConfig;
  private transient Connection connection;
  private transient Channel channel;

  private boolean blocked = false;

  public RabbitMQProducer()
  {
    this(new Declarator.NoOp());
  }

  public RabbitMQProducer(Declarator declarator) {
    this.declarator = declarator;
  }

  public void send(Message message) {
    if (message == Message.NONE) return;
    sendMessageWhenNotBlocked((Message.MessageForSending) message);
  }

  private void sendMessageWhenNotBlocked(Message.MessageForSending message) {
    while (true) {
      if (blocked) {
        try { Thread.sleep(100); } catch (InterruptedException ie) { }
      } else {
        sendMessageActual(message);
        return;
      }
    }
  }

  private void sendMessageActual(Message.MessageForSending message) {

    reinitIfNecessary();
    if (channel == null) throw new ReportedFailedException("No connection to RabbitMQ");
    try {
      AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                                                                .contentType(message.getContentType())
                                                                .contentEncoding(message.getContentEncoding())
                                                                .deliveryMode((message.isPersistent()) ? 2 : 1)
                                                                .headers(message.getHeaders())
                                                                .build();
      channel.basicPublish(message.getExchangeName(), message.getRoutingKey(), properties, message.getBody());
    } catch (AlreadyClosedException ace) {
      logger.error("already closed exception while attempting to send message", ace);
      reset();
      throw new ReportedFailedException(ace);
    } catch (IOException ioe) {
      logger.error("io exception while attempting to send message", ioe);
      reset();
      throw new ReportedFailedException(ioe);
    } catch (Exception e) {
      logger.warn("Unexpected error while sending message. Backing off for a bit before trying again (to allow time for recovery)", e);
      try { Thread.sleep(1000); } catch (InterruptedException ie) { }
    }
  }

  public void open(final Map config) {
    logger = LoggerFactory.getLogger(RabbitMQProducer.class);
    connectionConfig = ConnectionConfig.getFromStormConfig(config);
    internalOpen();
  }

  private void internalOpen() {
    try {
      connection = createConnection();
      channel = connection.createChannel();

      // run any declaration prior to message sending
      declarator.execute(channel);
    } catch (Exception e) {
      logger.error("could not open connection on rabbitmq", e);
      reset();
    }
  }

  public void close() {
    try {
      if (channel != null && channel.isOpen()) {
        channel.close();
      }
    } catch (Exception e) {
      logger.debug("error closing channel", e);
    }
    try {
      logger.info("closing connection to rabbitmq: " + connection);
      connection.close();
    } catch (Exception e) {
      logger.debug("error closing connection", e);
    }
    channel = null;
    connection = null;
  }

  private void reset() {
    channel = null;
  }

  private void reinitIfNecessary() {
    if (channel == null) {
      close();
      internalOpen();
    }
  }

  private Connection createConnection() throws IOException, TimeoutException {
    ConnectionFactory connectionFactory = connectionConfig.asConnectionFactory();
    Connection connection = connectionConfig.getHighAvailabilityHosts().isEmpty() ? connectionFactory.newConnection()
        : connectionFactory.newConnection(connectionConfig.getHighAvailabilityHosts().toAddresses());
    connection.addShutdownListener(new ShutdownListener() {
      @Override
      public void shutdownCompleted(ShutdownSignalException cause) {
        logger.error("shutdown signal received", cause);
        reset();
      }
    });
    connection.addBlockedListener(new BlockedListener()
    {
      @Override
      public void handleBlocked(String reason) throws IOException
      {
        blocked = true;
        logger.warn(String.format("Got blocked by rabbitmq with reason = %s", reason));
      }

      @Override
      public void handleUnblocked() throws IOException
      {
        blocked = false;
        logger.warn(String.format("Got unblocked by rabbitmq"));
      }
    });
    logger.info("connected to rabbitmq: " + connection);
    return connection;
  }
}
