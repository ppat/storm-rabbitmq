package io.latent.storm.rabbitmq;

import io.latent.storm.rabbitmq.Message.MessageWithHeaders;
import io.latent.storm.rabbitmq.config.ProducerConfig;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.ReportedFailedException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

public class RabbitMQProducer implements Serializable {
  private final Declarator declarator;

  private transient Logger logger;

  private transient ProducerConfig producerConfig;
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
    send(message, "");
  }

  public void send(Message message,
                   String routingKey) { sendMessageWhenNotBlocked(message, routingKey); }

  public void sendMessageWhenNotBlocked(Message message,
                                        String routingKey)
  {
    while (true) {
      if (blocked) {
        try { Thread.sleep(100); } catch (InterruptedException ie) { }
      } else {
        sendMessageActual(message, routingKey);
        return;
      }
    }
  }

  private void sendMessageActual(Message message,
                                 String routingKey) {
    if (message == Message.NONE) return;
    reinitIfNecessary();
    if (channel == null) throw new ReportedFailedException("No connection to RabbitMQ");
    try {
      AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                                                                .contentType(producerConfig.getContentType())
                                                                .contentEncoding(producerConfig.getContentEncoding())
                                                                .deliveryMode((producerConfig.isPersistent()) ? 2 : 1)
                                                                .headers(getHeaders(message))
                                                                .build();
      channel.basicPublish(producerConfig.getExchangeName(), routingKey, properties, message.getBody());
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
    producerConfig = ProducerConfig.getFromStormConfig(config);
    internalOpen();
  }

  private void internalOpen() {
    try {
      connection = createConnection();
      channel = connection.createChannel();

      // run any declaration prior to message sending
      declarator.execute(channel);
    } catch (Exception e) {
      logger.error("could not open connection on exchange " + producerConfig.getExchangeName());
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

  private Connection createConnection() throws IOException {
    Connection connection = producerConfig.getConnectionConfig().asConnectionFactory().newConnection();
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
    logger.info("connected to rabbitmq: " + connection + " for " + producerConfig.getExchangeName());
    return connection;
  }
  
  /**
   * This method will simply pull the headers out of a {@link Message} if the
   * type is a {@link MessageWithHeaders}. Otherwise it spits back an empty
   * {@link HashMap}.
   * 
   * @param message
   *          The {@link Message} object to get headers from
   * @return The headers from the {@link MessageWithHeaders} or an empty
   *         {@link Map} of headers
   */
  private Map<String, Object> getHeaders(final Message message) {
    Map<String, Object> headers = new HashMap<String, Object>();
    if (message instanceof MessageWithHeaders) {
      headers = ((MessageWithHeaders) message).getHeaders();
    }
    return headers;
  }
}
