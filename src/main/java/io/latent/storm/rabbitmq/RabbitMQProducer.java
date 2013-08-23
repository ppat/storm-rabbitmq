package io.latent.storm.rabbitmq;

import backtype.storm.topology.ReportedFailedException;
import com.rabbitmq.client.*;
import io.latent.storm.rabbitmq.config.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public class RabbitMQProducer implements Serializable {
  private final Declarator declarator;

  private Logger logger;

  private ProducerConfig producerConfig;
  private Connection connection;
  private Channel channel;

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
                   String routingKey) {
    if (message == Message.NONE) return;
    reinitIfNecessary();
    if (channel == null) throw new ReportedFailedException("No connection to RabbitMQ");
    try {
      AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                                                                .contentType(producerConfig.getContentType())
                                                                .contentEncoding(producerConfig.getContentEncoding())
                                                                .deliveryMode((producerConfig.isPersistent()) ? 2 : 1)
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
    logger.info("connected to rabbitmq: " + connection + " for " + producerConfig.getExchangeName());
    return connection;
  }
}
