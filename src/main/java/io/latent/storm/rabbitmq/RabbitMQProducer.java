package io.latent.storm.rabbitmq;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.ReportedFailedException;
import com.rabbitmq.client.*;
import io.latent.storm.rabbitmq.config.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public class RabbitMQProducer implements Serializable {
  private final String configKey;
  private final Declarator declarator;

  private Logger logger;

  private ProducerConfig producerConfig;
  private Connection connection;
  private Channel channel;

  public RabbitMQProducer(String configKey)
  {
    this(configKey, new Declarator.NoOp());
  }

  public RabbitMQProducer(String configKey,
                          Declarator declarator) {
    this.configKey = configKey;
    this.declarator = declarator;
  }

  public void send(Message message,
                   ErrorReporter reporter) {
    send(message, "", reporter);
  }

  public void send(Message message,
                   String routingKey,
                   ErrorReporter reporter) throws ReportedFailedException {
    if (message == Message.NONE) return;
    reinitIfNecessary();
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
      reporter.reportError(ace);
    } catch (IOException ioe) {
      logger.error("io exception while attempting to send message", ioe);
      reset();
      reporter.reportError(ioe);
    }
  }

  public void open(final Map config,
                   final TopologyContext context) {
    logger = LoggerFactory.getLogger(RabbitMQProducer.class);
    producerConfig = ProducerConfig.getFromStormConfig(configKey, config);
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
