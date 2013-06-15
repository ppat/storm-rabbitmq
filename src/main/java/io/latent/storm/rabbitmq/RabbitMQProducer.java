package io.latent.storm.rabbitmq;

import backtype.storm.topology.ReportedFailedException;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RabbitMQProducer {
  private final ConnectionFactory connectionFactory;
  private final String exchangeName;
  private final Declarator declarator;
  private final String contentType;
  private final String contentEncoding;
  private final boolean persistent;
  private final ErrorReporter reporter;
  private final Logger logger;

  private Connection connection;
  private Channel channel;

  public RabbitMQProducer(ConnectionFactory connectionFactory,
                          String exchangeName,
                          Declarator declarator,
                          String contentType,
                          String contentEncoding,
                          boolean persistent,
                          ErrorReporter reporter) {
    this.connectionFactory = connectionFactory;
    this.exchangeName = exchangeName;
    this.declarator = declarator;
    this.contentType = contentType;
    this.contentEncoding = contentEncoding;
    this.persistent = persistent;
    this.reporter = reporter;
    this.logger = LoggerFactory.getLogger(RabbitMQProducer.class);
  }

  public void send(Message message) {
    send(message, "");
  }

  public void send(Message message,
                   String routingKey) throws ReportedFailedException {
    if (message == Message.NONE) return;
    reinitIfNecessary();
    try {
      AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                                                                .contentType(contentType)
                                                                .contentEncoding(contentEncoding)
                                                                .deliveryMode((persistent) ? 2 : 1)
                                                                .build();
      channel.basicPublish(exchangeName, routingKey, properties, message.getBody());
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

  public void open() {
    try {
      connection = createConnection();
      channel = connection.createChannel();

      // run any declaration prior to message sending
      declarator.execute(channel);
    } catch (Exception e) {
      logger.error("could not open connection on exchange " + exchangeName);
      reporter.reportError(e);
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
    logger.info("connected to rabbitmq: " + connection + " for " + exchangeName);
    return connection;
  }
}
