package io.latent.storm.rabbitmq;

import io.latent.storm.rabbitmq.config.ConnectionConfig;
import io.latent.storm.rabbitmq.config.ProducerConfig;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pns.alltypes.rabbitmq.RabbitConnectionConfig;
import pns.alltypes.rabbitmq.RabbitMQConnectionManager;
import pns.alltypes.rabbitmq.RabbitMQConnectionManager.AmqpChannel;
import backtype.storm.topology.ReportedFailedException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;

public class RabbitMQProducer implements Serializable {
    private final Declarator declarator;

    private static RabbitMQConnectionManager RABBIT_MQ_CONNECTION_MANAGER;
    private transient Logger logger;

    private transient ConnectionConfig connectionConfig;
    private transient Channel channel;

    public RabbitMQProducer() {
        this(new Declarator.NoOp());
    }

    public RabbitMQProducer(final Declarator declarator) {
        this.declarator = declarator;
    }

    public void send(final Message message) {
        if (message == Message.NONE) {
            return;
        }
        sendMessageActual((Message.MessageForSending) message);
    }

    private void sendMessageActual(final Message.MessageForSending message) {

        // wait until channel is avaialable

        try {
            final AmqpChannel localChannel = RabbitMQProducer.RABBIT_MQ_CONNECTION_MANAGER.getChannel();
            channel = localChannel.getChannel();
            final AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder().contentType(message.getContentType())
                    .contentEncoding(message.getContentEncoding()).deliveryMode(message.isPersistent() ? 2 : 1).headers(message.getHeaders()).build();
            channel.basicPublish(message.getExchangeName(), message.getRoutingKey(), properties, message.getBody());
        } catch (final AlreadyClosedException ace) {
            logger.error("already closed exception while attempting to send message", ace);
            throw new ReportedFailedException(ace);
        } catch (final IOException ioe) {
            logger.error("io exception while attempting to send message", ioe);
            throw new ReportedFailedException(ioe);
        } catch (final Exception e) {
            logger.warn("Unexpected error while sending message. Backing off for a bit before trying again (to allow time for recovery)", e);
            try {
                Thread.sleep(1000);
            } catch (final InterruptedException ie) {
            }
        } finally {

        }
    }

    public void open(final Map config) {
        logger = LoggerFactory.getLogger(RabbitMQProducer.class);
        connectionConfig = ProducerConfig.getFromStormConfig(config).getConnectionConfig();
        internalOpen();
    }

    private void internalOpen() {
        try {
            RabbitMQProducer.RABBIT_MQ_CONNECTION_MANAGER = RabbitMQConnectionManager.getInstance(new RabbitConnectionConfig(connectionConfig
                    .asConnectionFactory(), connectionConfig.getHighAvailabilityHosts() == null ? null : connectionConfig.getHighAvailabilityHosts()
                            .toAddresses()));
            RabbitMQProducer.RABBIT_MQ_CONNECTION_MANAGER.addConnection();// give a hint on no of conn
            RabbitMQProducer.RABBIT_MQ_CONNECTION_MANAGER.addChannel();// give a hint on no of channels

            // run any declaration prior to message sending
            declarator.execute(channel);
        } catch (final Exception e) {
            logger.error("could not open connection on rabbitmq", e);

        }
    }

    public void close() {
        // shutdown of rmq done on exit of VM.
    }

}
