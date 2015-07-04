package io.latent.storm.rabbitmq;

import io.latent.storm.rabbitmq.config.ConnectionConfig;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pns.alltypes.rabbitmq.RabbitConnectionConfig;
import pns.alltypes.rabbitmq.RabbitMQConnectionManager;
import pns.alltypes.rabbitmq.RabbitMQConnectionManager.AmqpChannel;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * An abstraction on RabbitMQ client API to encapsulate interaction with RabbitMQ and de-couple Storm API from RabbitMQ
 * API.
 * @author peter@latent.io
 */
public class RabbitMQConsumer implements Serializable {
    public static final long MS_WAIT_FOR_MESSAGE = 1L;

    private final Address[] highAvailabilityHosts;
    private final int prefetchCount;
    private final String queueName;
    private final boolean requeueOnFail;
    private final Declarator declarator;
    private final ErrorReporter reporter;
    private final Logger logger;

    private Channel channel;
    private QueueingConsumer consumer;
    private String consumerTag;

    private final RabbitMQConnectionManager RABBIT_MQ_CONNECTION_MANAGER;

    public RabbitMQConsumer(final ConnectionConfig connectionConfig, final int prefetchCount, final String queueName, final boolean requeueOnFail,
            final Declarator declarator, final ErrorReporter errorReporter) {
        RABBIT_MQ_CONNECTION_MANAGER = RabbitMQConnectionManager.getInstance(new RabbitConnectionConfig(connectionConfig.asConnectionFactory(),
                connectionConfig.getHighAvailabilityHosts() == null ? null : connectionConfig.getHighAvailabilityHosts().toAddresses()));
        RABBIT_MQ_CONNECTION_MANAGER.hintResourceAddition();
        this.highAvailabilityHosts = connectionConfig.getHighAvailabilityHosts().toAddresses();
        this.prefetchCount = prefetchCount;
        this.queueName = queueName;
        this.requeueOnFail = requeueOnFail;
        this.declarator = declarator;

        this.reporter = errorReporter;
        this.logger = LoggerFactory.getLogger(RabbitMQConsumer.class);
    }

    public Message nextMessage() {
        if (consumerTag == null || consumer == null) {
            return Message.NONE;
        }
        try {
            return Message.forDelivery(consumer.nextDelivery(RabbitMQConsumer.MS_WAIT_FOR_MESSAGE));
        } catch (final ShutdownSignalException sse) {
            logger.error("shutdown signal received while attempting to get next message", sse);
            reporter.reportError(sse);
            return Message.NONE;
        } catch (final InterruptedException ie) {
            /* nothing to do. timed out waiting for message */
            logger.debug("interruepted while waiting for message", ie);
            return Message.NONE;
        } catch (final ConsumerCancelledException cce) {
            /* if the queue on the broker was deleted or node in the cluster containing the queue failed */
            logger.error("consumer got cancelled while attempting to get next message", cce);
            reporter.reportError(cce);
            return Message.NONE;
        }
    }

    public void ack(final Long msgId) {
        try {
            channel.basicAck(msgId, false);
        } catch (final ShutdownSignalException sse) {
            logger.error("shutdown signal received while attempting to ack message", sse);
            reporter.reportError(sse);
        } catch (final Exception e) {
            logger.error("could not ack for msgId: " + msgId, e);
            reporter.reportError(e);
        }
    }

    public void fail(final Long msgId) {
        if (requeueOnFail) {
            failWithRedelivery(msgId);
        } else {
            deadLetter(msgId);
        }
    }

    public void failWithRedelivery(final Long msgId) {
        try {
            channel.basicReject(msgId, true);
        } catch (final ShutdownSignalException sse) {
            logger.error("shutdown signal received while attempting to fail with redelivery", sse);
            reporter.reportError(sse);
        } catch (final Exception e) {
            logger.error("could not fail with redelivery for msgId: " + msgId, e);
            reporter.reportError(e);
        }
    }

    public void deadLetter(final Long msgId) {
        try {
            channel.basicReject(msgId, false);
        } catch (final ShutdownSignalException sse) {
            logger.error("shutdown signal received while attempting to fail with no redelivery", sse);
            reporter.reportError(sse);
        } catch (final Exception e) {
            logger.error("could not fail with dead-lettering (when configured) for msgId: " + msgId, e);
            reporter.reportError(e);
        }
    }

    public void open() {

        try {
            final AmqpChannel amqpChannel = RABBIT_MQ_CONNECTION_MANAGER.getChannel();
            channel = amqpChannel.getChannel();
            if (prefetchCount > 0) {
                logger.info("setting basic.qos / prefetch count to " + prefetchCount + " for " + queueName);
                channel.basicQos(prefetchCount);
            }
            // run any declaration prior to queue consumption
            declarator.execute(channel);

            consumer = new QueueingConsumer(channel);
            consumerTag = channel.basicConsume(queueName, isAutoAcking(), consumer);
        } catch (final Exception e) {
            logger.error("could not open listener on queue " + queueName);
            reporter.reportError(e);
        } finally {

        }
    }

    protected boolean isAutoAcking() {
        return false;
    }

    public void close() {

    }

}
