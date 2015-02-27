package io.latent.storm.rabbitmq;

import io.latent.storm.rabbitmq.config.ConsumerConfig;

import java.util.Map;

abstract class RabbitMQUtils {

	/**
	 * Load a new rabbitmq consumer
	 * 
	 * @param declarator the declarator
	 * @param reporter the error reporter
	 * @param config the consumer configuration
	 * @return returns a new RabbitMQConsumer instance
	 */
	public static RabbitMQConsumer loadConsumer(Declarator declarator, ErrorReporter reporter, ConsumerConfig config) {
		return new RabbitMQConsumer(config.getConnectionConfig(), 
				config.getPrefetchCount(), 
				config.getQueueName(),
				config.isRequeueOnFail(), 
				declarator, 
				reporter);
	}

	public static RabbitMQConsumer loadConsumer(Declarator declarator, ErrorReporter reporter, Map config) {
		final ConsumerConfig consumerConfig = ConsumerConfig.getFromStormConfig(config);
		return loadConsumer(declarator, reporter, consumerConfig);
	}

	/**
	 * Returns the delivery tag id for the message
	 * 
	 * @param message
	 * @return deliveryTag id
	 */
	public static long deliveryTagForMessage(Message message) {
		return ((Message.DeliveredMessage) message).getDeliveryTag();
	}

	public static Map<String, Object> headersForMessage(Message message) {
		return ((Message.DeliveredMessage) message).getHeaders();
	}

}
