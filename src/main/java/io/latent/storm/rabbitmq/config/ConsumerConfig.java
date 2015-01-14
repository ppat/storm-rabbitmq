package io.latent.storm.rabbitmq.config;

import static io.latent.storm.rabbitmq.config.ConfigUtils.addToMap;
import static io.latent.storm.rabbitmq.config.ConfigUtils.getFromMap;
import static io.latent.storm.rabbitmq.config.ConfigUtils.getFromMapAsBoolean;
import static io.latent.storm.rabbitmq.config.ConfigUtils.getFromMapAsInt;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ConsumerConfig implements Serializable {
	/**
	 * Serial version UID.
	 */
	private static final long serialVersionUID = 1L;

	private final ConnectionConfig connectionConfig;
	private final int prefetchCount;
	private final String queueName;
	private final boolean requeueOnFail;

	public ConsumerConfig(ConnectionConfig connectionConfig, int prefetchCount, String queueName, boolean requeueOnFail) {
		if (connectionConfig == null || prefetchCount < 1) {
			throw new IllegalArgumentException("Invalid configuration");
		}

		this.connectionConfig = connectionConfig;
		this.prefetchCount = prefetchCount;
		this.queueName = queueName;
		this.requeueOnFail = requeueOnFail;
	}

	public ConnectionConfig getConnectionConfig() {
		return connectionConfig;
	}

	public int getPrefetchCount() {
		return prefetchCount;
	}

	public String getQueueName() {
		return queueName;
	}

	public boolean isRequeueOnFail() {
		return requeueOnFail;
	}

	public static ConsumerConfig getFromStormConfig(Map<String, Object> stormConfig) {
		ConnectionConfig connectionConfig = ConnectionConfig.getFromStormConfig(stormConfig);
		return new ConsumerConfig(connectionConfig, getFromMapAsInt("rabbitmq.prefetchCount", stormConfig), getFromMap("rabbitmq.queueName", stormConfig),
				getFromMapAsBoolean("rabbitmq.requeueOnFail", stormConfig));
	}

	public Map<String, Object> asMap() {
		Map<String, Object> map = new HashMap<String, Object>();
		map.putAll(connectionConfig.asMap());
		addToMap("rabbitmq.prefetchCount", map, prefetchCount);
		addToMap("rabbitmq.queueName", map, queueName);
		addToMap("rabbitmq.requeueOnFail", map, requeueOnFail);
		return map;
	}
}
