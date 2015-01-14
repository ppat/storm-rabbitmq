package io.latent.storm.rabbitmq.config;

import static io.latent.storm.rabbitmq.config.ConfigUtils.addToMap;
import static io.latent.storm.rabbitmq.config.ConfigUtils.getFromMap;
import static io.latent.storm.rabbitmq.config.ConfigUtils.getFromMapAsInt;

import java.io.Serializable;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import com.rabbitmq.client.ConnectionFactory;

public class ConnectionConfig implements Serializable {

	/**
	 * Serial version UID.
	 */
	private static final long serialVersionUID = 1L;

	// Use named parameters
	private String host;
	private int port;
	private String username;
	private String password;
	private String virtualHost;
	private int heartBeat;
	private boolean ssl;

	// Use AMQP URI http://www.rabbitmq.com/uri-spec.html
	private String uri;

	public static ConnectionConfig forTest() {
		return new ConnectionConfig(ConnectionFactory.DEFAULT_HOST, ConnectionFactory.DEFAULT_USER, ConnectionFactory.DEFAULT_PASS);
	}

	public ConnectionConfig(String uri) {
		this.uri = uri;
	}

	public ConnectionConfig(String host, String username, String password) {
		this(host, ConnectionFactory.DEFAULT_AMQP_PORT, username, password, ConnectionFactory.DEFAULT_VHOST, 10);
	}

	public ConnectionConfig(String host, int port, String username, String password, String virtualHost, int heartBeat) {
		this(host, port, username, password, virtualHost, heartBeat, false);
	}

	public ConnectionConfig(String host, int port, String username, String password, String virtualHost, int heartBeat, boolean ssl) {
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
		this.virtualHost = virtualHost;
		this.heartBeat = heartBeat;
		this.ssl = ssl;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	public String getVirtualHost() {
		return virtualHost;
	}

	public int getHeartBeat() {
		return heartBeat;
	}

	public String getUri() {
		return uri;
	}

	public ConnectionFactory asConnectionFactory() {
		ConnectionFactory factory = new ConnectionFactory();
		if (uri != null) {
			try {
				factory.setUri(uri);
			} catch (URISyntaxException e) {
				throw new RuntimeException(e);
			} catch (NoSuchAlgorithmException e) {
				throw new RuntimeException(e);
			} catch (KeyManagementException e) {
				throw new RuntimeException(e);
			}
		} else {
			factory.setHost(host);
			factory.setPort(port);
			factory.setUsername(username);
			factory.setPassword(password);
			factory.setVirtualHost(virtualHost);
			factory.setRequestedHeartbeat(heartBeat);
			if (ssl) {
				try {
					factory.useSslProtocol();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}
		return factory;
	}

	public static ConnectionConfig getFromStormConfig(Map<String, Object> stormConfig) {
		if (stormConfig.containsKey("rabbitmq.uri")) {
			return new ConnectionConfig(getFromMap("rabbitmq.uri", stormConfig));
		} else {
			return new ConnectionConfig(getFromMap("rabbitmq.host", stormConfig, ConnectionFactory.DEFAULT_HOST), getFromMapAsInt("rabbitmq.port", stormConfig,
					ConnectionFactory.DEFAULT_AMQP_PORT), getFromMap("rabbitmq.username", stormConfig, ConnectionFactory.DEFAULT_USER), getFromMap(
					"rabbitmq.password", stormConfig, ConnectionFactory.DEFAULT_PASS), getFromMap("rabbitmq.virtualhost", stormConfig,
					ConnectionFactory.DEFAULT_VHOST), getFromMapAsInt("rabbitmq.heartbeat", stormConfig, ConnectionFactory.DEFAULT_HEARTBEAT),
					ConfigUtils.getFromMapAsBoolean("rabbitmq.ssl", stormConfig, false));
		}
	}

	public Map<String, Object> asMap() {
		Map<String, Object> map = new HashMap<String, Object>();
		if (uri != null) {
			addToMap("rabbitmq.uri", map, uri);
		} else {
			addToMap("rabbitmq.host", map, host);
			addToMap("rabbitmq.port", map, port);
			addToMap("rabbitmq.username", map, username);
			addToMap("rabbitmq.password", map, password);
			addToMap("rabbitmq.virtualhost", map, virtualHost);
			addToMap("rabbitmq.heartbeat", map, heartBeat);
			addToMap("rabbitmq.ssl", map, ssl);
		}
		return map;
	}
}
