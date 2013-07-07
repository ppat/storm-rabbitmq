package io.latent.storm.rabbitmq.config;

import com.rabbitmq.client.ConnectionFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static io.latent.storm.rabbitmq.config.ConfigUtils.*;

public class ConnectionConfig implements Serializable {
  private final String host;
  private final int port;
  private final String username;
  private final String password;
  private final String virtualHost;
  private final int heartBeat;

  public static ConnectionConfig forTest() {
    return new ConnectionConfig(ConnectionFactory.DEFAULT_HOST, ConnectionFactory.DEFAULT_USER, ConnectionFactory.DEFAULT_PASS);
  }

  public ConnectionConfig(String host,
                          String username,
                          String password) {
    this(host, ConnectionFactory.DEFAULT_AMQP_PORT, username, password, ConnectionFactory.DEFAULT_VHOST, 10);
  }

  public ConnectionConfig(String host,
                          int port,
                          String username,
                          String password,
                          String virtualHost,
                          int heartBeat) {
    this.host = host;
    this.port = port;
    this.username = username;
    this.password = password;
    this.virtualHost = virtualHost;
    this.heartBeat = heartBeat;
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

  public ConnectionFactory asConnectionFactory() {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(host);
    factory.setPort(port);
    factory.setUsername(username);
    factory.setPassword(password);
    factory.setVirtualHost(virtualHost);
    factory.setRequestedHeartbeat(heartBeat);
    return factory;
  }

  public static ConnectionConfig getFromStormConfig(String keyPrefix, Map<String, Object> stormConfig) {
    return new ConnectionConfig(getFromMap(keyPrefix, "host", stormConfig),
                                getFromMapAsInt(keyPrefix, "port", stormConfig),
                                getFromMap(keyPrefix, "username", stormConfig),
                                getFromMap(keyPrefix, "password", stormConfig),
                                getFromMap(keyPrefix, "virtualhost", stormConfig),
                                getFromMapAsInt(keyPrefix, "heartbeat", stormConfig));
  }

  public Map<String, Object> asMap(String keyPrefix) {
    Map<String, Object> map = new HashMap<String, Object>();
    addToMap(keyPrefix, "host", map, host);
    addToMap(keyPrefix, "port", map, port);
    addToMap(keyPrefix, "username", map, username);
    addToMap(keyPrefix, "password", map, password);
    addToMap(keyPrefix, "virtualhost", map, virtualHost);
    addToMap(keyPrefix, "heartbeat", map, heartBeat);
    return map;
  }
}
