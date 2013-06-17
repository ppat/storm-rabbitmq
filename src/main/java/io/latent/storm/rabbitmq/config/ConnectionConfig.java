package io.latent.storm.rabbitmq.config;

import com.rabbitmq.client.ConnectionFactory;

import java.io.Serializable;
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

  public static ConnectionConfig getFromStormConfig(String key, Map<String, Object> stormConfig) {
    return new ConnectionConfig(getFromMap(key, "host", stormConfig),
                                getFromMapAsInt(key, "port", stormConfig),
                                getFromMap(key, "username", stormConfig),
                                getFromMap(key, "password", stormConfig),
                                getFromMap(key, "virtualhost", stormConfig),
                                getFromMapAsInt(key, "heartbeat", stormConfig));
  }

  public void addToStormConfig(String key, Map<String, Object> stormConfig) {
    addToMap(key, "host", stormConfig, host);
    addToMap(key, "port", stormConfig, port);
    addToMap(key, "username", stormConfig, username);
    addToMap(key, "password", stormConfig, password);
    addToMap(key, "virtualhost", stormConfig, virtualHost);
    addToMap(key, "heartbeat", stormConfig, heartBeat);
  }
}
