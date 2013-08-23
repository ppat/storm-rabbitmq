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

  public static ConnectionConfig getFromStormConfig(Map<String, Object> stormConfig) {
    return new ConnectionConfig(getFromMap("rabbitmq.host", stormConfig),
                                getFromMapAsInt("rabbitmq.port", stormConfig),
                                getFromMap("rabbitmq.username", stormConfig),
                                getFromMap("rabbitmq.password", stormConfig),
                                getFromMap("rabbitmq.virtualhost", stormConfig),
                                getFromMapAsInt("rabbitmq.heartbeat", stormConfig));
  }

  public Map<String, Object> asMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    addToMap("rabbitmq.host", map, host);
    addToMap("rabbitmq.port", map, port);
    addToMap("rabbitmq.username", map, username);
    addToMap("rabbitmq.password", map, password);
    addToMap("rabbitmq.virtualhost", map, virtualHost);
    addToMap("rabbitmq.heartbeat", map, heartBeat);
    return map;
  }
}
