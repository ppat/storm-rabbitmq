package io.latent.storm.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;

import java.io.Serializable;

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
    this.host = host;
    this.port = ConnectionFactory.DEFAULT_AMQP_PORT;
    this.username = username;
    this.password = password;
    this.virtualHost = ConnectionFactory.DEFAULT_VHOST;
    this.heartBeat = 10;
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
}
