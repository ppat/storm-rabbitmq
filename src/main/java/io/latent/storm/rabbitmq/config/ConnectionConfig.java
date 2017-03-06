package io.latent.storm.rabbitmq.config;

import static io.latent.storm.rabbitmq.config.ConfigUtils.addToMap;
import static io.latent.storm.rabbitmq.config.ConfigUtils.getFromMap;
import static io.latent.storm.rabbitmq.config.ConfigUtils.getFromMapAsBoolean;
import static io.latent.storm.rabbitmq.config.ConfigUtils.getFromMapAsInt;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.security.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.rabbitmq.client.ConnectionFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

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
    // Backup hosts to try and connect to.
    private ConfigAvailableHosts highAvailabilityHosts = new ConfigAvailableHosts();
    // Use AMQP URI http://www.rabbitmq.com/uri-spec.html
    private String uri;

    //ssh config
    private String keyStorePassword;
    private String keyStoreFile;
    private String keyStoreType;
    private String keyStoreManagerType;
    private String trustStorePassword;
    private String trustStoreFile;
    private String trustStoreType;
    private String trustStoreManagerType;
    private String sslProtocol;

    public static ConnectionConfig forTest() {
        return new ConnectionConfig(ConnectionFactory.DEFAULT_HOST, ConnectionFactory.DEFAULT_USER, ConnectionFactory.DEFAULT_PASS);
    }

    public ConnectionConfig(String uri) {
        this.uri = uri;
    }

    public ConnectionConfig(String host,
                            String username,
                            String password) {
        this(host, ConnectionFactory.DEFAULT_AMQP_PORT, username, password, ConnectionFactory.DEFAULT_VHOST, 10, false);
    }

    public ConnectionConfig(String host,
                            String username,
                            String password, boolean ssl) {
        this(host, ConnectionFactory.DEFAULT_AMQP_PORT, username, password, ConnectionFactory.DEFAULT_VHOST, 10, ssl);
    }

    public ConnectionConfig(String host,
                            int port,
                            String username,
                            String password,
                            String virtualHost,
                            int heartBeat) {
        this(host,port,username,password,virtualHost,heartBeat,false);
    }

    public ConnectionConfig(String host, int port, String username, String password, String virtualHost, int heartBeat, boolean ssl) {
        this(new ConfigAvailableHosts(), host, port, username, password, virtualHost, heartBeat, ssl);
    }

    /**
     * Use this constructor if you wish to specify a set of
     * hosts to connect to in the event that you need a high
     * availability RabbitMQ connection.
     *
     * @param hosts The {@link ConfigAvailableHosts} that will give you the ability to specify a set of hosts
     * @param username
     * @param password
     * @param virtualHost
     */
    public ConnectionConfig(final ConfigAvailableHosts hosts, String host, int port, String username, String password, String virtualHost, int heartBeat, final boolean ssl) {
        this(hosts, host, port, username, password, virtualHost, heartBeat, ssl, null, null, null, null, null, null, null, null, null);
    }

    public ConnectionConfig(String host, int port, String username, String password,
                            String virtualHost, int heartBeat, final boolean ssl,
                            String keyStorePassword, String keyStoreFile, String keyStoreType, String keyStoreManagerType,
                            String trustStorePassword, String trustStoreFile, String trustStoreType, String trustStoreManagerType, String sslProtocol) {
        this (new ConfigAvailableHosts(), host, port, username, password, virtualHost, heartBeat, ssl,
                keyStorePassword, keyStoreFile, keyStoreType, keyStoreManagerType, trustStorePassword, trustStoreFile,
                trustStoreType, trustStoreManagerType, sslProtocol);
    }

    public ConnectionConfig(final ConfigAvailableHosts hosts, String host, int port, String username, String password,
                            String virtualHost, int heartBeat, final boolean ssl,
                            String keyStorePassword, String keyStoreFile, String keyStoreType, String keyStoreManagerType,
                            String trustStorePassword, String trustStoreFile, String trustStoreType, String trustStoreManagerType, String sslProtocol) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.virtualHost = virtualHost;
        this.heartBeat = heartBeat;
        this.ssl = ssl;
        this.highAvailabilityHosts = hosts;
        this.keyStorePassword=keyStorePassword;
        this.keyStoreFile=keyStoreFile;
        this.keyStoreType=keyStoreType;
        this.keyStoreManagerType=keyStoreManagerType;
        this.trustStorePassword=trustStorePassword;
        this.trustStoreFile=trustStoreFile;
        this.trustStoreType=trustStoreType;
        this.trustStoreManagerType=trustStoreManagerType;
        this.sslProtocol=sslProtocol;
    }

    public ConfigAvailableHosts getHighAvailabilityHosts() {
        return highAvailabilityHosts;
    }

    /**
     * Set this value if you want to use a set of high availability hosts
     * in addition to the specified primary host you want to connect to,
     * and didn't use the full constructor.
     *
     * @param highAvailabilityHosts The host configuration for using backup hosts
     */
    public void setHighAvailabilityHosts(ConfigAvailableHosts highAvailabilityHosts) {
        this.highAvailabilityHosts = highAvailabilityHosts;
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

    boolean isSsl(){
        return this.ssl;
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
            if(ssl){
                try {
                    factory.useSslProtocol(initSSLContext());
                } catch (KeyManagementException e) {
                    throw new RuntimeException(e);
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException(e);
                } catch (GeneralSecurityException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return factory;
    }
    private SSLContext initSSLContext() throws GeneralSecurityException, IOException {
        char[] keystorePassword = this.keyStorePassword.toCharArray();

        KeyStore keyStore = KeyStore.getInstance(keyStoreType);
        keyStore.load(new FileInputStream(keyStoreFile), keystorePassword);

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(keyStoreManagerType);
        kmf.init(keyStore, keystorePassword);

        char[] truststorePassword = (trustStorePassword).toCharArray();
        KeyStore trustStore = KeyStore.getInstance(trustStoreType);
        trustStore.load(new FileInputStream(trustStoreFile), truststorePassword);

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(trustStoreManagerType);
        tmf.init(trustStore);

        SSLContext sslContext = SSLContext.getInstance(sslProtocol);
        sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());

        return sslContext;
    }
    public static ConnectionConfig getFromStormConfig(Map<String, Object> stormConfig) {
        if (stormConfig.containsKey("rabbitmq.uri")) {
            return new ConnectionConfig(getFromMap("rabbitmq.uri", stormConfig));
        } else {
            String highAvailabilityHostsString = getFromMap("rabbitmq.ha.hosts", stormConfig);
            if(highAvailabilityHostsString != null){
                final ConfigAvailableHosts haHosts = ConfigAvailableHosts.fromString(highAvailabilityHostsString);
                return new ConnectionConfig(haHosts,
                        getFromMap("rabbitmq.host", stormConfig, ConnectionFactory.DEFAULT_HOST),
                        getFromMapAsInt("rabbitmq.port", stormConfig, ConnectionFactory.DEFAULT_AMQP_PORT),
                        getFromMap("rabbitmq.username", stormConfig, ConnectionFactory.DEFAULT_USER),
                        getFromMap("rabbitmq.password", stormConfig, ConnectionFactory.DEFAULT_PASS),
                        getFromMap("rabbitmq.virtualhost", stormConfig, ConnectionFactory.DEFAULT_VHOST),
                        getFromMapAsInt("rabbitmq.heartbeat", stormConfig, ConnectionFactory.DEFAULT_HEARTBEAT),
                        getFromMapAsBoolean("rabbitmq.ssl", stormConfig, false),
                        getFromMap("rabbitmq.keyStorePassword", stormConfig, null),
                        getFromMap("rabbitmq.keyStoreFile", stormConfig, null),
                        getFromMap("rabbitmq.keyStoreType", stormConfig, null),
                        getFromMap("rabbitmq.keyStoreManagerType", stormConfig, null),
                        getFromMap("rabbitmq.trustStorePassword", stormConfig, null),
                        getFromMap("rabbitmq.trustStoreFile", stormConfig, null),
                        getFromMap("rabbitmq.trustStoreType", stormConfig, null),
                        getFromMap("rabbitmq.trustStoreManagerType", stormConfig, null),
                        getFromMap("rabbitmq.sslProtocol", stormConfig, null)
                );
            }else{
                return new ConnectionConfig(
                        getFromMap("rabbitmq.host", stormConfig, ConnectionFactory.DEFAULT_HOST),
                        getFromMapAsInt("rabbitmq.port", stormConfig, ConnectionFactory.DEFAULT_AMQP_PORT),
                        getFromMap("rabbitmq.username", stormConfig, ConnectionFactory.DEFAULT_USER),
                        getFromMap("rabbitmq.password", stormConfig, ConnectionFactory.DEFAULT_PASS),
                        getFromMap("rabbitmq.virtualhost", stormConfig, ConnectionFactory.DEFAULT_VHOST),
                        getFromMapAsInt("rabbitmq.heartbeat", stormConfig, ConnectionFactory.DEFAULT_HEARTBEAT),
                        getFromMapAsBoolean("rabbitmq.ssl", stormConfig, false),
                        getFromMap("rabbitmq.keyStorePassword", stormConfig, null),
                        getFromMap("rabbitmq.keyStoreFile", stormConfig, null),
                        getFromMap("rabbitmq.keyStoreType", stormConfig, null),
                        getFromMap("rabbitmq.keyStoreManagerType", stormConfig, null),
                        getFromMap("rabbitmq.trustStorePassword", stormConfig, null),
                        getFromMap("rabbitmq.trustStoreFile", stormConfig, null),
                        getFromMap("rabbitmq.trustStoreType", stormConfig, null),
                        getFromMap("rabbitmq.trustStoreManagerType", stormConfig, null),
                        getFromMap("rabbitmq.sslProtocol", stormConfig, null)
                );
            }
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
            addToMap("rabbitmq.ha.hosts", map, highAvailabilityHosts.toString());
            addToMap("rabbitmq.keyStorePassword", map, keyStorePassword);
            addToMap("rabbitmq.keyStoreFile", map, keyStoreFile);
            addToMap("rabbitmq.keyStoreType", map, keyStoreType);
            addToMap("rabbitmq.keyStoreManagerType", map, keyStoreManagerType);
            addToMap("rabbitmq.trustStorePassword", map, trustStorePassword);
            addToMap("rabbitmq.trustStoreFile", map, trustStoreFile);
            addToMap("rabbitmq.trustStoreType", map, trustStoreType);
            addToMap("rabbitmq.trustStoreManagerType", map, trustStoreManagerType);
            addToMap("rabbitmq.sslProtocol", map, sslProtocol);
        }
        return map;
    }
}
