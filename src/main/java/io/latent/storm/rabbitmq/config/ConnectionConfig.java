package io.latent.storm.rabbitmq.config;

import com.rabbitmq.client.ConnectionFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Map;

import static io.latent.storm.rabbitmq.config.ConfigUtils.addToMap;
import static io.latent.storm.rabbitmq.config.ConfigUtils.getFromMap;
import static io.latent.storm.rabbitmq.config.ConfigUtils.getFromMapAsBoolean;
import static io.latent.storm.rabbitmq.config.ConfigUtils.getFromMapAsInt;

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
    private SslConnectionConfig sslConnectionConfig;

    // Backup hosts to try and connect to.
    private ConfigAvailableHosts highAvailabilityHosts = new ConfigAvailableHosts();

    // Use AMQP URI http://www.rabbitmq.com/uri-spec.html
    private String uri;

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
     * @param heartbeat
     */
    public ConnectionConfig(final ConfigAvailableHosts hosts, String host, int port, String username, String password, String virtualHost, int heartBeat, final boolean ssl) {
      this.host = host;
      this.port = port;
      this.username = username;
      this.password = password;
      this.virtualHost = virtualHost;
      this.heartBeat = heartBeat;
      this.ssl = ssl;
      this.highAvailabilityHosts = hosts;
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
                this.sslConnectionConfig.configureSsl(factory);
            }
        }
        return factory;
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
                    getFromMapAsBoolean("rabbitmq.ssl", stormConfig, false));
            }else{
                ConnectionConfig connConfig = new ConnectionConfig(getFromMap("rabbitmq.host", stormConfig, ConnectionFactory.DEFAULT_HOST),
                    getFromMapAsInt("rabbitmq.port", stormConfig, ConnectionFactory.DEFAULT_AMQP_PORT),
                    getFromMap("rabbitmq.username", stormConfig, ConnectionFactory.DEFAULT_USER),
                    getFromMap("rabbitmq.password", stormConfig, ConnectionFactory.DEFAULT_PASS),
                    getFromMap("rabbitmq.virtualhost", stormConfig, ConnectionFactory.DEFAULT_VHOST),
                    getFromMapAsInt("rabbitmq.heartbeat", stormConfig, ConnectionFactory.DEFAULT_HEARTBEAT),
                    getFromMapAsBoolean("rabbitmq.ssl", stormConfig, false));
                if (connConfig.isSsl()) {
                    connConfig.setSslConnectionConfig(new SslConnectionConfig(stormConfig));
                }
                return connConfig;
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
        }
        return map;
    }

    private void setSslConnectionConfig(SslConnectionConfig sslConnectionConfig) {
        this.sslConnectionConfig = sslConnectionConfig;
    }

    private static class SslConnectionConfig {
        String certType;
        String sslVersion;
        String keystorePath;
        String keystorePassword;
        String keystoreType;
        String truststorePath;
        String truststorePassword;
        String truststoreType;

        SslConnectionConfig(Map<String, Object> config) {
            certType = getFromMap("rabbitmq.certType", config, "SunX509");
            sslVersion = getFromMap("rabbitmq.sslVersion", config, "TLSv1.2");
            keystorePath = getFromMap("rabbitmq.keystorePath", config);
            keystorePassword = getFromMap("rabbitmq.keystorePassword", config);
            keystoreType = getFromMap("rabbitmq.keystoreType", config, "PKCS12");
            truststorePath = getFromMap("rabbitmq.truststorePath", config);
            truststorePassword = getFromMap("rabbitmq.truststorePassword", config);
            truststoreType = getFromMap("rabbitmq.truststoreType", config, "PKS");
        }

        private boolean isConfigured() {
            return keystorePath != null && keystorePassword != null && truststorePath != null &&
                    truststorePassword != null;
        }

        public void configureSsl(ConnectionFactory factory) {
            if (this.isConfigured()) {
                try {
                    char[] keyPassphrase;
                    KeyStore ks;
                    KeyManagerFactory kmf = KeyManagerFactory.getInstance(certType);

                    InputStream ksIn = getResource(keystorePath);
                    try {
                        keyPassphrase = keystorePassword.toCharArray();
                        ks = KeyStore.getInstance(keystoreType);
                        ks.load(ksIn, keyPassphrase);
                        kmf.init(ks, keyPassphrase);
                    } finally {
                        try {
                            ksIn.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    TrustManagerFactory tmf = TrustManagerFactory.getInstance(certType);
                    InputStream tksIn = getResource(this.truststorePath);
                    try {
                        char[] trustPassphrase = truststorePassword.toCharArray();
                        KeyStore tks = KeyStore.getInstance(truststoreType);
                        tks.load(tksIn, trustPassphrase);
                        tmf.init(tks);
                    } finally {
                        try {
                            tksIn.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    SSLContext c = SSLContext.getInstance(sslVersion);
                    c.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

                    factory.useSslProtocol(c);
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException(e);
                } catch (KeyStoreException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (CertificateException e) {
                    throw new RuntimeException(e);
                } catch (UnrecoverableKeyException e) {
                    throw new RuntimeException(e);
                } catch (KeyManagementException e) {
                    throw new RuntimeException(e);
                }
            } else {
                // non verifying
                try {
                    factory.useSslProtocol();
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException(e);
                } catch (KeyManagementException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private InputStream getResource(String resourcePath) throws FileNotFoundException {
            try {
                return new FileInputStream(resourcePath);
            } catch (FileNotFoundException e) {
                InputStream in = this.getClass().getResourceAsStream(resourcePath);
                if (in == null) {
                    throw new FileNotFoundException(resourcePath);
                }
                return in;
            }
        }
    }
}
