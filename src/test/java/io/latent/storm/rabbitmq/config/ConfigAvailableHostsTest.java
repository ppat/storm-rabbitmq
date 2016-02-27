package io.latent.storm.rabbitmq.config;

import com.rabbitmq.client.Address;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Created by justinh on 2/22/16.
 */
public class ConfigAvailableHostsTest {

    @Test
    public void fromString() throws Exception {
        final ConfigAvailableHosts hosts = ConfigAvailableHosts.fromString("123.123.123.123|456.456.456.456");
        assertFalse(hosts.isEmpty());
        assertEquals(hosts.toAddresses().length, 2);
        assertEquals(hosts.toAddresses()[0], new Address("123.123.123.123"));
        assertEquals(hosts.toAddresses()[1], new Address("456.456.456.456"));

    }
}