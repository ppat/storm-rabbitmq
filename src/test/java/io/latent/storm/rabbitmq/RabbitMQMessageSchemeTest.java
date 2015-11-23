package io.latent.storm.rabbitmq;

import backtype.storm.spout.RawScheme;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.LongString;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.impl.LongStringHelper;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;


public class RabbitMQMessageSchemeTest {

    @Test
    public void testDeserializeLongString() throws Exception {
        RabbitMQMessageScheme scheme = new RabbitMQMessageScheme(new RawScheme(), "env", "props");

        Envelope envelope = new Envelope(1, false, "test_exchange", "route1");

        HashMap<String, Object> headers = new HashMap<String, Object>();
        headers.put("long_string_header", LongStringHelper.asLongString("test string long string"));

        AMQP.BasicProperties properties = new AMQP.BasicProperties().builder().headers(headers).build();
        QueueingConsumer.Delivery delivery = new QueueingConsumer.Delivery(envelope, properties, new byte[]{});
        Message message = new Message.DeliveredMessage(delivery);

        List<Object> tuple = scheme.deserialize(message);
        RabbitMQMessageScheme.Properties latentProperties = (RabbitMQMessageScheme.Properties) tuple.get(2);
        latentProperties.getHeaders();

        assertEquals("test string long string", latentProperties.getHeaders().get("long_string_header"));
    }
}