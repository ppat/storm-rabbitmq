package ru.kiryam.storm.rabbitmq;

import backtype.storm.spout.RawScheme;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.impl.LongStringHelper;
import org.junit.Test;
import java.util.ArrayList;
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

        assertEquals("test string long string", latentProperties.getHeaders().get("long_string_header"));
    }

    @Test
    public void testDeserializeArrayList() throws Exception{
        RabbitMQMessageScheme scheme = new RabbitMQMessageScheme(new RawScheme(), "env", "props");
        Envelope envelope = new Envelope(1, false, "test_exchange", "route1");
        HashMap<String, Object> headers = new HashMap<String, Object>();

        ArrayList<HashMap<String, Object>> arrayList = new ArrayList<HashMap<String, Object>>();

        HashMap<String, Object> hashMap = new HashMap<String, Object>();
        hashMap.put("count", 1);

        arrayList.add(hashMap);

        headers.put("x-death", arrayList);

        AMQP.BasicProperties properties = new AMQP.BasicProperties().builder().headers(headers).build();
        QueueingConsumer.Delivery delivery = new QueueingConsumer.Delivery(envelope, properties, new byte[]{});
        Message message = new Message.DeliveredMessage(delivery);

        List<Object> tuple = scheme.deserialize(message);
        RabbitMQMessageScheme.Properties latentProperties = (RabbitMQMessageScheme.Properties) tuple.get(2);

        assertEquals(1, ((HashMap) ((ArrayList) latentProperties.getHeaders().get("x-death")).get(0)).get("count"));
    }
}