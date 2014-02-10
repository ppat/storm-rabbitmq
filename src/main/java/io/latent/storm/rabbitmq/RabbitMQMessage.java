package io.latent.storm.rabbitmq;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import backtype.storm.task.TopologyContext;


public class RabbitMQMessage implements MessageScheme {
  private final Scheme payloadScheme;
  private final Fields fields;

  public RabbitMQMessage(Scheme payloadScheme, Fields fields) {
  	this.payloadScheme = payloadScheme;
  	this.fields = fields;
  }

  @Override
  public void open(Map config, TopologyContext context) {
  }

  @Override
  public void close() {
  }

  @Override 
  public List<Object> deserialize(Message message) {
  	Message.DeliveredMessage dm = (Message.DeliveredMessage)message;
    Envelope envelope = new Envelope(dm.isRedelivery(), dm.getDeliveryTag(), dm.getReceivedExchange(), dm.getReceivedRoutingKey());

    Properties properties = new Properties(dm.getTimestamp());

    List<Object> payload = deserialize(message.getBody());

    List<Object> results = new ArrayList();
    results.add(envelope);
    results.add(properties);
    results.add(payload);

    return results;
  }

  @Override
  public List<Object> deserialize(byte[] payload) {
  	return payloadScheme.deserialize(payload);
  }

  @Override
  public Fields getOutputFields() {
    return new Fields("envelope", "properties", "payload");
  }

  public static class Envelope {
    private final boolean isRedelivery;
    private final long deliveryTag;
    private final String exchange;
    private final String routingKey;

    Envelope(boolean isRedelivery, long deliveryTag, String exchange, String routingKey) {
      this.isRedelivery = isRedelivery;
      this.deliveryTag = deliveryTag;
      this.exchange = exchange;
      this.routingKey = routingKey;
    }

    boolean isRedelivery() { return isRedelivery; }
    long getDeliveryTag() { return deliveryTag; }
    String getExchange() { return exchange; }
    String getRoutingKey() { return routingKey; }
  }

  public static class Properties {
    private final Date timestamp;

    Properties(Date timestamp) {
      this.timestamp = timestamp;
    }

    Date getTimestamp() { return timestamp; }
  }
}