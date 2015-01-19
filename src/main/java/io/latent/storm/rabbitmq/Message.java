package io.latent.storm.rabbitmq;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.rabbitmq.client.QueueingConsumer;

public class Message {
  public static final Message NONE = new None();

  private final byte[] body;

  public Message(byte[] body) {
    this.body = body;
  }

  public static Message forDelivery(QueueingConsumer.Delivery delivery) {
    return (delivery != null) ? new DeliveredMessage(delivery) : NONE;
  }

  public static Message forSending(byte[] body,
                                   Map<String, Object> headers,
                                   String exchangeName,
                                   String routingKey,
                                   String contentType,
                                   String contentEncoding,
                                   boolean persistent) {
    return (body != null && exchangeName != null && exchangeName.length() > 0) ?
        new MessageForSending(body, headers, exchangeName, routingKey, contentType, contentEncoding, persistent) :
        NONE;
  }

  public byte[] getBody() {
    return body;
  }

  public static class DeliveredMessage extends Message {
    private final boolean redelivery;
    private final long deliveryTag;
    private final String routingKey;
    private final String exchange;
    private final String className;
    private final String clusterId;
    private final String contentEncoding;
    private final String contentType;
    private final String correlationId;
    private final Integer deliveryMode;
    private final String expiration;
    private final Map<String, Object> headers;
    private final String messageId;
    private final Integer priority;
    private final String replyTo;
    private final Date timestamp;
    private final String type;
    private final String userId;

    private DeliveredMessage(QueueingConsumer.Delivery delivery) {
      super(delivery.getBody());
      redelivery = delivery.getEnvelope().isRedeliver();
      deliveryTag = delivery.getEnvelope().getDeliveryTag();
      routingKey = delivery.getEnvelope().getRoutingKey();
      exchange = delivery.getEnvelope().getExchange();
      className = delivery.getProperties().getClassName();
      clusterId = delivery.getProperties().getClusterId();
      contentEncoding = delivery.getProperties().getContentEncoding();
      contentType = delivery.getProperties().getContentType();
      correlationId = delivery.getProperties().getCorrelationId();
      deliveryMode = delivery.getProperties().getDeliveryMode();
      expiration = delivery.getProperties().getExpiration();
      headers = delivery.getProperties().getHeaders();
      messageId = delivery.getProperties().getMessageId();
      priority = delivery.getProperties().getPriority();
      replyTo = delivery.getProperties().getReplyTo();
      timestamp = delivery.getProperties().getTimestamp();
      type = delivery.getProperties().getType();
      userId = delivery.getProperties().getUserId();
    }

    public boolean isRedelivery() { return redelivery; }
    public long getDeliveryTag() { return deliveryTag; }
    public String getRoutingKey() { return routingKey; }
    public String getExchange() { return exchange; }
    public String getClassName() { return className;}
    public String getClusterId(){ return clusterId; }
    public String getContentEncoding() { return contentEncoding; }
    public String getContentType() { return contentType; }
    public String getCorrelationId() { return correlationId; }
    public Integer getDeliveryMode() { return deliveryMode; }
    public String getExpiration() { return expiration; }
    public Map<String, Object> getHeaders() { return headers; }
    public String getMessageId() { return messageId; }
    public Integer getPriority() { return priority; }
    public String getReplyTo() { return replyTo; }
    public Date getTimestamp() { return timestamp; }
    public String getType() { return type; }
    public String getUserId() { return userId; }
  }

  public static class None extends Message {
    private None() {
      super(null);
    }

    @Override
    public byte[] getBody() { throw new UnsupportedOperationException(); };
  }

  public static class MessageForSending extends Message {
    private final Map<String, Object> headers;
    private final String exchangeName;
    private final String routingKey;
    private final String contentType;
    private final String contentEncoding;
    private final boolean persistent;

    private MessageForSending(byte[] body,
                              Map<String, Object> headers,
                              String exchangeName,
                              String routingKey,
                              String contentType,
                              String contentEncoding,
                              boolean persistent) {
      super(body);
      this.headers = (headers != null) ? headers : new HashMap<String, Object>();
      this.exchangeName = exchangeName;
      this.routingKey = routingKey;
      this.contentType = contentType;
      this.contentEncoding = contentEncoding;
      this.persistent = persistent;
    }

    public Map<String, Object> getHeaders()
    {
      return headers;
    }

    public String getExchangeName()
    {
      return exchangeName;
    }

    public String getRoutingKey()
    {
      return routingKey;
    }

    public String getContentType()
    {
      return contentType;
    }

    public String getContentEncoding()
    {
      return contentEncoding;
    }

    public boolean isPersistent()
    {
      return persistent;
    }
  }
}
