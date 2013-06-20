package io.latent.storm.rabbitmq;

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

  public static Message forSending(byte[] body) {
    return (body != null) ? new Message(body) : NONE;
  }

  public byte[] getBody() {
    return body;
  }


  public static class DeliveredMessage extends Message {
    private final boolean redelivery;
    private final long deliveryTag;

    private DeliveredMessage(QueueingConsumer.Delivery delivery) {
      super(delivery.getBody());
      redelivery = delivery.getEnvelope().isRedeliver();
      deliveryTag = delivery.getEnvelope().getDeliveryTag();
    }

    public boolean isRedelivery() { return redelivery; }

    public long getDeliveryTag() { return deliveryTag; }
  }

  public static class None extends Message {
    private None() {
      super(null);
    }

    @Override
    public byte[] getBody() { throw new UnsupportedOperationException(); };
  }
}
