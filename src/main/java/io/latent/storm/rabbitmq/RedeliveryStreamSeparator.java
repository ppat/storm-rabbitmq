package io.latent.storm.rabbitmq;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Separates initial (first time) deliveries off of a RabbitMQ queue from redeliveries (messages that are being
 * processed after initial processing failed for some reason).
 *
 * @author peter@latent.io
 */
public class RedeliveryStreamSeparator implements MultiStreamSplitter {
  public static final String INITIAL_DELIVERY_STREAM = "initial_delivery";
  public static final String REDELIVERY_STREAM = "redelivery";

  private static final List<String> streams = Collections.unmodifiableList(Arrays.asList(INITIAL_DELIVERY_STREAM,
                                                                                         REDELIVERY_STREAM));

  @Override
  public List<String> streamNames() {
    return streams;
  }

  @Override
  public String selectStream(List<Object> tuple,
                             Message message) {
    Message.DeliveredMessage deliveredMessage = (Message.DeliveredMessage) message;
    return deliveredMessage.isRedelivery() ? REDELIVERY_STREAM : INITIAL_DELIVERY_STREAM;
  }
}
