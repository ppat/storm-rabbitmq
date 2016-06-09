package io.latent.storm.rabbitmq;

import org.apache.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * This interface describes an object that will perform the work of mapping
 * incoming {@link Tuple}s to {@link Message} objects for posting on a RabbitMQ
 * exchange.
 *
 */
public abstract class TupleToMessage implements Serializable {
  protected void prepare(@SuppressWarnings("rawtypes") Map stormConfig) {}

  /**
   * Convert the incoming {@link Tuple} on the Storm stream to a {@link Message}
   * for posting to RabbitMQ.
   * 
   * @param input
   *          The incoming {@link Tuple} from Storm
   * @return The {@link Message} for the {@link RabbitMQProducer} to publish. If
   *          transformation fails this should return Message.NONE.
   */
  protected Message produceMessage(Tuple input) {
    return Message.forSending(
        extractBody(input),
        specifyHeaders(input),
        determineExchangeName(input),
        determineRoutingKey(input),
        specifyContentType(input),
        specifyContentEncoding(input),
        specifyMessagePersistence(input)
    );
  }

  /**
   * Extract message body as a byte array from the incoming tuple. This is required.
   * This implementation must handle errors and should return null upon on unresolvable
   * errors.
   *
   * @param input the incoming tuple
   * @return message body as a byte array or null if extraction cannot be performed
   */
  protected abstract byte[] extractBody(Tuple input);

  /**
   * Determine the exchange where the message is published to. This can be
   * derived based on the incoming tuple or a fixed value.
   *
   * @param input the incoming tuple
   * @return the exchange where the message is published to.
   */
  protected abstract String determineExchangeName(Tuple input);

  /**
   * Determine the routing key used for this message. This can be derived based on
   * the incoming tuple or a fixed value. Default implementation provides no
   * routing key.
   *
   * @param input the incoming tuple
   * @return the routing key for this message
   */
  protected String determineRoutingKey(Tuple input) {
    return ""; // rabbitmq java client library treats "" as no routing key
  }

  /**
   * Specify the headers to be sent along with this message. The default implementation
   * return an empty map.
   *
   * @param input the incoming tuple
   * @return the headers as a map
   */
  protected Map<String, Object> specifyHeaders(Tuple input)
  {
    return new HashMap<String, Object>();
  }

  /**
   * Specify message body content type. Default implementation skips the provision
   * of this detail.
   *
   * @param input the incoming tuple
   * @return content type
   */
  protected String specifyContentType(Tuple input) {
    return null;
  }

  /**
   * Specify message body content encoding. Default implementation skips the provision
   * of this detail.
   *
   * @param input the incoming tuple
   * @return content encoding
   */
  protected String specifyContentEncoding(Tuple input) {
    return null;
  }

  /**
   * Specify whether each individual message should make use of message persistence
   * when it's on a rabbitmq queue. This does imply queue durability or high availability
   * or just avoidance of message loss. To accomplish that please read rabbitmq docs
   * on High Availability, Publisher Confirms and Queue Durability in addition to
   * having this return true. By default, message persistence returns false.
   *
   * @param input the incoming tuple
   * @return whether the message should be persistent to disk or not. Defaults to not.
   */
  protected boolean specifyMessagePersistence(Tuple input) {
    return false;
  }
}
