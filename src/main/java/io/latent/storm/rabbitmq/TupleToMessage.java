package io.latent.storm.rabbitmq;

import java.io.Serializable;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

/**
 * This interface describes an object that will perform the work of mapping
 * incoming {@link Tuple}s to {@link Message} objects for posting on a RabbitMQ
 * exchange. This object can also massage the incoming {@link Tuple} to emit new
 * values for further processing on the stream. It will do so only if the
 * isDrain() method returns false, as in this implementation is NOT simply a
 * drain on the stream).
 * 
 */
public interface TupleToMessage extends Serializable {

  public static final String[] NO_FIELDS = new String[] {};

  /**
   * This method will be called to initialize
   */
  void prepare(@SuppressWarnings("rawtypes") Map stormConfig, OutputCollector collector);

  /**
   * Convert the incoming {@link Tuple} on the Storm stream to a {@link Message}
   * for posting to RabbitMQ.
   * 
   * @param input
   *          The incoming {@link Tuple} from Storm
   * @return The {@link Message} for the {@link RabbitMQProducer} to publish
   */
  Message produceMessage(Tuple input);

  /**
   * If this {@link TupleToMessage} is NOT a drain, then it should implement
   * this method to publish what fields will be going out in the {@link Tuple} s
   * it emits.
   * 
   * @return The array of {@link String}s that are the ordered name of fields in
   *         {@link Tuple}s emitted by the wrapping bolt
   */
  String[] getOutputFields();

}
