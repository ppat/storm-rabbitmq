package io.latent.storm.rabbitmq;

import java.io.Serializable;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

/**
 * This interface describes an object that will perform the work of mapping
 * incoming {@link Tuple}s to {@link Message} objects for posting on a RabbitMQ
 * exchange.
 * 
 */
public interface TupleToMessage extends Serializable {

  /**
   * This method will be called to initialize this 
   * {@link TupleToMessage} object.
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

}
