package io.latent.storm.rabbitmq;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

/**
 * This abstract implementation of the {@link TupleToMessage} allows developers
 * to simply write the method converting the input {@link Tuple} to a
 * {@link Message} object for posting to RabbitMQ.
 *
 */
public abstract class AbstractTupleToMessage implements TupleToMessage {

  /**
   * Serial version UID.
   */
  private static final long serialVersionUID = 1L;

  /**
   * {@link OutputCollector} object for emitting {@link Tuple}s.
   */
  protected transient OutputCollector collector;

  /**
   * {@inheritDoc}
   */
  @Override
  public void prepare(@SuppressWarnings("rawtypes") Map stormConfig, OutputCollector collector) {
    this.collector = collector;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String[] getOutputFields() {
    return NO_FIELDS;
  }
}
