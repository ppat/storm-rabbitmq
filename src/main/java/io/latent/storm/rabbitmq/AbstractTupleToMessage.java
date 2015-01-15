package io.latent.storm.rabbitmq;

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
   * {@inheritDoc}
   */
  @Override
  public boolean isDrain() {
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void emitTuples(OutputCollector collector, Tuple input) {
    throw new IllegalArgumentException("Implementations of this abstract class must override this method.");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String[] getOutputFields() {
    return new String[] {};
  }

}
