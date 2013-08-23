package io.latent.storm.rabbitmq;

import java.io.Serializable;
import java.util.List;

/**
 * Used for splitting a stream into multiple streams by examining the each individual tuple/message
 *
 * @author peter@latent.io
 */
public interface MultiStreamSplitter extends Serializable {
  /**
   * @return a list of streams a tuple may be assigned to from this stream splitter
   */
  List<String> streamNames();

  /**
   * Given the tuple (the de-serialized message contents) and the message in its original serialized format, select
   * the stream this particular tuple should be emitted under.
   *
   * @param tuple the de-serialized form of message as a list of objects (a tuple)
   * @param message the original RabbitMQ message before it was de-serialized (incl. amqp envelope information)
   * @return the stream assigned to the tuple
   */
  String selectStream(List<Object> tuple,
                      Message message);
}
