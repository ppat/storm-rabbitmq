package io.latent.storm.rabbitmq;

import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;

import java.util.List;

/**
 * MultiStreamSpout will emit tuples on multiple streams by assigning tuples to a stream using the provided
 * MultiStreamSplitter.
 *
 * @author peter@latent.io
 */
public class MultiStreamSpout extends RabbitMQSpout {
  private final MultiStreamSplitter streamSplitter;
  private final Scheme scheme;

  public MultiStreamSpout(Scheme scheme,
                          MultiStreamSplitter streamSplitter) {
    super(scheme);
    this.scheme = scheme;
    this.streamSplitter = streamSplitter;
  }

  public MultiStreamSpout(MessageScheme scheme,
                          MultiStreamSplitter streamSplitter) {
    this((Scheme) scheme, streamSplitter);
  }

  public MultiStreamSpout(Scheme scheme,
                          MultiStreamSplitter streamSplitter,
                          Declarator declarator) {
    super(scheme, declarator);
    this.scheme = scheme;
    this.streamSplitter = streamSplitter;
  }

  public MultiStreamSpout(MessageScheme scheme,
                          Declarator declarator,
                          MultiStreamSplitter streamSplitter) {
    this((Scheme) scheme, streamSplitter, declarator);
  }

  @Override
  protected List<Integer> emit(List<Object> tuple,
                               Message message,
                               SpoutOutputCollector spoutOutputCollector) {
    String stream = streamSplitter.selectStream(tuple, message);
    return spoutOutputCollector.emit(stream, tuple, getDeliveryTag(message));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    for (String stream : streamSplitter.streamNames()) {
      outputFieldsDeclarer.declareStream(stream, scheme.getOutputFields());
    }
  }
}
