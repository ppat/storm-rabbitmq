package io.latent.storm.rabbitmq;

import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;

import java.util.List;

public class MultiStreamSpout extends RabbitMQSpout {
  private final MultiStreamCoordinator streamCoordinator;
  private final Scheme scheme;

  public MultiStreamSpout(Scheme scheme,
                          MultiStreamCoordinator streamCoordinator)
  {
    super(scheme);
    this.scheme = scheme;
    this.streamCoordinator = streamCoordinator;
  }

  public MultiStreamSpout(MessageScheme scheme,
                          MultiStreamCoordinator streamCoordinator) {
    this((Scheme) scheme, streamCoordinator);
  }

  public MultiStreamSpout(Scheme scheme,
                          MultiStreamCoordinator streamCoordinator,
                          Declarator declarator) {
    super(scheme, declarator);
    this.scheme = scheme;
    this.streamCoordinator = streamCoordinator;
  }

  public MultiStreamSpout(MessageScheme scheme,
                          Declarator declarator,
                          MultiStreamCoordinator streamCoordinator) {
    this((Scheme) scheme, streamCoordinator, declarator);
  }

  @Override
  protected List<Integer> emit(List<Object> tuple,
                               Message message,
                               SpoutOutputCollector spoutOutputCollector) {
    String stream = streamCoordinator.selectStream(tuple, message);
    return spoutOutputCollector.emit(stream, tuple, getDeliveryTag(message));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    for (String stream : streamCoordinator.streamNames()) {
      outputFieldsDeclarer.declareStream(stream, scheme.getOutputFields());
    }
  }
}
