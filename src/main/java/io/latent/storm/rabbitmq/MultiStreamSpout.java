package io.latent.storm.rabbitmq;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;

import java.util.List;

public class MultiStreamSpout extends RabbitMQSpout {
  private final MultiStreamCoordinator streamCoordinator;
  private final MessageScheme scheme;

  public MultiStreamSpout(String configKey,
                          MessageScheme scheme,
                          MultiStreamCoordinator streamCoordinator)
  {
    super(configKey, scheme);
    this.scheme = scheme;
    this.streamCoordinator = streamCoordinator;
  }

  public MultiStreamSpout(String configKey,
                          MessageScheme scheme,
                          MultiStreamCoordinator streamCoordinator,
                          Declarator declarator) {
    super(configKey, scheme, declarator);
    this.scheme = scheme;
    this.streamCoordinator = streamCoordinator;
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
