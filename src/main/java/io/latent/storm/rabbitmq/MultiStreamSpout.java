package io.latent.storm.rabbitmq;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;

import java.util.List;

public class MultiStreamSpout extends RabbitMQSpout {
  private final MultiStreamCoordinator streamCoordinator;

  public MultiStreamSpout(String configKey,
                          MultiStreamCoordinator streamCoordinator) {
    super(configKey);
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
      outputFieldsDeclarer.declareStream(stream, consumerConfig.getScheme().getOutputFields());
    }
  }
}
