package io.latent.storm.rabbitmq;

import backtype.storm.spout.Scheme;
import backtype.storm.task.TopologyContext;

import java.util.Map;

public interface MessageScheme extends Scheme
{
  void open(Map config, TopologyContext context);

  void close();
}
