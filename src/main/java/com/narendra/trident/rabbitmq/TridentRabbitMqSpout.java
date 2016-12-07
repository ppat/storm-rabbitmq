package com.narendra.trident.rabbitmq;

import java.util.Map;

import backtype.storm.spout.Scheme;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import io.latent.storm.rabbitmq.Declarator;
import io.latent.storm.rabbitmq.MessageScheme;
import storm.trident.spout.ITridentSpout;

@SuppressWarnings("rawtypes")
public class TridentRabbitMqSpout implements ITridentSpout<RabbitMQBatch> {

  public static final String MAX_BATCH_SIZE_CONF = "topology.spout.max.batch.size";
  public static final int DEFAULT_BATCH_SIZE = 1000;
  private static final long serialVersionUID = 1L;

  private String name;
  private static int nameIndex = 1;
  private String streamId;
  private Map consumerMap;
  private Declarator declarator;
  private MessageScheme scheme;

  public TridentRabbitMqSpout(Scheme scheme, String streamId,Map consumerMap){
    this(MessageScheme.Builder.from(scheme), null,new Declarator.NoOp(), streamId,consumerMap);
  }

  public TridentRabbitMqSpout(MessageScheme scheme, final TopologyContext context,
      Declarator declarator, String streamId, Map consumerMap) {
    this.scheme = scheme;
    this.declarator = declarator;
    this.streamId = streamId;
    this.consumerMap = consumerMap;
    this.name = "RabbitMQSpout" + (nameIndex++);
  }

  @Override
  public Map getComponentConfiguration() {
    return consumerMap;
  }

  @Override
  public storm.trident.spout.ITridentSpout.BatchCoordinator<RabbitMQBatch> getCoordinator(String txStateId,
      Map conf, TopologyContext context) {
    return new RabbitMQBatchCoordinator(name);
  }

  @Override
  public storm.trident.spout.ITridentSpout.Emitter<RabbitMQBatch> getEmitter(String txStateId,
  Map conf, TopologyContext context) {
    return new RabbitMQTridentEmitter(scheme, context, declarator, streamId, consumerMap);
  }

  @Override
  public Fields getOutputFields() {
    return this.scheme.getOutputFields();
  }

}
