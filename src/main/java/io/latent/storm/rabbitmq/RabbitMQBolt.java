package io.latent.storm.rabbitmq;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.Bolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * This is a simple {@link Bolt} for producing messages to RabbitMQ from a Storm
 * topology. It needs a {@link TupleToMessage} object to perform the real meat
 * of converting the incoming {@link Tuple} from a stream into a {@link Message}
 * to publish on RabbitMQ.
 * 
 * @author bdgould
 *
 */
public class RabbitMQBolt extends BaseRichBolt {

  /**
   * Serial version UID.
   */
  private static final long serialVersionUID = 1L;

  private final TupleToMessage scheme;
  private final Declarator declarator;
  private final String routingKey;

  private transient Logger logger;
  private transient RabbitMQProducer producer;
  private transient OutputCollector collector;

  public RabbitMQBolt(final TupleToMessage scheme) {
    this(scheme, new Declarator.NoOp(), "");
  }

  public RabbitMQBolt(final TupleToMessage scheme, final String routingKey) {
    this(scheme, new Declarator.NoOp(), routingKey);
  }

  public RabbitMQBolt(final TupleToMessage scheme, final Declarator declarator) {
    this(scheme, declarator, "");
  }

  public RabbitMQBolt(final TupleToMessage scheme, final Declarator declarator, final String routingKey) {
    this.scheme = scheme;
    this.declarator = declarator;
    this.routingKey = routingKey;
  }

  @Override
  public void prepare(@SuppressWarnings("rawtypes") final Map stormConf, final TopologyContext context, final OutputCollector collector) {
    producer = new RabbitMQProducer(declarator);
    producer.open(stormConf);
    logger = LoggerFactory.getLogger(RabbitMQProducer.class);
    this.collector = collector;
    logger.info("Successfully prepared RabbitMQBolt");
  }

  @Override
  public void execute(final Tuple input) {
    producer.send(scheme.produceMessage(input), routingKey);
    scheme.emitTuples(collector, input);
    collector.ack(input);
  }

  @Override
  public void declareOutputFields(final OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(scheme.getOutputFields()));
  }

}
