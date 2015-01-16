package io.latent.storm.rabbitmq;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.Bolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
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

  /**
   * Construct a new {@link RabbitMQBolt} using the default routing key ("")
   * and the provided {@link TupleToMessage}.
   * 
   * @param scheme Object that will serialize incoming 
   *    {@link Tuple}s into {@link Message} objects
   */
  public RabbitMQBolt(final TupleToMessage scheme) {
    this(scheme, new Declarator.NoOp(), "");
  }

  /**
   * Construct a new {@link RabbitMQBolt}.
   * 
   * @param scheme The {@link TupleToMessage} to convert {@link Tuple}s into {@link Message}
   *    objects
   * @param routingKey The routing key to use when publishing to an exchange
   */
  public RabbitMQBolt(final TupleToMessage scheme, final String routingKey) {
    this(scheme, new Declarator.NoOp(), routingKey);
  }

  /**
   * Construct a new {@link RabbitMQBolt} using the default routing key ("").
   * 
   * @param scheme The {@link TupleToMessage} to convert {@link Tuple}s into {@link Message} 
   *    objects
   * @param declarator The {@link Declarator} to initialize the exchange you're publishing
   *    {@link Message}s to
   */
  public RabbitMQBolt(final TupleToMessage scheme, final Declarator declarator) {
    this(scheme, declarator, "");
  }

  /**
   * Construct a new {@link RabbitMQBolt}.
   * 
   * @param scheme The {@link TupleToMessage} to convert {@link Tuple}s into {@link Message} 
   *    objects
   * @param declaratorThe {@link Declarator} to initialize the exchange you're publishing
   *    {@link Message}s to
   * @param routingKey The routing key to use when posting messages to the configured exchange
   */
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
    this.scheme.prepare(stormConf, collector);
    logger.info("Successfully prepared RabbitMQBolt");
  }

  @Override
  public void execute(final Tuple input) {
    producer.send(scheme.produceMessage(input), routingKey);
    collector.ack(input);
  }

  @Override
  public void declareOutputFields(final OutputFieldsDeclarer declarer) {
    //No fields are emitted from this drain.
  }

}
