package io.latent.storm.rabbitmq;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * This is a simple bolt for producing messages to RabbitMQ from a Storm
 * topology. It needs a {@link TupleToMessage} object to perform the real meat
 * of converting the incoming {@link Tuple} from a stream into a {@link Message}
 * to publish on RabbitMQ.
 * 
 * @author bdgould
 *
 */
public class RabbitMQBolt extends BaseRichBolt {
  private static final long serialVersionUID = 97236452008970L;

  private final TupleToMessage scheme;
  private final Declarator declarator;

  private transient Logger logger;
  private transient RabbitMQProducer producer;
  private transient OutputCollector collector;

  public RabbitMQBolt(final TupleToMessage scheme) {
    this(scheme, new Declarator.NoOp());
  }

  public RabbitMQBolt(final TupleToMessage scheme, final Declarator declarator) {
    this.scheme = scheme;
    this.declarator = declarator;
  }

  @Override
  public void prepare(@SuppressWarnings("rawtypes") final Map stormConf, final TopologyContext context, final OutputCollector collector) {
    producer = new RabbitMQProducer(declarator);
    producer.open(stormConf);
    logger = LoggerFactory.getLogger(this.getClass());
    this.collector = collector;
    this.scheme.prepare(stormConf);
    logger.info("Successfully prepared RabbitMQBolt");
  }

  @Override
  public void execute(final Tuple tuple) {
      publish(tuple);
      // tuples are always acked, even when transformation by scheme yields Message.NONE as
    // if it failed once it's unlikely to succeed when re-attempted (i.e. serialization/deserilization errors).
      acknowledge(tuple);
  }

    protected void acknowledge(Tuple tuple) {
        collector.ack(tuple);
    }

    protected void publish(Tuple tuple) {
        producer.send(scheme.produceMessage(tuple));
    }

    @Override
  public void declareOutputFields(final OutputFieldsDeclarer declarer) {
    //No fields are emitted from this drain.
  }
}
