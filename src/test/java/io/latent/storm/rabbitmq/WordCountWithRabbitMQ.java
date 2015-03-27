package io.latent.storm.rabbitmq;


import java.util.Random;

import org.apache.commons.lang.StringUtils;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.fluent.GroupedStream;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.MapGet;
import storm.trident.spout.IBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class WordCountWithRabbitMQ {

	private static final String CONTENT_ENCODING = "UTF-8";
	private static final String CONTENT_TYPE = "text/plain";
	private static final String QUEUE_NAME = "testQueue";
	private static final int BATCH_SIZE = 100;

	
	static class SendMessagesToRabbitMQ implements Runnable {
		
		private static final int MESSAGES_BATCH = 1000;

		private static final String ROUTING_KEY = "test";

		private static final String AMQ_DIRECT = "amq.direct";

		static final String [] SENTENCES = {"this is my fucking car", "you ! mother fucker cacarrocha", "i'll see you in hell, bastard!"};
		
		private final RabbitMQProducer producer;

		private final int messagesBatch;

		private boolean _running = true;
		
		public void stop() {
			_running = false;
		}

		public SendMessagesToRabbitMQ(RabbitMQProducer producer) {
			this(producer, MESSAGES_BATCH);
		}
		
		public SendMessagesToRabbitMQ(RabbitMQProducer producer, int messagesBatch) {
			this.producer = producer;
			this.messagesBatch = messagesBatch;
		}
		
		@Override
		public void run() {
			while(_running) {
				final long start = System.currentTimeMillis();
				for (int i = 0; i < messagesBatch; i++) {
					producer.send(generateMessage());
				}
				final long end = System.currentTimeMillis();
				Utils.sleep(Math.abs(1000 - (end- start)));
			}
			
			producer.close();
		}

		private Message generateMessage() {
			return Message.forSending(nextRandomSentence().getBytes(), null, AMQ_DIRECT, ROUTING_KEY, CONTENT_TYPE, CONTENT_ENCODING, false);
		}

		private String nextRandomSentence() {
			final int i = Math.abs(new Random().nextInt() % SENTENCES.length);
			return SENTENCES[i];
		}
	}
	

	public static void main(String[] args) {

		final LocalDRPC drcp = new LocalDRPC();
		final LocalCluster cluster = new LocalCluster();
		final Config config = stormConfiguration();
		
		final SendMessagesToRabbitMQ messagesSender = startSendingMessages(config);
		
		cluster.submitTopology("test", config, wordCountWithRabbitMQSpout(drcp));

		final long start = System.currentTimeMillis();
		do {
			System.out.println(drcp.execute("words-count", "abrigo mother fucking"));
			Utils.sleep(10000);
		} while (!timeExpired(start));
		
		
		messagesSender.stop();
		cluster.shutdown();
		drcp.shutdown();
	}

	private static SendMessagesToRabbitMQ startSendingMessages(final Config config) {
		final RabbitMQProducer producer = new RabbitMQProducer();
		producer.open(config);

		final SendMessagesToRabbitMQ messagesSender = new SendMessagesToRabbitMQ(producer);
		final Thread sendMessages = new Thread(messagesSender); 
		
		sendMessages.start();
		
		return messagesSender;
	}

	private static Config stormConfiguration() {
		final Config config = new Config();
		config.put("rabbitmq.autoRecovery", true);
		config.put("rabbitmq.queueName", QUEUE_NAME);
		config.put("rabbitmq.prefetchCount", 2*BATCH_SIZE);
		config.put("rabbitmq.exchangeName", StringUtils.EMPTY);
		config.put("rabbitmq.routingKey", QUEUE_NAME);
		config.put("rabbitmq.contentType", CONTENT_TYPE);
		config.put("rabbitmq.contentEncoding", CONTENT_ENCODING);		
		config.put("rabbitmq.persistent", false);
		return config;
	}

	private static boolean timeExpired(long start) {
		final long now = System.currentTimeMillis();
		
		return now - start > 30000;
	}
	
	@SuppressWarnings("serial")
	static class MessageDeserializer extends BaseFunction {
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			final byte[] body = tuple.getBinary(0);
			collector.emit(new Values(new String(body)));
		}
	}

	public static StormTopology wordCountWithRabbitMQSpout(LocalDRPC drcp) {
		final TridentTopology topology = new TridentTopology();
		
		final GroupedStream stream = topology.newStream("words", rabbitmqSpout())
			.name("incoming-rabbitMQ-message")
			.each(new Fields("body"), new MessageDeserializer(), new Fields("message"))
			.name("deserialized-message")
			.project(new Fields("message"))
			.each(new Fields("message"), new Split(), new Fields("word"))
			.name("split-message-in-word")
			.project(new Fields("word"))
			.groupBy(new Fields("word"));
		
		final TridentState wordCountState = stream
			.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("word-count"));
		
//		stream.toStream().each(new Fields("word"), new Debug());
		
		topology.newDRPCStream("words-count", drcp)
			.name("drpc-words-count")
			.each(new Fields("args"), new Split(), new Fields("query-word"))
			.project(new Fields("query-word"))
			.groupBy(new Fields("query-word"))
			.stateQuery(wordCountState, new Fields("query-word"), new MapGet(), new Fields("words-count"));
		
		return topology.build();
	}


	private static IBatchSpout rabbitmqSpout() {
		final RabbitMQBatchSpout spout = new RabbitMQBatchSpout(new Declarator.NoOp(), BATCH_SIZE);
		return spout;
	}

}
