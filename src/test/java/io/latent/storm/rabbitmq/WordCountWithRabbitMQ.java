package io.latent.storm.rabbitmq;


import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.fluent.GroupedStream;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Debug;
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

	private static final int BATCH_SIZE = 5;


	public static void main(String[] args) {

		final LocalDRPC drcp = new LocalDRPC();
		final LocalCluster cluster = new LocalCluster();
		final Config config = new Config();

		config.put("rabbitmq.autoRecovery", true);
		config.put("rabbitmq.queueName", "testQueue");
		config.put("rabbitmq.prefetchCount", 2*BATCH_SIZE);
		
		cluster.submitTopology("test", config, wordCountWithRabbitMQSpout(drcp));

		final long start = System.currentTimeMillis();
		do {
			System.out.println(drcp.execute("words-count", "abrigo hermano"));
			Utils.sleep(10000);
		} while (!timeExpired(start));
		
		cluster.shutdown();
		drcp.shutdown();
	}

	private static boolean timeExpired(long start) {
		final long now = System.currentTimeMillis();
		
		return now - start > 30000;
	}
	
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
			.each(new Fields("body"), new MessageDeserializer(), new Fields("message"))
			.project(new Fields("message"))
			.each(new Fields("message"), new Split(), new Fields("word"))
			.project(new Fields("word"))
			.groupBy(new Fields("word"));
		
		final TridentState wordCountState = stream
			.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("word-count"));
		
		stream.toStream().each(new Fields("word"), new Debug());
		
		topology.newDRPCStream("words-count", drcp)
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
