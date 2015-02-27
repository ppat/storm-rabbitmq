package io.latent.storm.rabbitmq;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.storm.guava.cache.Cache;
import org.apache.storm.guava.cache.CacheBuilder;
import org.apache.storm.guava.cache.RemovalCause;
import org.apache.storm.guava.cache.RemovalListener;
import org.apache.storm.guava.cache.RemovalNotification;
import org.apache.storm.guava.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * Simplest spout for trident topologies. Ensures no message is lost, but are not processed just once
 * 
 * @author frascuchon@gmail.com
 *
 */
@SuppressWarnings("serial")
public class RabbitMQBatchSpout implements IBatchSpout {

	private static final int DEFAULT_MAX_SIZE = 5;

	private final Logger _LOGGER = LoggerFactory.getLogger(RabbitMQBatchSpout.class);

	private static final int DEFAULT_MAX_BATCH_SIZE = 100;

	private RabbitMQConsumer _consumer;

	private Cache<Long, List<Message>> _inProgressBatches;

	private final Declarator declarator;

	private final int maxBatchSize;

	private final List<Object> _emptyBatch = Lists.newArrayList();

	public RabbitMQBatchSpout() {
		this(new Declarator.NoOp(), DEFAULT_MAX_BATCH_SIZE);
	}

	public RabbitMQBatchSpout(Declarator declarator) {
		this(declarator, DEFAULT_MAX_BATCH_SIZE);
	}

	public RabbitMQBatchSpout(Declarator declarator, int maxBatchSize) {
		this.declarator = declarator;
		this.maxBatchSize = maxBatchSize;
	}

	@Override
	public void open(Map conf, TopologyContext context) {
		_inProgressBatches = batchCache(conf);

		_consumer = RabbitMQUtils.loadConsumer(declarator, ErrorReporter.defaultReporter, conf);
		_consumer.open();
	}

	private Cache<Long, List<Message>> batchCache(Map conf) {
		return CacheBuilder.newBuilder().expireAfterWrite(Utils.getInt(conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)), TimeUnit.SECONDS)
				.removalListener(new RemovalListener<Long, List<Message>>() {
					@Override
					public void onRemoval(RemovalNotification<Long, List<Message>> paramRemovalNotification) {
						if (RemovalCause.EXPIRED.equals(paramRemovalNotification.getCause())) {
							final List<Message> messages = paramRemovalNotification.getValue();
							
							for (final Message message : messages) {
								_consumer.failWithRedelivery(RabbitMQUtils.deliveryTagForMessage(message));
							}
						}
					}
				}).maximumSize(Utils.getInt(conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING), DEFAULT_MAX_SIZE)).build();
	}

	@Override
	public void emitBatch(long batchId, TridentCollector collector) {

		final List<Message> currentBatch = Lists.newArrayList();

		for (int i = 0; i < maxBatchSize; i++) {
			final Message message = _consumer.nextMessage();
			if (!Message.NONE.equals(message)) {
				currentBatch.add(message);
				collector.emit(new Values(message.getBody(), RabbitMQUtils.headersForMessage(message)));
			}
		}
		if (currentBatch.isEmpty()) {
			_emptyBatch .add(batchId);
		} else {
			_inProgressBatches.put(batchId, currentBatch);
		}
	}

	@Override
	public void ack(long batchId) {
		
		if (isEmptyBatch(batchId)) {
			return;
		}
		
		final List<Message> ackBatch = _inProgressBatches.getIfPresent(batchId);
		if (ackBatch == null) {
			_LOGGER.warn(String.format("Cannot acknoledge batch %s", batchId));
		}
		else {
			commitBatch(ackBatch);
		}
	}

	private boolean isEmptyBatch(long batchId) {
		return _emptyBatch.remove(batchId);
	}

	private void commitBatch(List<Message> ackBatch) {
		for (final Message message : ackBatch) {
			_consumer.ack(RabbitMQUtils.deliveryTagForMessage(message));
		}
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("body", "headers");
	}

	@Override
	public void close() {
		rejectAllPendingBatches();
		_consumer.close();
	}

	private void rejectAllPendingBatches() {
		for (final List<Message> messages : _inProgressBatches.asMap().values()) {
			for (final Message message : messages) {
				_consumer.failWithRedelivery(RabbitMQUtils.deliveryTagForMessage(message));
			}
		}
	}

	@Override
	public Map getComponentConfiguration() {
		return null;
	}

}
