package io.latent.storm.rabbitmq;

import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

/**
 * MultiStreamSpout will emit tuples on multiple streams by assigning tuples to
 * a stream using the provided MultiStreamSplitter.
 *
 * @author peter@latent.io
 */
public class MultiStreamSpout extends RabbitMQSpout {
	/**
	 * Serial version UID.
	 */
	private static final long serialVersionUID = 1L;

	private final MultiStreamSplitter streamSplitter;
	private final Fields outputFields;

	public MultiStreamSpout(Scheme scheme, MultiStreamSplitter streamSplitter) {
		super(scheme);
		this.outputFields = scheme.getOutputFields();
		this.streamSplitter = streamSplitter;
	}

	public MultiStreamSpout(Scheme scheme, MultiStreamSplitter streamSplitter, Declarator declarator) {
		super(scheme, declarator);
		this.outputFields = scheme.getOutputFields();
		this.streamSplitter = streamSplitter;
	}

	@Override
	protected List<Integer> emit(List<Object> tuple, Message message, SpoutOutputCollector spoutOutputCollector) {
		String stream = streamSplitter.selectStream(tuple, message);
		return spoutOutputCollector.emit(stream, tuple, getDeliveryTag(message));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		for (String stream : streamSplitter.streamNames()) {
			outputFieldsDeclarer.declareStream(stream, outputFields);
		}
	}
}
