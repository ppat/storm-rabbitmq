package io.latent.storm.rabbitmq;


import org.apache.storm.tuple.Tuple;

/**
 * Simple extension of {@link io.latent.storm.rabbitmq.RabbitMQBolt} that provides the ability to determine whether a message should be published
 * based on the input tuple
 * This class is sort of an SPI meaning that it is meant to be subclassed
 * and the method {@link io.latent.storm.rabbitmq.ConditionalPublishingRabbitMQBolt#shouldPublish}
 * to be overridden with the custom decision logic
 */
public class ConditionalPublishingRabbitMQBolt extends RabbitMQBolt {

    public ConditionalPublishingRabbitMQBolt(TupleToMessage scheme) {
        super(scheme);
    }

    public ConditionalPublishingRabbitMQBolt(TupleToMessage scheme, Declarator declarator) {
        super(scheme, declarator);
    }

    @Override
    public void execute(final Tuple tuple) {
        if(shouldPublish(tuple)) {
            publish(tuple);
        }
        acknowledge(tuple);
    }

    protected boolean shouldPublish(Tuple tuple) {
        return true;
    }
}
