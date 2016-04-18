package io.latent.storm.rabbitmq;

import org.apache.storm.spout.Scheme;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public interface MessageScheme extends Scheme {
  void open(Map config,
            TopologyContext context);

  void close();

  List<Object> deserialize(Message message);

  class Builder {
    public static MessageScheme from(final Scheme scheme) {
      if (scheme instanceof MessageScheme)
        return (MessageScheme) scheme;
      else
        return create(scheme);
    }

    private static MessageScheme create(final Scheme scheme) {
      return new MessageScheme() {
        @Override
        public void open(Map config,
                         TopologyContext context) { }

        @Override
        public void close() { }

        @Override
        public List<Object> deserialize(Message message) {
          return scheme.deserialize(ByteBuffer.wrap(message.getBody()));
        }

        @Override
        public List<Object> deserialize(ByteBuffer byteBuffer) {
          return scheme.deserialize(byteBuffer);
        }

        @Override
        public Fields getOutputFields() {
          return scheme.getOutputFields();
        }
      };
    }
  }
}
