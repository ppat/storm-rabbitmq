package io.latent.storm.rabbitmq;

import java.util.List;
import java.util.Map;

import backtype.storm.spout.Scheme;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;

public interface MessageScheme extends Scheme {
  void open(@SuppressWarnings("rawtypes") Map config, TopologyContext context);

  void close();

  List<Object> deserialize(Message message);

  public static class Builder {
    public static MessageScheme from(final Scheme scheme) {
      if (scheme instanceof MessageScheme)
        return (MessageScheme) scheme;
      else
        return create(scheme);
    }

    private static MessageScheme create(final Scheme scheme) {
      return new MessageScheme() {
        /**
         * Serial version UID.
         */
        private static final long serialVersionUID = 1L;

        @Override
        public void open(@SuppressWarnings("rawtypes") Map config, TopologyContext context) {
        }

        @Override
        public void close() {
        }

        @Override
        public List<Object> deserialize(Message message) {
          return scheme.deserialize(message.getBody());
        }

        @Override
        public List<Object> deserialize(byte[] bytes) {
          return scheme.deserialize(bytes);
        }

        @Override
        public Fields getOutputFields() {
          return scheme.getOutputFields();
        }
      };
    }
  }
}
