package io.latent.storm.rabbitmq;

import com.rabbitmq.client.Channel;

import java.io.Serializable;

public interface Declarator extends Serializable {
  void execute(Channel channel);

  public static class NoOp implements Declarator {
    @Override
    public void execute(Channel channel) {}
  }
}
