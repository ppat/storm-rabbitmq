package io.latent.storm.rabbitmq;

public interface ErrorReporter {
  void reportError(java.lang.Throwable error);
}
