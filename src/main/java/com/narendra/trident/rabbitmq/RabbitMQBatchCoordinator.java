package com.narendra.trident.rabbitmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.spout.ITridentSpout.BatchCoordinator;

/**
 * Bare implementation of a BatchCoordinator, returning a null RabbitMQ object
 *
 */
public class RabbitMQBatchCoordinator implements BatchCoordinator<RabbitMQBatch> {

  private final String name;

  private final Logger LOG = LoggerFactory.getLogger(RabbitMQBatchCoordinator.class);

  public RabbitMQBatchCoordinator(String name) {
    this.name = name;
    LOG.info("Created batch coordinator for " + name);
  }

  @Override
  public RabbitMQBatch initializeTransaction(long txid, RabbitMQBatch prevMetadata, RabbitMQBatch curMetadata) {
    LOG.debug("Initialise transaction " + txid + " for " + name);
    return null;
  }

  @Override
  public void success(long txid) {}

  @Override
  public boolean isReady(long txid) {
    return true;
  }

  @Override
  public void close() {}

}

