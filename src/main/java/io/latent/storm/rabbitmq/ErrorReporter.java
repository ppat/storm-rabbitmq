package io.latent.storm.rabbitmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface ErrorReporter {
  void reportError(java.lang.Throwable error);
  
  ErrorReporter defaultReporter = new LogErrorReporter();
  
  class LogErrorReporter implements ErrorReporter {
	  static Logger _LOGGER = LoggerFactory.getLogger(LogErrorReporter.class);

	@Override
	public void reportError(Throwable error) {
		_LOGGER.error("Error", error);
	}
  }
}
