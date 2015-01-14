package io.latent.storm.rabbitmq;

import java.io.Serializable;

import com.rabbitmq.client.Channel;

public interface Declarator extends Serializable {
	void execute(Channel channel);

	public static class NoOp implements Declarator {
		/**
		 * Serial version UID.
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public void execute(Channel channel) {
		}
	}
}
