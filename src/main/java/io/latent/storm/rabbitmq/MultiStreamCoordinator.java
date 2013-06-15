package io.latent.storm.rabbitmq;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public interface MultiStreamCoordinator extends Serializable {
  List<String> streamNames();

  String selectStream(List<Object> tuple,
                      Message message);


  public static class RedeliveryStreamSeparator implements MultiStreamCoordinator {
    public static final String INITIAL_DELIVERY_STREAM = "initial_delivery";
    public static final String REDELIVERY_STREAM = "redelivery";

    private static final List<String> streams;
    static {
      List<String> temp = new ArrayList<String>(2);
      temp.add(INITIAL_DELIVERY_STREAM);
      temp.add(REDELIVERY_STREAM);
      streams = Collections.unmodifiableList(temp);
    }

    @Override
    public List<String> streamNames() {
      return streams;
    }

    @Override
    public String selectStream(List<Object> tuple,
                               Message message) {
      Message.DeliveredMessage deliveredMessage = (Message.DeliveredMessage) message;
      return deliveredMessage.isRedelivery() ? REDELIVERY_STREAM : INITIAL_DELIVERY_STREAM;
    }
  }
}
