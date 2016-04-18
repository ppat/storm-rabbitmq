package io.latent.storm.rabbitmq;

import java.nio.ByteBuffer;
import java.util.*;

import com.rabbitmq.client.LongString;
import org.apache.storm.spout.Scheme;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;

import java.io.Serializable;


public class RabbitMQMessageScheme implements MessageScheme {
  private final Scheme payloadScheme;
  private final List<String> fieldNames;


  public RabbitMQMessageScheme(Scheme payloadScheme, String envelopeFieldName, String propertiesFieldName) {
    this.payloadScheme = payloadScheme;

    List<String> payloadFieldNames = payloadScheme.getOutputFields().toList();

    this.fieldNames = new ArrayList<String>();
    fieldNames.addAll(payloadFieldNames);
    fieldNames.add(envelopeFieldName);
    fieldNames.add(propertiesFieldName);
  }

  @Override
  public void open(Map config, TopologyContext context) {
  }

  @Override
  public void close() {
  }

  @Override 
  public List<Object> deserialize(Message message) {
    Message.DeliveredMessage dm = (Message.DeliveredMessage)message;
    Envelope envelope = createEnvelope(dm);
    Properties properties = createProperties(dm);
    List<Object> payloadValues = deserialize(ByteBuffer.wrap(dm.getBody()));

    List<Object> values = new ArrayList<Object>();
    values.addAll(payloadValues);
    values.add(envelope);
    values.add(properties);

    return values;
  }

  @Override
  public List<Object> deserialize(ByteBuffer byteBuffer) {
    return payloadScheme.deserialize(byteBuffer);
  }

  @Override
  public Fields getOutputFields() {
    return new Fields(fieldNames);
  }

  private Envelope createEnvelope(Message.DeliveredMessage dm) {
    return new Envelope(dm.isRedelivery(), dm.getDeliveryTag(), dm.getExchange(), dm.getRoutingKey());
  }

  private Properties createProperties(Message.DeliveredMessage dm) {
    return new Properties(dm.getClassName(),
            dm.getClusterId(),
            dm.getContentEncoding(),
            dm.getContentType(),
            dm.getCorrelationId(),
            dm.getDeliveryMode(),
            dm.getExpiration(),
            serializableHeaders(dm.getHeaders()),
            dm.getMessageId(),
            dm.getPriority(),
            dm.getReplyTo(),
            dm.getTimestamp(),
            dm.getType(),
            dm.getUserId());
  }

    private Map<String, Object> serializableHeaders(Map<String, Object> headers) {
        if (headers == null) {
            return new HashMap<String, Object>();
        }

        Map<String, Object> headersSerializable = new HashMap<String, Object>(headers.size());
        for (Map.Entry<String, Object> entry : headers.entrySet()) {
            if (entry.getValue() instanceof Number ||
                    entry.getValue() instanceof Boolean ||
                    entry.getValue() instanceof Character ||
                    entry.getValue() instanceof String ||
                    entry.getValue() instanceof Date) {
                headersSerializable.put(entry.getKey(), entry.getValue());
            } else if (entry.getValue() instanceof LongString) {
                headersSerializable.put(entry.getKey(), entry.getValue().toString());
            } else if (entry.getValue() instanceof ArrayList) {
                ArrayList serializedList = new ArrayList();
                for (Object elm : ((ArrayList) entry.getValue())) {
                    if (elm instanceof HashMap) {
                        serializedList.add(serializableHeaders((HashMap<String, Object>) elm));
                    }
                }
                headersSerializable.put(entry.getKey(), serializedList);
            }
        }
        return headersSerializable;
    }

  public static class Envelope implements Serializable {
    private final boolean isRedelivery;
    private final long deliveryTag;
    private final String exchange;
    private final String routingKey;

    public Envelope(boolean isRedelivery, long deliveryTag, String exchange, String routingKey) {
      this.isRedelivery = isRedelivery;
      this.deliveryTag = deliveryTag;
      this.exchange = exchange;
      this.routingKey = routingKey;
    }

    public boolean isRedelivery() { return isRedelivery; }
    public long getDeliveryTag() { return deliveryTag; }
    public String getExchange() { return exchange; }
    public String getRoutingKey() { return routingKey; }
  }

  public static class Properties implements Serializable {
    private final String className;
    private final String clusterId;
    private final String contentEncoding;
    private final String contentType;
    private final String correlationId;
    private final Integer deliveryMode;
    private final String expiration;
    private final Map<String, Object> headers;
    private final String messageId;
    private final Integer priority;
    private final String replyTo;
    private final Date timestamp;
    private final String type;
    private final String userId;


    public Properties(String className,
                      String clusterId,
                      String contentEncoding,
                      String contentType,
                      String correlationId,
                      Integer deliveryMode,
                      String expiration,
                      Map<String, Object> headers,
                      String messageId,
                      Integer priority,
                      String replyTo,
                      Date timestamp,
                      String type,
                      String userId) {
      this.className = className;
      this.clusterId = clusterId;
      this.contentEncoding = contentEncoding;
      this.contentType = contentType;
      this.correlationId = correlationId;
      this.deliveryMode = deliveryMode;
      this.expiration = expiration;
      this.headers = headers;
      this.messageId = messageId;
      this.priority = priority;
      this.replyTo = replyTo;
      this.timestamp = timestamp;
      this.type = type;
      this.userId = userId;
    }

    public String getClassName() { return className; }
    public String getClusterId() { return clusterId; }
    public String getContentEncoding() { return contentEncoding; }
    public String getContentType() { return contentType; }
    public String getCorrelationId() { return correlationId; }
    public Integer getDeliveryMode() { return deliveryMode; }
    public String getExpiration() { return expiration; }
    public Map<String, Object> getHeaders() { return headers; }
    public String getMessageId() { return messageId; }
    public Integer getPriority() { return priority; }
    public String getReplyTo() { return replyTo; }
    public Date getTimestamp() { return timestamp; }
    public String getType() { return type; }
    public String getUserId() { return userId; }
  }
}

