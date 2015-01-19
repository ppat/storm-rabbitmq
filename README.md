# storm-rabbitmq

[![Build Status](https://travis-ci.org/ppat/storm-rabbitmq.png)](https://travis-ci.org/ppat/storm-rabbitmq)


Storm RabbitMQ is a library of tools to be employed while integrating with [RabbitMQ](https://github.com/rabbitmq/rabbitmq-server/) from [Storm](https://github.com/nathanmarz/storm/). This library is intended to be used with RabbitMQ specifically and may not work with other AMQP brokers as this library will be using RabbitMQ specific extensions to AMQP.

LICENSE: MIT License


## Pre-requisites

You will need an implementation of ```backtype.storm.spout.Scheme``` to deserialize a RabbitMQ message.


## RabbitMQ Spout

- This spout will deserialize incoming messages using ```YourCustomMessageScheme``` and emit it on an anchored stream.

```java
Scheme scheme = new YourCustomMessageScheme();
IRichSpout spout = new RabbitMQSpout(scheme);
```

- Configuring connection to RabbitMQ. If ```requeueOnFail``` is turned on, messages will be redelivered if they fail anywhere within the topology. If its turned off, failed messages are removed from the queue and potentially sent to a [dead letter exchange](http://www.rabbitmq.com/dlx.html) in RabbitMQ (if one has been configured for this queue).

```java
ConnectionConfig connectionConfig = new ConnectionConfig("localhost", 5672, "guest", "guest", ConnectionFactory.DEFAULT_VHOST, 10); // host, port, username, password, virtualHost, heartBeat 
ConsumerConfig spoutConfig = new ConsumerConfigBuilder().connection(connectionConfig)
                                                        .queue("your.rabbitmq.queue")
                                                        .prefetch(200)
                                                        .requeueOnFail()
                                                        .build();
```

- Add to topology using TopologyBuilder. Set the MaxSpoutPending in Storm to same value as RabbitMQ's Prefetch count (set in ConsumerConfig above) initially. You can tune them later separately but MaxSpoutPending should always be <= Prefetch.

```java
TopologyBuilder builder = new TopologyBuilder();

builder.setSpout("my-spout", spout)
       .addConfigurations(spoutConfig.asMap())
       .setMaxSpoutPending(200);
```

## Unanchored Spout

While the standard ```RabbitMQSpout``` above will deliver messages on an anchored stream, if fault tolerance is not required, you can use the ```UnanchoredRabbitMQSpout```.

```java
Scheme scheme = new YourCustomMessageScheme();
IRichSpout spout = new UnanchoredRabbitMQSpout(scheme);
```

## MultiStream Spout

If you want to split the incoming message stream from your RabbitMQ queue in some manner suitable for your use case, you can use the ```MultiStreamSpout```. You need to provide an implementation of ```MultiStreamSplitter``` that will separate the stream of tuples based on either the deserialized message (as a tuple) or the original serialized ```Message```.

```java
MultiStreamSplitter streamSeparator = new MultiStreamSplitter() {
  @Override
  public List<String> streamNames() {
     return Arrays.asList("stream-X", "stream-Y");
  }

  @Override
  public String selectStream(List<Object> tuple, Message message) {
     // you can look at the deserialized messge in the form of List<Object> tuple
     // or you can look at the original RabbitMQ Message to determine which stream it should be emitted in
     // this is just a simple example for demonstration purpose, you probably will want to inspect the right tuple
     // or message values to and do something more intelligent to determine which stream it should be assigned to
     if (tuple.get(0).toString().equalsIgnoreCase("something"))
       return "stream-X";
     else
       return "stream-Y";
  }
};

IRichSpout spout = new MultiStreamSpout(scheme, streamSeparator);
```

Now you can bind any bolts to this spout on either "stream-X" or "stream-Y".

```java
TopologyBuilder builder = new TopologyBuilder();

builder.setSpout("split-streams-spout", spout)
       .addConfigurations(spoutConfig.asMap())
       .setMaxSpoutPending(200);
builder.setBolt("work-on-stream-X", new StreamXBolt())
       .shuffleGrouping("split-streams-spout", "stream-X");       
builder.setBolt("work-on-stream-Y", new StreamYBolt())
       .shuffleGrouping("split-streams-spout", "stream-Y");       
```

### RedeliveryStreamSeparator

This comes with an implementation of ```MultiStreamSplitter```  called ```RedeliveryStreamSeparator``` which can be used when you want to split the tuple stream into initial deliveries and redeliveries of messages that failed somewhere within the topology. Since RabbitMQ returns all failed messages back to the beginning of the queue, by separating redeliveries from initial deliveries, you can ensure that failing messages do not clog the stream for complete message stream.

```java
MultiStreamSplitter streamSeparator = new RedeliveryStreamSeparator();
IRichSpout spout = new MultiStreamSpout(scheme, streamSeparator);
```

Now you can send initial deliveries to a FastBolt that can fail any tuple that cannot be processed quickly by timing out the calculation. So that the main stream of tuples will have no bottleneck due to individual messages that may take a long time to process. The messages that take a long time to process will be redelivered on a separate stream that will go to the SlowBolt.

```java
TopologyBuilder builder = new TopologyBuilder();

builder.setSpout("redelivery-split-spout", spout)
       .addConfigurations(spoutConfig.asMap())
       .setMaxSpoutPending(200);
builder.setBolt("process-quickly-or-fail-bolt", new FastBolt(), 100) // fast bolt with parallelism
       .shuffleGrouping("redelivery-split-spout", RedeliveryStreamSeparator.INITIAL_DELIVERY_STREAM);       
builder.setBolt("retry-failures-with-longer-timeout", new SlowBolt(),  20) // slow bolt with different parallelism
       .shuffleGrouping("redelivery-split-spout", RedeliveryStreamSeparator.REDELIVERY_STREAM);       
```

## Declarator

By default, these spouts assume that the queue in question already exists in RabbitMQ. If you want the queue declaration to also happen on the spout, you need to provide an implementation of ```io.latent.storm.rabbitmq.Declarator```. Declarator (and therefore storm-rabbitmq) is unopinionated about how the queue this spout will listen on should be wired to exchange(s) and you are free to choose any form of wiring that serves your use case.

```java
public class CustomStormDeclarator implements Declarator {
  private final String exchange;
  private final String queue;
  private final String routingKey;

  public CustomStormDeclarator(String exchange, String queue) {
    this(exchange, queue, "");
  }

  public CustomStormDeclarator(String exchange, String queue, String routingKey) {
    this.exchange = exchange;
    this.queue = queue;
    this.routingKey = routingKey;
  }

  @Override
  public void execute(Channel channel) {
    // you're given a RabbitMQ Channel so you're free to wire up your exchange/queue bindings as you see fit
    try {
      Map<String, Object> args = new HashMap<>();
      channel.queueDeclare(queue, true, false, false, args);
      channel.exchangeDeclare(exchange, "topic", true);
      channel.queueBind(queue, exchange, routingKey);
    } catch (IOException e) {
      throw new RuntimeException("Error executing rabbitmq declarations.", e);
    }
  }
}
```

And then pass it to spout constructor. 
```
Declarator declarator = new CustomStormDeclarator("your.exchange", "your.rabbitmq.queue", "routing.key");
IRichSpout spout = new RabbitMQSpout(scheme, declarator);
``` 
The other spouts (UnanchoredRabbitMQSpout, MultiStreamSpout) also take in the declarator as a parameter.

## RabbitMQMessageScheme

The standard backtype message scheme only allows access to the payload of a message received via RabbitMQ. Normally, the payload is all you will need. There are scenarios where this isn't true; you need access to the routing key as part of your topology logic, you only want to handle "new" messages and need access to the message timestamp, whatever your use case, the payload isn't enough. The provided RabbitMQMessageScheme allows you to gain access to RabbitMQ message information without having to change every bolt that interacts with a RabbitMQSpout. 

When constructing a RabbitMQMessageScheme you need to provide 3 pieces of information:

* an implementation of ```backtype.storm.spout.Scheme``` to deserialize a RabbitMQ message payload.
* the tuple field name to use for RabbitMQ message envelope info
* the tuple field name to use for the RabbitMQ properties info

The first should be fairly explanatory. You supply your existing payload handling scheme. All existing bolts will continue to function as is. No field names need to be changed nor would any field indexes. The supplied envelope and properties names will be used to allow you to access them in your bolt. Additionally, if you access tuple fields by index, the envelope and properties will be added as 2 additional fields at the end of the tuple. 

If you were to create a RabbitMQMessageScheme as below:

```java
Scheme scheme = new RabbitMQMessageScheme(new SimpleJSONScheme(), "myMessageEnvelope", "myMessageProperties");
```

then in any bolt attached to the spout stream you could access them as:

```java
RabbitMQMessageScheme.Envelope envelope = tuple.getValueByField("myMessageEnvelope");

RabbitMQMessageScheme.Properties properties = tuple.getValueByField("myMessageProperties");
```

All standard RabbitMQ envelope and message properties are available. See RabbitMQMessageScheme.java for the full interface.


## RabbitMQ as a Sink

There may be times when you wish to send messages to RabbitMQ at the end of one of the stream within your topology. 

First you need to provide an implementation of `TupleToMessage`. This indicates how to transform an incoming tuple within your stream into a RabbitMQ Message.

```java
TupleToMessage scheme = new TupleToMessage() {
  @Override
  byte[] extractBody(Tuple input) { return input.getStringByField("my-message-body").getBytes(); }

  @Override
  String determineExchangeName(Tuple input) { return input.getStringByField("exchange-to-publish-to"); }

  @Override
  String determineRoutingKey(Tuple input) { return input.getStringByField("my-routing-key"); }

  @Override
  Map<String, Object> specifiyHeaders(Tuple input) { return new HashMap<String, Object>(); }

  @Override
  String specifyContentType(Tuple input) { return "application/json"; }

  @Override
  String specifyContentEncoding(Tuple input) { return "UTF-8"; }

  @Override
  boolean specifyMessagePersistence(Tuple input) { return false; }
};
```

Then you need your RabbitMQ connection config (just like we did for the spout before).

```java
ConnectionConfig connectionConfig = new ConnectionConfig("localhost", 5672, "guest", "guest", ConnectionFactory.DEFAULT_VHOST, 10); // host, port, username, password, virtualHost, heartBeat 
ProducerConfig sinkConfig = new ProducerConfigBuilder().connection(connectionConfig).build();
```                                                        

Now we are ready to add RabbitMQBolt as a sink to your topology.

```java
TopologyBuilder builder = new TopologyBuilder
...
builder.setBolt("rabbitmq-sink", new RabbitMQBolt(scheme))
       .addConfigurations(sinkConfig)
       .shuffleGrouping("previous-bolt")
```       

### When message attributes are non-dynamic

Sometimes your message attributes (exchange name, routing key, content-type, etc..) do not change on a message by message basis and are fixed per topology. When this is the case you can use `TupleToMessageNonDynamic` to provide a more simplified implementation by providing the required fields (exchange name, routing key) via storm configuration.

```java
TupleToMessage scheme = new TupleToMessageNonDynamic() {
  @Override
  byte[] extractBody(Tuple input) { return input.getStringByField("my-message-body").getBytes(); }
};
ConnectionConfig connectionConfig = new ConnectionConfig("localhost", 5672, "guest", "guest", ConnectionFactory.DEFAULT_VHOST, 10); // host, port, username, password, virtualHost, heartBeat 
ProducerConfig sinkConfig = new ProducerConfigBuilder()
    .connection(connectionConfig)
    .contentEncoding("UTF-8")
    .contentType("application/json")
    .exchange("exchange-to-publish-to")
    .routingKey("")
    .build();
...
builder.setBolt("rabbitmq-sink", new RabbitMQBolt(scheme))
       .addConfigurations(sinkConfig)
       .shuffleGrouping("previous-bolt")
```
