# storm-rabbitmq

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

While the standard ```RabbitMQSpout``` above will deliver messages on an anchored stream, if fault tolerance is not required, you can use the ```UnanchordRabbitMQSpout```.

```java
Scheme scheme = new YourCustomMessageScheme();
IRichSpout spout = new UnanchordRabbitMQSpout(scheme);
```

## MultiStream Spout

If you want to split the incoming message stream from your RabbitMQ queue in some manner suitable for your use case, you can use the ```MultiStreamSpout```. You need to provide an implementation of ```MultiStreamCoordinator``` that will separate the stream of tuples based on either the deserialized message (as a tuple) or the original serialized ```Message```.

```java
MultiStreamCoordinator streamSeparator = new MultiStreamCoordinator()
{
  @Override
  public List<String> streamNames()
  {
     return Arrays.asList("stream-X", "stream-Y");
  }

  @Override
  public String selectStream(List<Object> tuple, Message message)
  {
     // you can look at the deserialized messge in the form of List<Object> tuple
     // or you can look at the original RabbitMQ Message to determine which stream it should be emitted in
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

This comes with an implementation of ```MultiStreamCoordinator```  called ```RedeliveryStreamSeparator``` which can be used when you want to split the tuple stream into initial deliveries and redeliveries of messages that failed somewhere within the topology. Since RabbitMQ returns all failed messages back to the beginning of the queue, by separating redeliveries from initial deliveries, you can ensure that failing messages do not clog the stream for complete message stream.

```java
MultiStreamCoordinator.RedeliveryStreamSeparator streamSeparator = new MultiStreamCoordinator.RedeliveryStreamSeparator();
IRichSpout spout = new MultiStreamSpout(scheme, streamSeparator);
```

Now you can send initial deliveries to a FastBolt that can fail any tuple that cannot be processed quickly by timing out the calculation. So that the main stream of tuples will have no bottleneck due to individual messages that may take a long time to process. The messages that take a long time to process will be redelivered on a separate stream that will go to the SlowBolt.

```java
TopologyBuilder builder = new TopologyBuilder();

builder.setSpout("redelivery-split-spout", spout)
       .addConfigurations(spoutConfig.asMap())
       .setMaxSpoutPending(200);
builder.setBolt("process-quickly-or-fail-bolt", new FastBolt(), 100) // fast bolt with higher parallelism
       .shuffleGrouping("redelivery-split-spout", MultiStreamCoordinator.RedeliveryStreamSeparator.INITIAL_DELIVERY_STREAM);       
builder.setBolt("retry-failures-with-longer-timeout", new SlowBolt(),  10) // slow bolt with lower parallelism
       .shuffleGrouping("redelivery-split-spout", MultiStreamCoordinator.RedeliveryStreamSeparator.REDELIVERY_STREAM);       
```

## Declarator

By default, these spouts assume that the queue in question already exists in RabbitMQ. If you want the queue declaration to also happen on the spout, you need to provide an implementation of ```io.latent.storm.rabbitmq.Declarator```. Declarator (and therefore storm-rabbitmq) is unopinionated about how the queue this spout will listen on should be wired to exchange(s) and you are free to choose any form of wiring that serves your use case.

```java
public class CustomStormDeclarator implements Declarator
{
  private final String exchange;
  private final String queue;
  private final String routingKey;

  public CustomStormDeclarator(String exchange, String queue)
  {
    this(exchange, queue, "");
  }

  public CustomStormDeclarator(String exchange, String queue, String routingKey)
  {
    this.exchange = exchange;
    this.queue = queue;
    this.routingKey = routingKey;
  }

  @Override
  public void execute(Channel channel)
  {
    // you're given a RabbitMQ Channel so you're free to wire up your exchange/queue bindings as you see fit
    try {
      Map<String, Object> args = new HashMap<>();
      // declare queue
      channel.queueDeclare(queue, true, false, false, args);
      // declare exchange
      channel.exchangeDeclare(exchange, "topic", true);
      // bind to queue to exchange
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
The other spouts (UnanchordRabbitMQSpout, MultiStreamSpout) also take in the declarator as a parameter.

