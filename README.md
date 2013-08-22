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

- Configuring connection to RabbitMQ. If ```requeueOnFail``` is turned on, messages will be redelivered if they fail anywhere within the topology.

```java
    /* host, port, username, password, virtualHost, heartBeat */
    ConnectionConfig connectionConfig = new ConnectionConfig("localhost", 5672, "guest", "guest", ConnectionFactory.DEFAULT_VHOST, 10);
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

## Declarator

By default, these spouts assume that the queue in question already exists in RabbitMQ. If you want the queue declaration to also happen on the spout, you need to provide an implementation of ```io.latent.storm.rabbitmq.Declarator```. 

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
    try {
      Map<String, Object> map = new HashMap<>();
      channel.queueDeclare(queue, true, false, false, map);

      channel.exchangeDeclare(exchange, "topic", true);
      channel.queueBind(queue, exchange, routingKey);
    } catch (IOException e) {
      throw new RuntimeException("Error executing rabbitmq declarations.", e);
    }
  }
}
```
