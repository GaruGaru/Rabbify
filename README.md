# Rabbify

## Java fluent RabbitMQ consumer/publisher api

![alt tag](https://github.com/GaruGaru/Rabbify/blob/master/raw/icon.png)

# Usage

## Configuration

Create a **rabbit.properties** file in your project root:

```
    host=127.0.0.1
    queueName=queue-test
    username=guest
    password=guest
```

## Publisher

### Publish object


```
        RabbitPublisher<Rabbit> publisher = new RabbitPublisher.Builder<Rabbit>()
                .of(Rabbit.class)
                .withProperties("rabbit.properties")
                .build();

        publisher.connect();

        publisher.publish(new Rabbit("Rabbit-Name"));

        publisher.disconnect();
```
        
## Consumer

### Consume object

```
   RabbitConsumer<Rabbit> consumer = new RabbitConsumer.Builder<Rabbit>()
                .of(Rabbit.class)
                .consume(System.out::println)
                .withProperties("rabbit.properties")
                .prefetch(10)
                .parallelism(5)
                .autoAck(true)
                .build();

        consumer.connect();

        consumer.consume();

        consumer.disconnect();
             
```
