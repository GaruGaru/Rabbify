package core;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by Garu on 18/02/2017.
 */
public class RabbifyConsumerWorker<T> extends DefaultConsumer {

    private final Function<byte[], T> adapter;
    private final Consumer<T> consumer;
    private final boolean autoAck;
    private final ExecutorService executorService;

    public RabbifyConsumerWorker(Channel channel, Function<byte[], T> adapter, Consumer<T> messageConsumer, boolean autoAck, ExecutorService executorService) {
        super(channel);
        this.adapter = adapter;
        this.consumer = messageConsumer;
        this.autoAck = autoAck;
        this.executorService = executorService;
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {

        executorService.execute(() -> {
            consumer.accept(adapter.apply(body));
            try {
                if (!autoAck)
                    getChannel().basicAck(envelope.getDeliveryTag(), false);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
}
