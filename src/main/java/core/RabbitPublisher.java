package core;

import common.Json;
import configuration.IConfiguration;
import configuration.PropertiesConfig;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by Tommaso Garuglieri on 25/09/2016.
 */
public class RabbitPublisher<T> extends RabbitBase {

    private Function<T, byte[]> adapter;

    private String exchange;

    private boolean queueDurable;


    protected RabbitPublisher(String host, String vHost, String queueName, String username, String password,
                              Consumer<String> logConsumer, Consumer<Throwable> throwableConsumer,
                              Function<T, byte[]> adapter,
                              String exchange, boolean queueDurable) {

        super(host, vHost, queueName, username, password, logConsumer, throwableConsumer);
        this.queueDurable = queueDurable;
        this.exchange = exchange;
        this.adapter = adapter;
    }


    @Override
    public void connect() {
        super.connect();
        try {
            channel.queueDeclare(queueName, queueDurable, false, false, null);
        } catch (IOException e) {
            error(e);
        }
    }

    public void publish(T object) {
        this.publish(adapter.apply(object));
    }

    public void publish(byte[] bytes) {
        try {
            channel.basicPublish(exchange, queueName, null, bytes);
            log(String.format("Published %d bytes to %s", bytes.length, queueName));
        } catch (IOException e) {
            error(e);
        }
    }


    public static class Builder<T> {

        private Class<T> tClass;

        private boolean durableQueue = false;

        private String exchange = "";

        private String host, username, password, queue, vHost;

        private Consumer<String> logConsumer = System.out::println;

        private Consumer<Throwable> throwableConsumer = Throwable::printStackTrace;

        private Function<T, byte[]> adapter;

        public RabbitPublisher.Builder<T> withProperties(String file) {
            IConfiguration configuration = PropertiesConfig.fromFile(file);
            if (!configuration.contains("rabbitmq.host"))
                throw new IllegalArgumentException("Invalid properties file: " + file);
            this.host = configuration.get("rabbitmq.host", "");
            this.username = configuration.get("rabbitmq.username", "");
            this.password = configuration.get("rabbitmq.password", "");
            this.queue = configuration.get("rabbitmq.queue", "");
            this.vHost = configuration.get("rabbitmq.vhost", "");
            return this;
        }
        private boolean debug;

        public Builder<T> debug() {
            debug = true;
            return this;
        }

        public Builder<T> logOn(Consumer<String> consumer) {
            this.logConsumer = consumer;
            return this;
        }

        public Builder<T> onError(Consumer<Throwable> consumer) {
            this.throwableConsumer = consumer;
            return this;
        }

        public Builder<T> host(String host) {
            this.host = host;
            return this;
        }

        public Builder<T> withCredentials(String username, String password) {
            this.username = username;
            this.password = password;
            return this;
        }

        public Builder<T> onVHost(String vHost){
            this.vHost = vHost;
            return this;
        }

        public Builder<T> onQueue(String queue) {
            this.queue = queue;
            return this;
        }

        public Builder<T> of(Class<T> tClass) {
            this.tClass = tClass;
            return this;
        }

        public Builder<T> durableQueue(boolean value) {
            this.durableQueue = value;
            return this;
        }

        public Builder<T> onExchange(String exchange) {
            this.exchange = exchange;
            return this;
        }

        public Builder<T> convert(Function<T, byte[]> function) {
            this.adapter = function;
            return this;
        }

        public RabbitPublisher<T> build() {
            if (adapter == null)
                adapter = t -> Json.getInstance().toJson(t).getBytes();

            RabbitPublisher<T> tRabbitPublisher = new RabbitPublisher<>(
                    host, vHost, queue, username,
                    password, logConsumer, throwableConsumer,
                    adapter, exchange, durableQueue);

            tRabbitPublisher.setDebug(debug);
            return tRabbitPublisher;
        }
    }
}
