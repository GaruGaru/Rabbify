package core;

import com.rabbitmq.client.DefaultConsumer;
import common.Json;
import configuration.IConfiguration;
import configuration.PropertiesConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by Tommaso Garuglieri on 25/09/2016.
 */
public class RabbitConsumer<T> extends RabbitBase {

    private Class<T> tClass;

    private Map<String, Object> argsMap;

    private int prefetch = 10;

    private int parallelism = 1;

    private boolean autoAck;
    private Consumer<T> messageConsumer = t -> log(t.toString());

    private Function<byte[], T> adapter;

    private ExecutorService executorService;

    protected RabbitConsumer(String host, String vHost, String queueName, String username, String password,
                             Consumer<String> logConsumer, Consumer<Throwable> throwableConsumer,
                             Class<T> tClass, Consumer<T> consumer, Function<byte[], T> conveter,
                             int prefetch, int parallelism, boolean autoAck, Map<String, Object> argsMap) {

        super(host, vHost, queueName, username, password, logConsumer, throwableConsumer);
        this.tClass = tClass;
        this.prefetch = prefetch;
        this.parallelism = parallelism;
        this.autoAck = autoAck;
        this.argsMap = argsMap;
        this.messageConsumer = consumer;
        this.adapter = conveter;
    }

    @Override
    public void connect() {
        super.connect();
        this.executorService = Executors.newFixedThreadPool(parallelism);
        log("Connected " + host);
    }

    public void consume() {
        final DefaultConsumer consumer = new RabbifyConsumerWorker<>(channel, adapter, messageConsumer, autoAck, executorService);

        try {
            channel.queueDeclare(queueName, false, false, false, argsMap);
            channel.basicQos(prefetch);
            channel.basicConsume(queueName, autoAck, argsMap, consumer);
        } catch (IOException e) {
            error(e);
        }

    }

    @Override
    public void disconnect() {
        try {
            int timeout = (int) argsMap.getOrDefault("shutdown-timeout", 1000);
            this.executorService.awaitTermination(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            super.disconnect();
        }

    }

    public static class Builder<T> {

        private Map<String, Object> argsMap = new HashMap<>();

        private int prefetch = 1, parallelism = 1;

        private boolean autoAck;
        private Class<T> tClass;

        private String host, username, password, queue, vHost;

        private Consumer<String> logConsumer = System.out::println;

        private Consumer<Throwable> throwableConsumer = Throwable::printStackTrace;

        private Consumer<T> messageConsumer;

        private Function<byte[], T> converter;

        private boolean debug;

        public Builder<T> debug() {
            debug = true;
            return this;
        }

        public Builder<T> withProperties(String file) {
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

        public Builder<T> logOn(Consumer<String> consumer) {
            this.logConsumer = consumer;
            return this;
        }

        public Builder<T> onError(Consumer<Throwable> consumer) {
            this.throwableConsumer = consumer;
            return this;
        }

        public Builder<T> consume(Consumer<T> consumer) {
            this.messageConsumer = consumer;
            return this;
        }

        public Builder<T> convert(Function<byte[], T> converter) {
            this.converter = converter;
            return this;
        }

        public Builder<T> consumerPriority(int value) {
            argsMap.put("x-max-priority", value);
            return this;
        }

        public Builder<T> shutdownTimeout(int value) {
            argsMap.put("shutdown-timeout", value);
            return this;
        }

        public Builder<T> onVHost(String vHost){
            this.vHost = vHost;
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

        public Builder<T> onQueue(String queue) {
            this.queue = queue;
            return this;
        }

        public Builder<T> of(Class<T> tClass) {
            this.tClass = tClass;
            return this;
        }

        public Builder<T> prefetch(int prefetch) {
            this.prefetch = prefetch;
            return this;
        }

        public Builder<T> parallelism(int parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public Builder<T> autoAck(boolean value) {
            this.autoAck = value;
            return this;
        }

        public RabbitConsumer<T> build() {

            if (converter == null)
                converter = bytes -> Json.getInstance().fromJson(new String(bytes), tClass);

            RabbitConsumer<T> tRabbitConsumer = new RabbitConsumer<>(host, vHost, queue, username,
                    password, logConsumer, throwableConsumer,
                    tClass, messageConsumer, converter, prefetch, parallelism, autoAck, argsMap);
            tRabbitConsumer.setDebug(debug);

            return tRabbitConsumer;
        }
    }

}
