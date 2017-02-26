package core;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Created by Tommaso Garuglieri on 18/09/2016.
 */
public class RabbitBase {

    private Consumer<String> logConsumer;

    private Consumer<Throwable> throwableConsumer;

    String queueName;

    String host;

    String virtualHost;

    Channel channel;

    Connection connection;

    String username, password;

    private boolean debug = false;

    protected ConnectionFactory connectionFactory;

    protected RabbitBase(String host, String vHost, String queueName, String username, String password, Consumer<String> logConsumer, Consumer<Throwable> throwableConsumer) {
        this.host = host;
        this.queueName = queueName;
        this.username = username;
        this.password = password;
        this.logConsumer = logConsumer;
        this.throwableConsumer = throwableConsumer;
        this.virtualHost = vHost;
    }


    public void connect() {
        try {
            connectionFactory = new ConnectionFactory();
            connectionFactory.setHost(host);

            if (username != null && !username.isEmpty())
                connectionFactory.setUsername(username);
            if (password != null && !password.isEmpty())
                connectionFactory.setPassword(password);
            if (virtualHost != null && !virtualHost.isEmpty())
                connectionFactory.setVirtualHost(virtualHost);

            this.connection = connectionFactory.newConnection();
            this.channel = connection.createChannel();
            log("Connected " + host);
        } catch (IOException | TimeoutException e) {
            error(e);
        }
    }

    public void disconnect() {
        try {
            if (connection != null && connection.isOpen())
                connection.close();
            if (channel != null && channel.isOpen())
                channel.close();
            log("Closed connection with " + host + " - " + queueName);
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    protected void log(String message) {
        if (debug) {
            logConsumer.accept(message);
        }
    }

    protected void error(Throwable error) {
        throwableConsumer.accept(error);
    }


}
