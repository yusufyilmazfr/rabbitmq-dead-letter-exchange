package com.example.rabbitmqdeadletterexchange;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class Main
{
    private static final String EXCHANGE_NAME = "tickets";
    private static final String EXCHANGE_TYPE = "direct";
    private static final String ROUTING_KEY = "payment_is_done";
    private static final String QUEUE_NAME = "TICKETS_AFTER_APPROVING_PAYMENT";

    private static final String DLX_EXCHANGE_NAME = "tickets_dlx";
    private static final String DLX_QUEUE_NAME = "DLX_TICKETS_AFTER_APPROVING_PAYMENT";

    private static final boolean DURABLE = true;
    private static final boolean EXCLUSIVE = false;
    private static final boolean AUTO_DELETE = false;
    private static final Map<String, Object> ARGS = new HashMap<>()
    {
        {
            put("x-max-length", 10);
            put("x-message-ttl", 5000);
            put("x-dead-letter-exchange", DLX_EXCHANGE_NAME);
            put("x-dead-letter-routing-key", ROUTING_KEY);
        }
    };

    public static void main(String[] args) throws IOException, TimeoutException
    {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);
        channel.exchangeDeclare(DLX_EXCHANGE_NAME, EXCHANGE_TYPE);

        channel.queueDeclare(QUEUE_NAME, DURABLE, EXCLUSIVE, AUTO_DELETE, ARGS);
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);

        channel.queueDeclare(DLX_QUEUE_NAME, DURABLE, EXCLUSIVE, AUTO_DELETE, null);
        channel.queueBind(DLX_QUEUE_NAME, DLX_EXCHANGE_NAME, ROUTING_KEY);

        Consumer<PaymentInfo> publish = (PaymentInfo paymentInfo) ->
        {
            try
            {
                System.out.println("It has been sent! PaymentInfo: " + paymentInfo);
                channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, null,
                        paymentInfo.toString().getBytes(StandardCharsets.UTF_8));
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        };

        Runnable messagePublisher = () ->
        {
            Executors
                    .newScheduledThreadPool(1)
                    .scheduleAtFixedRate(() -> publish.accept(new PaymentInfo()), 1L, 1L, TimeUnit.SECONDS);

        };

        DeliverCallback deliverCallbackDlx = (consumerTag, delivery) -> {
            String routingKey = delivery.getEnvelope().getRoutingKey();
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

            System.out.println(" [x] Received WITH DLX! '" + routingKey + "':'" + message + "'");

            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };

        Runnable messageConsumerDlx = () ->
        {
            try
            {
                channel.basicConsume(DLX_QUEUE_NAME, false, deliverCallbackDlx, consumerTag -> {
                });
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        };

        messagePublisher.run();
        messageConsumerDlx.run();
    }
}

class PaymentInfo
{
    private static final Random random = new Random();

    private final Integer userId;
    private final Integer paymentId;

    public PaymentInfo()
    {
        this.userId = Math.abs(random.nextInt());
        this.paymentId = Math.abs(random.nextInt());
    }

    @Override
    public String toString()
    {
        return "PaymentInfo{" +
                "userId=" + userId +
                ", paymentId=" + paymentId +
                '}';
    }
}