package com.meltwater.rxrabbit;

import com.google.common.collect.Lists;
import com.meltwater.rxrabbit.impl.DefaultChannelFactory;
import com.meltwater.rxrabbit.util.Logger;
import com.rabbitmq.client.AMQP;
import rx.Observable;
import rx.Subscription;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ExampleCode {

    public int publishAndConsume(int numMessages,
                                 long maxWaitMillis,
                                 String brokerHost,
                                 int brokerPort,
                                 String queue,
                                 String exchange) throws IOException, InterruptedException {
        //---------- Set Up ----------
        Logger logger = new Logger(ExampleCode.class);

        final ConnectionSettings connectionSettings = new ConnectionSettings();

        BrokerAddresses brokers = new BrokerAddresses(Lists.newArrayList(
                new BrokerAddresses.BrokerAddressBuilder()
                        .withHost(brokerHost)
                        .withPort(brokerPort)
                        .build()));

        DefaultChannelFactory channelFactory = new DefaultChannelFactory(brokers, connectionSettings);

        //Create queue, exchange and bind together
        AdminChannel adminChannel = channelFactory.createAdminChannel();
        adminChannel.exchangeDeclare(exchange, "topic", true, false, false, new HashMap<>());
        adminChannel.queueDeclare(queue, true, false,false, new HashMap<>());
        adminChannel.queueBind(queue, exchange,"#", new HashMap<>());
        adminChannel.close();

        //---------- Publish messages ----------
        final PublisherSettings publisherSettings = new PublisherSettings()
                .withNumChannels(2)
                .withPublisherConfirms(true)
                .withRetryCount(3);

        RabbitPublisher publisher = new DefaultPublisherFactory(channelFactory, publisherSettings).createPublisher();

        Observable.range(0,numMessages)
                .map(String::valueOf)
                .flatMap((input) ->
                        publisher.call(
                                new Exchange(exchange),
                                new RoutingKey("#"),
                                new AMQP.BasicProperties(),
                                new Payload(input.getBytes())).toObservable()
                )
                .doOnError((e) -> logger.errorWithParams("Failed to publish message", e))
                .subscribe();


        // ---------- Consume messages ----------
        ConsumerSettings consumerSettings = new ConsumerSettings()
                .withRetryCount(ConsumerSettings.RETRY_FOREVER)
                .withNumChannels(1)
                .withPreFetchCount(1024);

        Observable<Message> consumer =
                new DefaultConsumerFactory(channelFactory, consumerSettings)
                        .createConsumer(queue);

        final AtomicInteger consumedMessages = new AtomicInteger(0);
        Subscription consumeSubscription = //save the Subscription so you can stop consuming later
                consumer
                        .doOnNext(message -> consumedMessages.incrementAndGet())
                        .doOnNext(message -> message.acknowledger.ack()).subscribe(); //make sure to acknowledge the messages
        try {
            long startTime = System.currentTimeMillis();
            while (consumedMessages.get() < numMessages) {
                Thread.sleep(10);
                if ((System.currentTimeMillis() - startTime) > maxWaitMillis) {
                    throw new RuntimeException("Did not receive all '"+numMessages+"' messages within '"+maxWaitMillis+"' millis.");
                }
            }
        }finally {
            consumeSubscription.unsubscribe(); //unsubscribe (closes channel, plus connection if there are no other consumers)
            publisher.close(); //closes channel, plus connection of there are no other publishers
        }

        return consumedMessages.get();
    }

}
