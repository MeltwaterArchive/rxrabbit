package com.meltwater.rxrabbit.example;

import com.google.common.base.Charsets;
import com.meltwater.rxrabbit.BrokerAddresses;
import com.meltwater.rxrabbit.ChannelFactory;
import com.meltwater.rxrabbit.ConnectionSettings;
import com.meltwater.rxrabbit.DefaultPublisherFactory;
import com.meltwater.rxrabbit.DeliveryMode;
import com.meltwater.rxrabbit.Exchange;
import com.meltwater.rxrabbit.Payload;
import com.meltwater.rxrabbit.PublisherFactory;
import com.meltwater.rxrabbit.PublisherSettings;
import com.meltwater.rxrabbit.RabbitPublisher;
import com.meltwater.rxrabbit.RoutingKey;
import com.meltwater.rxrabbit.impl.DefaultChannelFactory;
import com.meltwater.rxrabbit.util.Logger;
import com.rabbitmq.client.AMQP;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static rx.Observable.from;

public class LoadGenerator {

    private static final Logger log = new Logger(LoadGenerator.class);

    public static void main(String[] args) throws IOException {
        Properties prop = new Properties();
        prop.load(LoadGenerator.class.getResourceAsStream("/load_generator.properties"));
        prop.putAll(System.getProperties());

        publishTestMessages(
                new BrokerAddresses(prop.getProperty("rabbit.broker.uris")),
                prop.getProperty("publish.to.exchange"),
                Long.parseLong(prop.getProperty("publish.message.count")));
    }

    public static void publishTestMessages(BrokerAddresses addresses, String outputExchange, long nrToPublish) throws IOException {
        ConnectionSettings connectionSettings = new ConnectionSettings();
        PublisherSettings publisherSettings = new PublisherSettings();

        ChannelFactory channelFactory = new DefaultChannelFactory(addresses, connectionSettings);
        PublisherFactory publisherFactory = new DefaultPublisherFactory(channelFactory, publisherSettings);
        final RabbitPublisher publish = publisherFactory.createPublisher();

        List<Long> ids = new ArrayList<>();
        for (long i = 1; i<=nrToPublish; i++){
            ids.add(i);
        }

        log.infoWithParams("Publishing messages to exchange.",
                "numToPublish", nrToPublish,
                "exchange", outputExchange);

        from(ids)
                .flatMap( id -> {
                    AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties().builder();
                    builder.messageId(String.valueOf(id));
                    builder.deliveryMode(DeliveryMode.persistent.code);
                    builder.appId("load-generator");
                    String msgPayload = "Message nr " + id;
                    return publish.call(
                            new Exchange(outputExchange),
                            new RoutingKey("routing.key"),
                            builder.build(),
                            new Payload(msgPayload.getBytes(Charsets.UTF_8)))
                            .toObservable();
                })
                .doOnError(throwable -> log.errorWithParams("Unexpected error when publishing.", throwable))
                .timeout(30, TimeUnit.SECONDS)
                .toBlocking()
                .last();

        log.infoWithParams("All messages sent to exchange.",
                "numSent", nrToPublish,
                "exchange", outputExchange);

        publish.close();
    }

}
