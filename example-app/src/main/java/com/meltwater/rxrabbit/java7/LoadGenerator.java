package com.meltwater.rxrabbit.java7;

import com.google.common.base.Charsets;
import com.meltwater.rxrabbit.BrokerAddresses;
import com.meltwater.rxrabbit.ChannelFactory;
import com.meltwater.rxrabbit.ConnectionSettings;
import com.meltwater.rxrabbit.DefaultPublisherFactory;
import com.meltwater.rxrabbit.Exchange;
import com.meltwater.rxrabbit.Payload;
import com.meltwater.rxrabbit.PublisherFactory;
import com.meltwater.rxrabbit.PublisherSettings;
import com.meltwater.rxrabbit.RabbitPublisher;
import com.meltwater.rxrabbit.RoutingKey;
import com.meltwater.rxrabbit.impl.DefaultChannelFactory;
import com.meltwater.rxrabbit.util.Logger;
import com.rabbitmq.client.AMQP;
import rx.Observable;
import rx.Single;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static rx.Observable.from;

//TODO this should also have start- and stop methods
public class LoadGenerator {

    private static final Logger log = new Logger(LoadGenerator.class);

    public static void main(String[] args) throws IOException {
        if(args.length!=4) {
            LoadGenerator.printUsage();
            System.exit(1);
        }
        Properties prop = new Properties();
        prop.load(LoadGenerator.class.getResourceAsStream("/load_generator.properties"));
        prop.putAll(System.getProperties());


        ConnectionSettings connectionSettings = new ConnectionSettings();
        PublisherSettings publisherSettings = new PublisherSettings();

        BrokerAddresses addresses = new BrokerAddresses(prop.getProperty("rabbit.broker.uris"));

        ChannelFactory channelFactory = new DefaultChannelFactory(addresses, connectionSettings);
        PublisherFactory publisherFactory = new DefaultPublisherFactory(channelFactory, publisherSettings);


        final RabbitPublisher publish = publisherFactory.createPublisher();

        String outputExchange = prop.getProperty("rabbit.output.exchange");
        List<Long> ids = new ArrayList<>();
        long nrToPublish = Long.parseLong(prop.getProperty("publish.message.count"));
        for (long i = 1; i<=nrToPublish; i++){
            ids.add(i);
        }


        log.infoWithParams("Publishing messages to exchange.",
                "numToPublish", nrToPublish,
                "exchange", outputExchange);

        from(ids)
                .observeOn(Schedulers.computation())
                .flatMap((Func1<Long, Observable<?>>) id -> {
                    AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties().builder();
                    builder.messageId(String.valueOf(id));
                    String payloadMessage = "Message nr " + id;
                    return publish.call(
                            new Exchange(outputExchange),
                            new RoutingKey("#"), //TODO setting
                            builder.build(),
                            new Payload(payloadMessage.getBytes(Charsets.UTF_8)))
                            .toObservable();
                })
                .toBlocking()
                .last();

        log.infoWithParams("All quiddities sent to exchange.",
                "numSent", nrToPublish,
                "exchange", outputExchange);

        publish.close();
    }

    private static void printUsage(){
        System.out.println("Needs four parameters: Exchange, number of channels, number of messages to send and should bulk or not (true/false)");
        System.out.println("Example: test.in 1 100 true (Publish 100 messages to exchange test.in using 1 channel. Use bulk.");
    }

}
