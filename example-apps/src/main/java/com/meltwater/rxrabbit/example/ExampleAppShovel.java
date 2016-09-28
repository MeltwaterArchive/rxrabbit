package com.meltwater.rxrabbit.example;

import com.meltwater.rxrabbit.BrokerAddresses;
import com.meltwater.rxrabbit.ChannelFactory;
import com.meltwater.rxrabbit.ConnectionSettings;
import com.meltwater.rxrabbit.ConsumerFactory;
import com.meltwater.rxrabbit.ConsumerSettings;
import com.meltwater.rxrabbit.DefaultConsumerFactory;
import com.meltwater.rxrabbit.DefaultPublisherFactory;
import com.meltwater.rxrabbit.Exchange;
import com.meltwater.rxrabbit.Message;
import com.meltwater.rxrabbit.Payload;
import com.meltwater.rxrabbit.PublisherFactory;
import com.meltwater.rxrabbit.PublisherSettings;
import com.meltwater.rxrabbit.RabbitPublisher;
import com.meltwater.rxrabbit.RoutingKey;
import com.meltwater.rxrabbit.impl.DefaultChannelFactory;
import com.meltwater.rxrabbit.util.Logger;
import rx.Observable;
import rx.Subscription;

import java.io.IOException;
import java.util.Properties;

/**
 * An example app which demonstrates both consuming and publishing using RxRabbit
 */
public class ExampleAppShovel {

    private static final Logger log = new Logger(ExampleAppShovel.class);

    public static void main(String[] args) throws IOException {
        Properties prop = new Properties();
        prop.load(ExampleAppShovel.class.getResourceAsStream("/example_app_shovel.properties"));
        prop.putAll(System.getProperties());

        //Create and start the app
        final ExampleAppShovel exampleAppShovel = new ExampleAppShovel(
                prop.getProperty("rabbit.input.queue"),
                new Exchange(prop.getProperty("rabbit.output.exchange")),
                new ConnectionSettings(),
                new BrokerAddresses(prop.getProperty("rabbit.broker.uris"))
        );

        exampleAppShovel.start();

        //On shutdown call stop
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.infoWithParams("Closing app ...");
                exampleAppShovel.stop();            }
        });

        //Wait for Ctrl+C
        while (true){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }


    private final Observable<Message> messages;
    private volatile Subscription subscription;

    public ExampleAppShovel(final String inputQueue,
                            final Exchange outputExchange,
                            final ConnectionSettings connectionSettings,
                            final BrokerAddresses addresses) {

        ChannelFactory channelFactory = new DefaultChannelFactory(addresses,connectionSettings);
        PublisherSettings settings1 = new PublisherSettings();
        PublisherFactory publisherFactory = new DefaultPublisherFactory(channelFactory, settings1); //TODO publisher settings in the file
        ConsumerSettings settings = new ConsumerSettings();
        settings.withConsumerTagPrefix("");
        ConsumerFactory consumerFactory = new DefaultConsumerFactory(channelFactory, settings); //TODO publisher settings in the file

        final RabbitPublisher publisher =  publisherFactory.createPublisher();

        messages = consumerFactory
                .createConsumer(inputQueue)
                .doOnNext(message -> handleMessage(outputExchange, publisher, message))
                .doOnCompleted(() -> {
                    try {
                        publisher.close();
                    } catch (IOException ignored) {}
                })
                .doOnError(e -> {
                    log.errorWithParams("Fatal error encountered. Closing down application.", e);
                    System.exit(1);
                });
    }

    private void handleMessage(Exchange outputExchange, RabbitPublisher publisher, Message message) {
        try {
            //change in logback.xml to DEBUG level to see every message payload logged
            log.debugWithParams("Received message.",
                    "payload", new String(message.payload),
                    "metadata", message.basicProperties);
            publisher.call(
                    outputExchange,
                    new RoutingKey(message.envelope.getRoutingKey()),
                    message.basicProperties,
                    new Payload(message.payload)
            )
                    .doOnSuccess(ignore -> message.acknowledger.ack())
                    .doOnError(throwable -> message.acknowledger.reject())
                    .subscribe();
        }catch (Exception e){
            message.acknowledger.reject();
        }
    }

    void start(){
        if(subscription==null) {
            subscription = messages.subscribe();
        }
    }

    void stop(){
        if(subscription!=null && !subscription.isUnsubscribed()){
            subscription.unsubscribe();
        }
    }

}

