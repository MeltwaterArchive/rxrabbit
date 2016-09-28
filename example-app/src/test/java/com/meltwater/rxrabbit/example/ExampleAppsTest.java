package com.meltwater.rxrabbit.example;


import com.meltwater.rxrabbit.BrokerAddresses;
import com.meltwater.rxrabbit.ConnectionSettings;
import com.meltwater.rxrabbit.ConsumerFactory;
import com.meltwater.rxrabbit.ConsumerSettings;
import com.meltwater.rxrabbit.DefaultConsumerFactory;
import com.meltwater.rxrabbit.Exchange;
import com.meltwater.rxrabbit.docker.DockerContainers;
import com.meltwater.rxrabbit.impl.DefaultChannelFactory;
import com.meltwater.rxrabbit.util.Logger;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ExampleAppsTest {

    private static final Logger log = new Logger(ExampleAppsTest.class);
    private static final DockerContainers testContainers = new DockerContainers(ExampleAppsTest.class);
    private static final  String outputQueue = "test-out";

    static BrokerAddresses addresses;
    static DefaultChannelFactory channelFactory;
    static Properties prop;

    @Before
    public void setup() throws Exception {
        killAndRestartRabbitDocker();

        prop = new Properties();
        prop.load(ExampleAppShovel.class.getResourceAsStream("/example_app_shovel.properties"));
        prop.load(LoadGenerator.class.getResourceAsStream("/load_generator.properties"));
        prop.putAll(System.getProperties());

        addresses = new BrokerAddresses(prop.getProperty("rabbit.broker.uris"));
        channelFactory = new DefaultChannelFactory(addresses, new ConnectionSettings());
    }

    @AfterClass
    public static void teardownSpec(){
        testContainers.cleanup();
    }

    @After
    public void cleanUp() throws InterruptedException {
        List<DefaultChannelFactory.ConnectionInfo> openConnections;
        int attempts = 0;
        do {
            openConnections = channelFactory.getOpenConnections();
            for (DefaultChannelFactory.ConnectionInfo connection : openConnections) {
                log.errorWithParams("Found open connection", "connection", connection);
                Thread.sleep(500);
            }
            attempts++;
        } while (openConnections.size()>0 && attempts<4);
        log.infoWithParams("Checked open connections", "connections", openConnections);
        assertThat(openConnections.size(), Matchers.is(0));
    }

    @Test
    public void can_consume_and_publish() throws InterruptedException, IOException {
        BrokerAddresses broker = new BrokerAddresses(prop.getProperty("rabbit.broker.uris"));
        final ExampleAppShovel exampleApp = new ExampleAppShovel(
                prop.getProperty("rabbit.input.queue"),
                new Exchange(prop.getProperty("rabbit.output.exchange")),
                new ConnectionSettings(),
                broker
        );
        exampleApp.start();

        int nrToPublish = 5_000;
        LoadGenerator.publishTestMessages(broker, prop.getProperty("publish.to.exchange"), nrToPublish);

        Set<String> idSet = consumeDocumentsAndGetIds(nrToPublish);
        assertThat(idSet.size(), is(nrToPublish));

        exampleApp.stop();
    }

    private Set<String> consumeDocumentsAndGetIds(int nrToPublish) throws InterruptedException {
        final SortedSet<String> out = Collections.synchronizedSortedSet(new TreeSet<String>());
        ConsumerFactory consumerFactory = new DefaultConsumerFactory(channelFactory, new ConsumerSettings());

        consumerFactory.createConsumer(outputQueue)
                .doOnNext(message -> message.acknowledger.ack())
                .doOnNext(message -> out.add(message.basicProperties.getMessageId()))
                .take(nrToPublish)
                .timeout(100, TimeUnit.SECONDS)
                .toBlocking()
                .last();

        return out;
    }

    private static void killAndRestartRabbitDocker() throws Exception {
        testContainers.resetAll(false);
        testContainers.rabbit().assertUp();
        String rabbitTcpPort = testContainers.rabbit().tcpPort();
        System.setProperty("rabbit.broker.uris", "amqp://guest:guest@localhost:" + rabbitTcpPort);
    }
}