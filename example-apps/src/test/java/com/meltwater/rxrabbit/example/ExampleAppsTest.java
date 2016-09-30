package com.meltwater.rxrabbit.example;


import com.meltwater.rxrabbit.BrokerAddresses;
import com.meltwater.rxrabbit.ConnectionSettings;
import com.meltwater.rxrabbit.ConsumerFactory;
import com.meltwater.rxrabbit.ConsumerSettings;
import com.meltwater.rxrabbit.DefaultConsumerFactory;
import com.meltwater.rxrabbit.Exchange;
import com.meltwater.rxrabbit.RabbitTestUtils;
import com.meltwater.rxrabbit.docker.DockerContainers;
import com.meltwater.rxrabbit.impl.DefaultChannelFactory;
import com.meltwater.rxrabbit.util.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ExampleAppsTest {

    private static final Logger log = new Logger(ExampleAppsTest.class);
    private static final DockerContainers dockerContainers = new DockerContainers(ExampleAppsTest.class);

    static BrokerAddresses addresses;
    static DefaultChannelFactory channelFactory;
    static Properties prop;

    @Before
    public void setup() throws Exception {
        killAndRestartRabbitDocker();

        prop = new Properties();
        prop.load(ExampleAppsTest.class.getResourceAsStream("/test.properties"));
        prop.putAll(System.getProperties());

        addresses = new BrokerAddresses(prop.getProperty("rabbit.broker.uris"));
        channelFactory = new DefaultChannelFactory(addresses, new ConnectionSettings());
        RabbitTestUtils.createQueues(channelFactory, prop.getProperty("in.queue"), new Exchange(prop.getProperty("in.exchange")));
        RabbitTestUtils.createQueues(channelFactory, prop.getProperty("out.queue"), new Exchange(prop.getProperty("out.exchange")));
    }

    @AfterClass
    public static void teardownSpec(){
        dockerContainers.cleanup();
    }

    @After
    public void cleanUp() throws InterruptedException {
        RabbitTestUtils.waitForAllConnectionsToClose(channelFactory, dockerContainers);
    }

    @Test
    public void can_consume_and_publish() throws InterruptedException, IOException {
        BrokerAddresses broker = new BrokerAddresses(prop.getProperty("rabbit.broker.uris"));
        final ExampleAppShovel exampleApp = new ExampleAppShovel(
                prop.getProperty("in.queue"),
                new Exchange(prop.getProperty("out.exchange")),
                new ConnectionSettings(),
                broker
        );
        try{
            exampleApp.start();
            int nrToPublish = 5_000;
            LoadGenerator.publishTestMessages(broker, prop.getProperty("in.exchange"), nrToPublish);

            Set<String> idSet = consumeDocumentsAndGetIds(nrToPublish);
            assertThat(idSet.size(), is(nrToPublish));
        } finally {
            exampleApp.stop();
        }
    }

    private Set<String> consumeDocumentsAndGetIds(int nrToPublish) throws InterruptedException {
        final SortedSet<String> out = Collections.synchronizedSortedSet(new TreeSet<String>());
        ConsumerFactory consumerFactory = new DefaultConsumerFactory(channelFactory, new ConsumerSettings());

        consumerFactory.createConsumer(prop.getProperty("out.queue"))
                .doOnNext(message -> message.acknowledger.ack())
                .doOnNext(message -> out.add(message.basicProperties.getMessageId()))
                .take(nrToPublish)
                .timeout(100, TimeUnit.SECONDS)
                .toBlocking()
                .last();

        return out;
    }

    private static void killAndRestartRabbitDocker() throws Exception {
        dockerContainers.resetAll(false);
        dockerContainers.rabbit().assertUp();
        String rabbitTcpPort = dockerContainers.rabbit().tcpPort();
        System.setProperty("rabbit.broker.uris", "amqp://guest:guest@localhost:" + rabbitTcpPort);
    }

}