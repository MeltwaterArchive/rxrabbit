package com.meltwater.rxrabbit.example;


import com.meltwater.rxrabbit.AdminChannel;
import com.meltwater.rxrabbit.BrokerAddresses;
import com.meltwater.rxrabbit.ChannelFactory;
import com.meltwater.rxrabbit.ConnectionSettings;
import com.meltwater.rxrabbit.ConsumerFactory;
import com.meltwater.rxrabbit.ConsumerSettings;
import com.meltwater.rxrabbit.DefaultConsumerFactory;
import com.meltwater.rxrabbit.Exchange;
import com.meltwater.rxrabbit.Message;
import com.meltwater.rxrabbit.docker.DockerContainers;
import com.meltwater.rxrabbit.impl.DefaultChannelFactory;
import com.meltwater.rxrabbit.util.Logger;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    private static final int CONNECTION_BACKOFF_TIME = 500;
    private static final int CONNECTION_MAX_ATTEMPT = 20;

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
        createQueues(channelFactory,prop.getProperty("in.queue"),new Exchange(prop.getProperty("in.exchange")));
        createQueues(channelFactory,prop.getProperty("out.queue"),new Exchange(prop.getProperty("out.exchange")));
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
        } while (openConnections.size()>0 && attempts< CONNECTION_MAX_ATTEMPT);
        log.infoWithParams("Checked open connections", "connections", openConnections);
        assertThat(openConnections.size(), Matchers.is(0));
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
        testContainers.resetAll(false);
        testContainers.rabbit().assertUp();
        String rabbitTcpPort = testContainers.rabbit().tcpPort();
        System.setProperty("rabbit.broker.uris", "amqp://guest:guest@localhost:" + rabbitTcpPort);
    }

    public static void createQueues(ChannelFactory channelFactory, String inputQueue, Exchange inputExchange) throws Exception {
        AdminChannel adminChannel;
        for (int i = 1; i <= CONNECTION_MAX_ATTEMPT; i++) {
            try {
                adminChannel = channelFactory.createAdminChannel();
                adminChannel.queueDelete(inputQueue, false, false);
                declareQueueAndExchange(adminChannel, inputQueue, inputExchange);
                adminChannel.closeWithError();
                break;
            } catch (Exception ignored) {
                log.infoWithParams("Failed to create connection.. will try again ", "attempt", i, "max-attempts", CONNECTION_MAX_ATTEMPT);
                Thread.sleep(CONNECTION_BACKOFF_TIME);
                if(i==CONNECTION_MAX_ATTEMPT) throw ignored;
            }
        }
    }


    public static void declareQueueAndExchange(AdminChannel sendChannel, String inputQueue, Exchange inputExchange) throws IOException {
        sendChannel.exchangeDeclare(inputExchange.name, "topic", true, false, false, new HashMap<>());
        declareAndBindQueue(sendChannel, inputQueue, inputExchange);
    }

    public static void declareAndBindQueue(AdminChannel sendChannel, String inputQueue, Exchange inputExchange) throws IOException {
        sendChannel.queueDeclare(inputQueue, true, false, false, new HashMap<>());
        sendChannel.queueBind(inputQueue, inputExchange.name, "#", new HashMap<>());
    }
}