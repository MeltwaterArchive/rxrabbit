package com.meltwater.rxrabbit;

import com.meltwater.rxrabbit.docker.DockerContainers;
import com.meltwater.rxrabbit.impl.DefaultChannelFactory;
import com.meltwater.rxrabbit.util.ConstantBackoffAlgorithm;
import com.meltwater.rxrabbit.util.Logger;
import com.meltwater.rxrabbit.util.TakeAndAckTransformer;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import com.rabbitmq.client.AMQP;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import rx.Observable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static com.meltwater.rxrabbit.RabbitTestUtils.createConsumer;
import static com.meltwater.rxrabbit.RabbitTestUtils.createQueues;
import static com.meltwater.rxrabbit.RabbitTestUtils.realm;
import static com.meltwater.rxrabbit.RabbitTestUtils.waitForAllConnectionsToClose;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class RxRabbitMultiNodeTest {

    private final int TIMEOUT = 100_000;

    private static final Logger log = new Logger(RxRabbitTests.class);

    private final static String RABBIT_1 = "rabbit1";
    private final static String RABBIT_2 = "rabbit2";

    private static final String inputQueue = "test-queue";
    private static final String inputExchange = "test-exchange";

    private static DockerContainers dockerContainers = new DockerContainers("docker-compose-multi-broker.yml",RxRabbitTests.class);

    private static String rabbitAdminPort;
    private static AsyncHttpClient httpClient;

    private static ConnectionSettings connectionSettings;
    private static PublisherSettings  publishSettings;

    private static ConsumerSettings consumeSettings;
    private static DefaultChannelFactory channelFactory;
    private static DefaultConsumerFactory consumerFactory;
    private static RabbitPublisher publisher;

    @Rule
    public RepeatRule repeatRule = new RepeatRule();

    @Rule
    public Timeout globalTimeout = Timeout.builder()
            .withTimeout(TIMEOUT, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true).build();

    @BeforeClass
    public static void setupSpec() throws Exception {

        dockerContainers.build();
        Map<String,String> clientProps = new HashMap<>();
        clientProps.put("app_id", RxRabbitTests.class.getName());

        connectionSettings = new ConnectionSettings()
                .withClientProperties(clientProps)
                .withHeartbeatSecs(1)
                .withConnectionTimeoutMillis(500)
                .withShutdownTimeoutMillis(5_000);

        publishSettings = new PublisherSettings()
                .withPublisherConfirms(true)
                .withPublishTimeoutSecs(20)
                .withNumChannels(1)
                .withBackoffAlgorithm(new ConstantBackoffAlgorithm(100))
                .withRetryCount(4)
                .withCloseTimeoutMillis(5_000);

        int prefetchCount = 10;
        consumeSettings = new ConsumerSettings()
                .withPreFetchCount(prefetchCount)
                .withNumChannels(1)
                .withRetryCount(-1)
                .withBackoffAlgorithm(new ConstantBackoffAlgorithm(100))
                .withCloseTimeoutMillis(5_000);

    }


    @AfterClass
    public static void teardownSpec() throws Exception {
        dockerContainers.cleanup();
    }

    final SortedSet<Integer> messagesSeen = Collections.synchronizedSortedSet(new TreeSet<>());

    @Before
    public void setup() throws Exception {
        dockerContainers.resetAll(false);
        dockerContainers.rabbit(RABBIT_1).assertUp();
        dockerContainers.rabbit(RABBIT_2).assertUp();

        String rabbitTcpPort = dockerContainers.rabbit(RABBIT_1).tcpPort();
        rabbitAdminPort = dockerContainers.rabbit(RABBIT_1).adminPort();
        String rabbit2TcpPort = dockerContainers.rabbit(RABBIT_2).tcpPort();
        log.infoWithParams("****** Rabbit brokers are up and running *****");

        BrokerAddresses addresses = new BrokerAddresses(
                "amqp://localhost:" + rabbitTcpPort + "," +
                        "amqp://localhost:" + rabbit2TcpPort
        );

        channelFactory = new DefaultChannelFactory(addresses, connectionSettings);
        consumerFactory = new DefaultConsumerFactory(channelFactory, consumeSettings);
        DefaultPublisherFactory publisherFactory = new DefaultPublisherFactory(channelFactory, publishSettings);
        httpClient = new AsyncHttpClient();

        messagesSeen.clear();
        createQueues(channelFactory, inputQueue, new Exchange(inputExchange));
        publisher = publisherFactory.createPublisher();
    }

    @After
    public void teardown() throws Exception {
        publisher.close();
        messagesSeen.clear();
        waitForAllConnectionsToClose(channelFactory, dockerContainers);
    }

    @Test
    public void handles_broker_failover_with_ha_queues() throws Exception{
        int nrMessages = 1_00;

        log.infoWithParams("Setting ha-mode to 'all' for queues");
        Response setPolicyResponse = httpClient.preparePut("http://localhost:" + rabbitAdminPort +
                "/api/policies/%2f/ha-all")
                .setBody("{\"pattern\":\"(.*?)\", \"definition\":{\"ha-mode\":\"all\"}}")
                .setRealm(realm)
                .setHeader("Content-Type", "application/json")
                .execute().get();

        assertThat(setPolicyResponse.getStatusCode(), Matchers.is(204));

        log.infoWithParams("Sending messages");
        SortedSet<Integer> sent = sendNMessages(nrMessages, publisher);
        log.infoWithParams("Killing the primary rabbit broker");
        dockerContainers.rabbit(RABBIT_1).kill();
        SortedSet<Integer> received = consumeAndGetIds(nrMessages, createConsumer(consumerFactory, inputQueue));
        assertThat(received.size(), equalTo(nrMessages));
        assertEquals(received, sent);
    }


    public SortedSet<Integer> sendNMessages(int numMessages, final RabbitPublisher publisher) throws Exception {
        final SortedSet<Integer> out = new TreeSet<>(
                sendNMessagesAsync(numMessages, 0, publisher)
                        .map(msg -> msg.id)
                        .toList()
                        .toBlocking()
                        .last());
        log.infoWithParams("Successfully sent messages to rabbit", "numMessages", numMessages);
        return out;
    }


    private Observable<RxRabbitTests.PublishedMessage> sendNMessagesAsync(int numMessages, int offset, RabbitPublisher publisher) {
        final List<Observable<RxRabbitTests.PublishedMessage>> sendCallbacks = new ArrayList<>();
        log.infoWithParams("Scheduling messages to rabbit", "numMessages", numMessages);
        for (int it = 1 ; it<=numMessages; it++) {
            final int id = it+offset;
            String messageId = String.valueOf(it);
            sendCallbacks.add(
                    publisher.call(
                            new Exchange(inputExchange),
                            new RoutingKey("routing"),
                            new AMQP.BasicProperties.Builder()
                                    .appId("send-messages")
                                    .messageId(messageId)
                                    .deliveryMode(DeliveryMode.persistent.code)
                                    .headers(new HashMap<>())
                                    .build(),
                            new Payload(messageId.getBytes()))
                            .map(aVoid -> new RxRabbitTests.PublishedMessage(id, false))
                            .onErrorReturn(throwable -> {
                                log.errorWithParams("Failed message.", throwable);
                                return new RxRabbitTests.PublishedMessage(id, true);
                            })
                            .toObservable());
        }
        return Observable.merge(sendCallbacks);
    }

    private TreeSet<Integer> consumeAndGetIds(int nrMessages, Observable<Message> consumer) {
        return new TreeSet<>(consumer
                .compose(getIdsTransformer(nrMessages))
                .toBlocking()
                .last());
    }

    private Observable.Transformer<Message,List<Integer>> getIdsTransformer(int nrMessages){
        return input -> input.
                compose(new TakeAndAckTransformer(nrMessages, TIMEOUT/10*9))
                .doOnNext(message -> log.debugWithParams("Got message", "id",message.basicProperties.getMessageId()))
                .map(RxRabbitTests::msgToInteger)
                .distinct()
                .toList();
    }


}
