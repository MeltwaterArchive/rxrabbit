package com.meltwater.rxrabbit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Collections2;
import com.meltwater.rxrabbit.docker.DockerContainers;
import com.meltwater.rxrabbit.example.ExampleCode;
import com.meltwater.rxrabbit.impl.DefaultChannelFactory;
import com.meltwater.rxrabbit.util.ConstantBackoffAlgorithm;
import com.meltwater.rxrabbit.util.Logger;
import com.meltwater.rxrabbit.util.TakeAndAckTransformer;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Realm;
import com.ning.http.client.Response;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConfirmListener;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class RxRabbitTests {

    private final int TIMEOUT = 100_000;

    @Rule
    public RepeatRule repeatRule = new RepeatRule();
    @Rule
    public Timeout globalTimeout= new Timeout(TIMEOUT, TimeUnit.MILLISECONDS);

    private static final Logger log = new Logger(RxRabbitTests.class);

    private static final int CONNECTION_BACKOFF_TIME = 500;
    private static final int CONNECTION_MAX_ATTEMPT = 20;
    private static final String inputQueue = "test-queue";
    private static final String inputExchange = "test-exchange";

    private static DockerContainers dockerContainers = new DockerContainers(RxRabbitTests.class);

    private static String rabbitTcpPort;
    private static String rabbitAdminPort;

    private static AsyncHttpClient httpClient;

    private static final Realm realm = new Realm.RealmBuilder()
            .setPrincipal("guest")
            .setPassword("guest")
            .setUsePreemptiveAuth(true)
            .setScheme(Realm.AuthScheme.BASIC)
            .build();
    private static DefaultPublisherFactory publisherFactory;
    private static ConnectionSettings connectionSettings;
    private static PublisherSettings  publishSettings;

    private static ConsumerSettings consumeSettings;
    private static DefaultChannelFactory channelFactory;
    private static DefaultConsumerFactory consumerFactory;
    private static RabbitPublisher publisher;
    private static int prefetchCount = 10;

    @BeforeClass
    public static void setupSpec() throws Exception {

        dockerContainers.resetAll(false);

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
                .withRetryCount(4)
                .withBackoffAlgorithm(new ConstantBackoffAlgorithm(100))
                .withCloseTimeoutMillis(5_000);

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
        dockerContainers.rabbit().assertUp();
        rabbitTcpPort = dockerContainers.rabbit().tcpPort();
        rabbitAdminPort = dockerContainers.rabbit().adminPort();
        log.infoWithParams("****** Rabbit broker is up and running *****");

        BrokerAddresses addresses = new BrokerAddresses("amqp://localhost:" + rabbitTcpPort);
        channelFactory = new DefaultChannelFactory(addresses, connectionSettings);
        consumerFactory = new DefaultConsumerFactory(channelFactory, consumeSettings);
        publisherFactory = new DefaultPublisherFactory(channelFactory, publishSettings);
        httpClient = new AsyncHttpClient();

        messagesSeen.clear();
        AdminChannel adminChannel;
        for (int i = 1; i <= CONNECTION_MAX_ATTEMPT; i++) {
            try {
                adminChannel = channelFactory.createAdminChannel();
                adminChannel.queueDelete(inputQueue, false, false);
                declareQueueAndExchange(adminChannel);
                adminChannel.closeWithError();
                break;
            } catch (Exception ignored) {
                log.infoWithParams("Failed to create connection.. will try again ", "attempt", i, "max-attempts", CONNECTION_MAX_ATTEMPT);
                Thread.sleep(CONNECTION_BACKOFF_TIME);
                if(i==CONNECTION_MAX_ATTEMPT) throw ignored;
            }
        }
        publisher = publisherFactory.createPublisher();
    }

    @After
    public void teardown() throws Exception {
        publisher.close();
        messagesSeen.clear();

        List<DefaultChannelFactory.ConnectionInfo> openConnections;
        int attempts = 0;
        do {
            openConnections = channelFactory.getOpenConnections();
            for (DefaultChannelFactory.ConnectionInfo connection : openConnections) {
                log.errorWithParams("Found open connection", "connection", connection);
                Thread.sleep(1000);
            }
            attempts++;
        } while (openConnections.size()>0 && attempts<10);
        log.infoWithParams("Checked open connections", "connections", openConnections);

        if (openConnections.size()>0){
            log.errorWithParams("There are open connections left on the broker. Restarting the broker to flush them out ...");
            dockerContainers.resetAll(false);
            log.infoWithParams("Broker successfully re-started");
        }

        assertThat("Too many open connections.", openConnections.size(), is(0));

    }

    //TODO tests to add
    //  'nice close' should wait for outstanding acks
    //  'nice close' should wait for publish confirms
    //  test overflow handling of publishes

    //TODO re-write some tests to use take(x)  instead they can perhaps be more readable that way

    @Test
    public void happy_path() throws Exception {
        int nrMessages = 500;
        SortedSet<Integer> sent = sendNMessages(nrMessages, publisher);
        SortedSet<Integer> received = consumeAndGetIds(nrMessages, createConsumer());
        assertThat(received.size(), equalTo(nrMessages));
        assertEquals(received, sent);
    }

    @Test
    public void test_example_code() throws Exception {
        int sentMessages = 1000;
        assertThat(new ExampleCode().publishAndConsume(
                sentMessages,
                20_000, "localhost",
                Integer.valueOf(rabbitTcpPort),
                inputQueue,
                inputExchange), is(sentMessages));
    }

    @Test
    public void ad_hoc_happy_path() throws Exception {
        int nrMessages = 500;
        AdminChannel adminChannel = channelFactory.createAdminChannel();
        adminChannel.queueDelete(inputQueue, false, false);
        adminChannel.closeWithError();
        consumerFactory
                .createConsumer(inputExchange, "#")
                .doOnNext(message -> {
                    log.infoWithParams("Received message.", "id", message.basicProperties.getMessageId());
                    messagesSeen.add(Integer.valueOf(message.basicProperties.getMessageId()));
                    message.acknowledger.ack();
                })
                .take(nrMessages)
                .subscribe();
        sendNMessages(nrMessages, publisher);
        waitForNMessages(nrMessages);
        assertEquals(messagesSeen.size(), nrMessages);
    }

    @Test
    public void removes_ad_hoc_queue_when_unsubscribing() throws Exception {
        Subscription s = consumerFactory
                .createConsumer(inputExchange, "#")
                .subscribe();
        waitForNumQueuesToBePresent(2);
        assertEquals(getQueueNames().size(), 2);
        s.unsubscribe();
        waitForNumQueuesToBePresent(1);
        assertEquals(getQueueNames().size(), 1);
    }

    @Test
    public void recreates_ad_hoc_queue_on_connection_drop() throws Exception {
        Subscription s = consumerFactory
                .createConsumer(inputExchange, "#")
                .doOnNext(message -> {
                    messagesSeen.add(Integer.valueOf(message.basicProperties.getMessageId()));
                    message.acknowledger.ack();
                })
                .subscribe();
        waitForNumQueuesToBePresent(2);
        assertEquals(getQueueNames().size(), 2);

        int nrMessages = 25_000;
        SortedSet<Integer> sent = sendNMessages(nrMessages, publisher);
        waitForNMessages(nrMessages / 2);

        List<String> connectionName = getConnectionNames();
        deleteConnections(connectionName);

        waitForNumQueuesToBePresent(2);
        assertEquals(getQueueNames().size(), 2);

        //NOTE we need to send all messages again because we can't be sure how many that got dropped while the queue was not present,
        // this is a limitation of the re-connect feature for exclusive, auto delete queues that we have to live with
        sendNMessages(nrMessages, publisher);

        waitForNMessages(nrMessages);
        s.unsubscribe();

        assertThat(messagesSeen.size(), equalTo(nrMessages));
        assertEquals(messagesSeen, sent);
    }

    @Test
    public void ad_hoc_consumer_retries_when_broker_unavailable() throws Exception {
        log.infoWithParams("Killing the rabbitMQ broker");
        dockerContainers.rabbit().kill();
        int nrMessages = 500;
        final AtomicBoolean done = new AtomicBoolean(false);
        final List<Integer> received = new ArrayList<>();
        consumerFactory
                .createConsumer(inputExchange, "#")
                .compose(getIdsTransformer(nrMessages))  //TODO use this pattern instead of wait for N..
                .doOnNext(integers -> {
                    received.addAll(integers);
                    done.set(true);
                })
                .subscribe();
        Thread.sleep(5_000); //wait 5 secs before starting the broker
        log.infoWithParams("Starting up the rabbitMQ broker");
        dockerContainers.rabbit().start().assertUp();
        waitForNumQueuesToBePresent(2);
        sendNMessages(nrMessages, publisher);
        while (!done.get()){
            Thread.sleep(10);
        }
        assertEquals(received.size(), nrMessages);
    }

    @Test
    public void handles_backpressure_on_consume() throws Exception {
        int nrMessages = 4_000;
        sendNMessages(nrMessages, publisher);
        DefaultConsumerFactory consumerFactory = new DefaultConsumerFactory(
                channelFactory,
                new ConsumerSettings().withPreFetchCount(2000).withNumChannels(1));
        final Observable<Message> consumer = consumerFactory.createConsumer(inputQueue);
        List<Integer> messages = consumer
                .observeOn(Schedulers.computation())
                .map(message -> {
                    try {
                        log.traceWithParams("Message", "id", message.basicProperties.getMessageId());
                        Thread.sleep(1);
                    } catch (InterruptedException ignored) {}
                    return message;
                })
                .compose(getIdsTransformer(nrMessages))
                .toBlocking()
                .last();
        assertThat(messages.size(), equalTo(nrMessages));
    }

    @Test
    public void consumer_recovers_from_queue_recreated() throws Exception {
        final AdminChannel adminChannel = channelFactory.createAdminChannel();
        int nrMessages = 1000;

        AtomicInteger receivedCount = new AtomicInteger();
        final Observable<Message> consumer = consumerFactory.createConsumer(inputQueue);
        //send one message so we can start consuming
        sendNMessages(1,publisher);
        int count = consumer
                .doOnNext(message -> message.acknowledger.ack())
                .map(RxRabbitTests::msgToInteger)
                .doOnNext(integer -> {
                    int currentCount = receivedCount.incrementAndGet();
                    if(currentCount == 1){
                        try{
                            log.infoWithParams("Deleting queue");
                            deleteQueue(inputQueue, adminChannel);
                            log.infoWithParams("Checking that queue was deleted");
                            String existingQueues = httpClient
                                    .prepareGet("http://localhost:" + rabbitAdminPort + "/api/queues/")
                                    .setRealm(realm)
                                    .execute().get().getResponseBody();
                            if (!existingQueues.equals("[]")) {
                                throw new RuntimeException("Queue was not deleted");
                            }
                            log.infoWithParams("Queue was successfully deleted. Re-creating queue");
                            declareAndBindQueue(adminChannel);
                            log.infoWithParams("Sending messages to queue");
                            sendNMessagesAsync(nrMessages,0,publisher).subscribe();
                        }catch(Exception e){
                            throw new RuntimeException(e);
                        }
                    }
                })
                .take(nrMessages + 1)
                .timeout(1,TimeUnit.MINUTES)
                .count()
                .doOnTerminate(adminChannel::close)
                .toBlocking().last();

        assertThat(count, equalTo(nrMessages + 1));
    }

    @Test
    public void can_handle_multiple_consumers() throws Exception {
        int nrMessages = 30_000;
        final Observable<Message> consumers = Observable.merge(createConsumer(), createConsumer(), createConsumer());

        AtomicInteger receivedCount = new AtomicInteger();
        final SortedSet<Integer> received = Collections.synchronizedSortedSet(new TreeSet<>());
        Subscription s = consumers
                .compose(new TakeAndAckTransformer(nrMessages*2, TIMEOUT))
                .map(RxRabbitTests::msgToInteger)
                .distinct()
                .doOnNext(integer -> {
                    receivedCount.incrementAndGet();
                    received.add(integer);
                })
                .subscribe();

        SortedSet<Integer> sent = sendNMessages(nrMessages, publisher);
        while (receivedCount.get()<nrMessages){
            Thread.sleep(10);
        }
        assertThat(received.size(), equalTo(nrMessages));
        assertEquals(received, sent);
        int nrConsumers = countConsumers();
        s.unsubscribe();
        assertThat(nrConsumers, equalTo(3));
        while (countConsumers()>0){
            Thread.sleep(50);
        }
    }

    @Test
    public void consumer_recovers_from_connection_shutdown() throws Exception {
        int nrMessages = 25_000;
        SortedSet<Integer> sent = sendNMessages(nrMessages, publisher);

        final Observable<Message> consumer = createConsumer();
        Subscription s = startConsuming(consumer);
        List<String> connectionName = getConnectionNames();

        waitForNMessages(nrMessages / 2);
        deleteConnections(connectionName);
        waitForNMessages(nrMessages);
        s.unsubscribe();

        assertThat(messagesSeen.size(), equalTo(nrMessages));
        assertEquals(messagesSeen, sent);
    }

    @Test
    public void mulitple_publishers_recover_from_connection_shutdown() throws Exception {
        RabbitPublisher publisher = publisherFactory.createPublisher();
        RabbitPublisher publisher2 = publisherFactory.createPublisher();
        RabbitPublisher publisher3 = publisherFactory.createPublisher();

        final int nrMessages = 30_000;
        List<Observable<PublishedMessage>> sent = new ArrayList<>();

        sent.add(sendNMessagesAsync(nrMessages, 0, publisher));
        sent.add(sendNMessagesAsync(nrMessages, nrMessages + 1, publisher2));
        sent.add(sendNMessagesAsync(nrMessages, nrMessages * 2 + 1, publisher3));
        Observable<PublishedMessage> merge = Observable.merge(sent);

        final Semaphore ugly = new Semaphore(0);
        final List<PublishedMessage> res = new ArrayList<>();
        merge
                .subscribe(new Subscriber<PublishedMessage>() {
                    @Override
                    public void onCompleted() {
                        ugly.release();
                    }
                    @Override
                    public void onError(Throwable e) {
                        log.errorWithParams("got error", e);
                    }
                    @Override
                    public void onNext(PublishedMessage m) {
                        res.add(m);
                        if (res.size() == nrMessages) {
                            try {
                                log.infoWithParams("Closing connection");
                                List<String> connectionNames = getConnectionNames();
                                log.infoWithParams("Nr connections", "is", connectionNames.size());
                                deleteConnections(connectionNames);
                            } catch (Exception e) {
                                log.infoWithParams("Got exception, THIS SHOULD NEVER HAPPEN", e);
                            }
                        }
                    }
                });

        log.infoWithParams("Waiting for all publish confirms");
        ugly.acquire();
        publisher.close();
        publisher2.close();
        publisher3.close();
        AdminChannel channel = channelFactory.createAdminChannel();
        deleteQueue(inputQueue, channel);
        declareAndBindQueue(channel);
        channel.close();

        assertThat(res.size(), equalTo(nrMessages * 3));

        final List<PublishedMessage> fails = new ArrayList<>(Collections2.filter(res, input -> input.failed));
        assertThat(fails.size(), equalTo(0));
    }


    @Test
    public void publisher_retries_when_broker_unavailable() throws Exception {
        int nrMessages = 1_000;
        final Observable<Message> consumer = createConsumer();
        Subscription s = startConsuming(consumer);
        Observable<PublishedMessage> sent = sendNMessagesAsync(nrMessages, 0, publisher);

        log.infoWithParams("Killing the rabbitMQ broker");
        dockerContainers.rabbit().kill();
        log.infoWithParams("Starting up the rabbitMQ broker");
        dockerContainers.up();
        List<PublishedMessage> sentMsgs = sent.toList().toBlocking().last();

        waitForNMessages(nrMessages);
        s.unsubscribe();
        assertEquals(messagesSeen, new TreeSet<>(Collections2.transform(sentMsgs, input -> input.id)));
        assertThat(messagesSeen.size(), equalTo(nrMessages));
    }


    @Test
    public void publisher_retries_max_ntimes() throws Exception {
        int nrMessages = 1;
        Observable<PublishedMessage> sent = sendNMessagesAsync(nrMessages, 0, publisher);

        log.infoWithParams("Killing the rabbitMQ broker");
        dockerContainers.rabbit().kill();

        PublishedMessage sentMsgs = sent.toList().toBlocking().last().get(0);
        assertTrue(sentMsgs.failed);

        dockerContainers.up();
    }

    @Test
    public void consumer_retries_max_ntimes_to_start() throws Exception {
        log.infoWithParams("Killing the rabbitMQ broker");
        dockerContainers.rabbit().kill();

        DefaultConsumerFactory consumerFactory = new DefaultConsumerFactory(channelFactory, new ConsumerSettings().withRetryCount(2));
        final List<Message> consumed = consumerFactory.createConsumer(inputQueue).onErrorResumeNext(Observable.empty()).toList().timeout(1, MINUTES).toBlocking().last();
        assertTrue(consumed.isEmpty());

        dockerContainers.up();
    }


    @Test
    public void consumer_retries_max_ntimes_when_connection_lost() throws Exception {
        int nrMessages = 10_000;
        sendNMessagesAsync(nrMessages, 0, publisher).toList().toBlocking().last();

        final Observable<Message> consumer = createConsumer();
        final Subscription subscription = startConsuming(consumer);

        waitForNMessages(1);
        log.infoWithParams("Killing the rabbitMQ broker");
        dockerContainers.rabbit().kill();

        assertThat(messagesSeen.size(), lessThan(nrMessages));

        dockerContainers.up();
        Thread.sleep(1000);
        subscription.unsubscribe();
    }

    @Test
    public void ignores_acks_when_connection_is_down() throws Exception {
        int nrMessages = 1_000;
        sendNMessagesAsync(nrMessages, 0, publisher).toList().toBlocking().last();

        final Observable<Message> consumer = createConsumer();
        final Set<Integer> uniqueMessages = new HashSet<>();
        final List<Message> seenMessages = new ArrayList<>();

        final Subscription subscribe = consumer
                .doOnNext(message -> {
                    log.traceWithParams("Got message", "basicProperties", message.basicProperties);
                    synchronized (seenMessages) {
                        seenMessages.add(message);
                        uniqueMessages.add(Integer.valueOf(message.basicProperties.getMessageId()));
                        if (seenMessages.size() == prefetchCount) {
                            log.infoWithParams("Killing the rabbitMQ broker");
                            try {
                                dockerContainers.rabbit().kill();
                                log.infoWithParams("Acking messages when broker is down");
                                for (Message m: seenMessages){
                                    if (new Random().nextBoolean()) {
                                        m.acknowledger.ack();
                                    } else {
                                        m.acknowledger.reject();
                                    }
                                }
                                dockerContainers.up();
                            } catch (Exception e) {
                                log.errorWithParams("TODO this should NEVER happen. (but it can :()", e);
                            }
                        } else if (seenMessages.size() > prefetchCount) {
                            message.acknowledger.ack();
                        }
                        seenMessages.notifyAll();
                    }
                })
                .subscribeOn(Schedulers.io())
                .onErrorResumeNext(throwable -> {
                    log.errorWithParams("Error", throwable);
                    return Observable.empty();
                })
                .subscribe();



        while(uniqueMessages.size() < nrMessages){ //TODO timeout?
            synchronized (seenMessages){
                seenMessages.wait(100);
            }
        }

        subscribe.unsubscribe();
        assertThat(uniqueMessages.size(), is(nrMessages));
    }

    @Test
    public void can_handle_publish_confirms_after_connection_error() throws IOException {
        ChannelFactory proxyChannelFactory = getDroppingAndExceptionThrowingChannelFactory(1, 2);

        final PublisherSettings proxyPublishSettings = new PublisherSettings().withNumChannels(1).withPublisherConfirms(true).withRetryCount(5).withPublishTimeoutSecs(1);
        DefaultPublisherFactory proxyPublishFactory = new DefaultPublisherFactory(proxyChannelFactory,proxyPublishSettings);
        RabbitPublisher publisher = proxyPublishFactory.createPublisher();
        final List<PublishedMessage> res = sendNMessagesAsync(3, 0, publisher)
                .take(3)
                .timeout(30, TimeUnit.SECONDS)
                .toList()
                .toBlocking()
                .last();
        for (PublishedMessage re : res) {
            assertFalse(re.failed);
        }

        final List<Message> consumeRes = consumerFactory.createConsumer(inputQueue)
                .doOnNext(m -> m.acknowledger.ack())
                .take(3)
                .timeout(10, TimeUnit.SECONDS)
                .toList()
                .toBlocking()
                .last();

        assertThat(consumeRes.size(), is(3));
        publisher.close();
    }


    @Test
    public void if_not_using_publish_confirms_messages_can_be_lost() throws IOException {
        ChannelFactory proxyChannelFactory = getDroppingAndExceptionThrowingChannelFactory(1, 2);

        final PublisherSettings proxyPublishSettings = new PublisherSettings().withNumChannels(1).withPublisherConfirms(false).withRetryCount(5);
        DefaultPublisherFactory proxyPublishFactory = new DefaultPublisherFactory(proxyChannelFactory,proxyPublishSettings);
        RabbitPublisher publisher = proxyPublishFactory.createPublisher();
        final List<PublishedMessage> res = sendNMessagesAsync(3, 0, publisher)
                .take(3)
                .timeout(30, TimeUnit.SECONDS)
                .toList()
                .toBlocking()
                .last();
        for (PublishedMessage re : res) {
            assertFalse(re.failed);
        }

        final List<Message> consumeRes = consumerFactory.createConsumer(inputQueue)
                .doOnNext(m -> m.acknowledger.ack())
                .take(2)
                .timeout(10, TimeUnit.SECONDS)
                .toList()
                .toBlocking()
                .last();

        assertThat(consumeRes.size(), is(2));

        final List<Message> timeoutConsume = consumerFactory.createConsumer(inputQueue)
                .doOnNext(m -> m.acknowledger.ack())
                .take(1)
                .timeout(3, TimeUnit.SECONDS)
                .onErrorResumeNext(Observable.<Message>empty())
                .toList()
                .toBlocking()
                .last();

        assertTrue(timeoutConsume.isEmpty());

        publisher.close();
    }

    @Test
    @RepeatRule.Repeat( times = 3 )
    public void ignores_acks_on_messages_delivered_before_connection_reset() throws Exception {
        int nrMessages = 20;
        sendNMessagesAsync(nrMessages, 0, publisher).toBlocking().last();

        final Observable<Message> consumer = createConsumer();
        final Set<Integer> uniqueMessages = new TreeSet<>();
        final Set<Long> deliveryTags = new HashSet<>();
        final List<Message> seenMessages = new ArrayList<>();

        final Subscription subscribe = consumer
                .observeOn(Schedulers.io())
                .doOnNext(message -> {
                    log.traceWithParams("Got message", "basicProperties", message.basicProperties);
                    synchronized (seenMessages) {
                        seenMessages.add(message);
                        uniqueMessages.add(Integer.valueOf(message.basicProperties.getMessageId()));
                        deliveryTags.add(message.envelope.getDeliveryTag());
                        if (seenMessages.size() == prefetchCount) {
                            log.infoWithParams("Restarting the rabbitMQ broker");
                            try {
                                dockerContainers.rabbit().kill();
                                dockerContainers.up();
                                int connectionSize = 0;
                                while (connectionSize == 0) {
                                    try {
                                        connectionSize = getConnectionNames().size();
                                    } catch (Exception ignored) {
                                        log.infoWithParams("Waiting for connection to be visible to rabbit admin interface.");
                                        Thread.sleep(100);
                                    }
                                }
                                log.infoWithParams("Acking messages received before broker was restarted");
                                for(Message m:seenMessages){
                                    m.acknowledger.ack();
                                }
                            } catch (Exception e) {
                                log.errorWithParams("TODO this should NEVER happen. (but it can :( )", e);
                            }
                        } else if (seenMessages.size() > prefetchCount) {
                            message.acknowledger.ack();
                        }
                        seenMessages.notifyAll();
                    }
                })
                .subscribeOn(Schedulers.io())
                .onErrorResumeNext(throwable -> {
                    log.errorWithParams("Error", throwable);
                    return Observable.empty();
                })
                .subscribe();



        while(uniqueMessages.size() < nrMessages){ //TODO timeout?
            synchronized (seenMessages){
                seenMessages.wait(100);
            }
        }

        subscribe.unsubscribe();
        log.infoWithParams("delivery tags", "nr", deliveryTags.size(), "tags", deliveryTags);
        assertThat(uniqueMessages.size(), is(nrMessages));
        assertThat(deliveryTags.size(), is(nrMessages + prefetchCount));
    }

    @Test
    public void can_close_consumer_without_losing_messages() throws Exception {
        final int nrMessages = 1_000;
        SortedSet<Integer> sent = sendNMessages(nrMessages, publisher);

        Observable<Message> consumer = createConsumer();
        final Semaphore ugly = new Semaphore(0);
        final AtomicReference<Subscription> sub = new AtomicReference<>();
        sub.set(consumer
                .map(consumedMessage -> {
                    consumedMessage.acknowledger.ack();
                    return consumedMessage.basicProperties.getMessageId();
                })
                .subscribeOn(Schedulers.io())
                .subscribe(new Subscriber<String>() {
                    @Override
                    public void onCompleted() {
                        log.infoWithParams("completed");
                    }
                    @Override
                    public void onError(Throwable throwable) {
                        log.errorWithParams("onError", throwable);
                    }
                    @Override
                    public void onNext(String s) {
                        messagesSeen.add(Integer.valueOf(s));
                        if (messagesSeen.size() == 1) {
                            sub.get().unsubscribe();
                            ugly.release();
                        }

                    }
                }));
        assertThat(messagesSeen.size(), lessThan(nrMessages));
        log.infoWithParams("waiting for consumer to complete.");
        ugly.acquire();
        log.infoWithParams("Creating new consumer.");
        consumer = createConsumer();
        sub.set(consumer
                .map(consumedMessage -> {
                    consumedMessage.acknowledger.ack();
                    return consumedMessage.basicProperties.getMessageId();
                })
                .subscribeOn(Schedulers.computation())
                .subscribe(new Subscriber<String>() {
                    @Override
                    public void onCompleted() {
                        log.infoWithParams("completed");
                    }
                    @Override
                    public void onError(Throwable throwable) {
                        log.errorWithParams("onError", throwable);
                    }
                    @Override
                    public void onNext(String s) {
                        messagesSeen.add(Integer.valueOf(s));
                        if (messagesSeen.size() == nrMessages) {
                            sub.get().unsubscribe();
                            ugly.release();
                        }
                    }
                })
        );
        ugly.acquire();

        assertThat(messagesSeen.size(), equalTo(nrMessages));
        assertEquals(messagesSeen, sent);
    }

    @Test
    public void can_recover_from_broker_restart() throws Exception {
        int nrMessages = 10_000;
        SortedSet<Integer> sent = sendNMessages(nrMessages, publisher);

        final Observable<Message> consumer = createConsumer();
        final Subscription subscription = startConsuming(consumer);

        waitForNMessages(nrMessages / 2);
        log.infoWithParams("Killing the rabbitMQ broker");
        dockerContainers.rabbit().kill();
        log.infoWithParams("Starting up the rabbitMQ broker");
        dockerContainers.up();
        waitForNMessages(nrMessages);
        subscription.unsubscribe();

        assertThat(messagesSeen.size(), equalTo(nrMessages));
        assertEquals(messagesSeen, sent);

    }

    @Test
    public void can_subscribe_multiple_times_to_consumer() throws Exception {
        while (getConnectionNames().size()>0){
            log.infoWithParams("Waiting for all connections to clean up before starting the test.");
            Thread.sleep(100);
        }
        final Observable<Message> consumer = createConsumer();
        Subscription s1 = consumer.subscribe();
        Subscription s2 = consumer.subscribe();
        Subscription s3 = consumer.subscribe();
        Thread.sleep(5000);

        List<String> connectionNames = getConnectionNames();
        assertThat("More than one connections is present. "+connectionNames.toString(), connectionNames.size(), is(1));
        s1.unsubscribe();
        s2.unsubscribe();

        Thread.sleep(5000);
        connectionNames = getConnectionNames();
        assertThat("More than one connections is present. "+connectionNames.toString(), connectionNames.size(), is(1));


        s3.unsubscribe();
        Thread.sleep(5000);
        connectionNames = getConnectionNames();
        assertThat("Not all connections are closed. "+connectionNames.toString(), connectionNames.size(), is(0));
    }

    @Test
    public void can_not_unsubscribe_before_consumer_registered() throws Exception {
        final AtomicBoolean error = new AtomicBoolean(false);
        final AtomicBoolean completed = new AtomicBoolean(false);
        final Observable<Message> consumer = createConsumer();
        Subscription s = consumer
                .subscribeOn(Schedulers.io())
                .unsubscribeOn(Schedulers.trampoline())
                .doOnUnsubscribe(() -> completed.set(true))
                .doOnError((e) -> error.set(true))
                .subscribe();
        s.unsubscribe();
        while (!(completed.get() || error.get())){
            Thread.sleep(1);
        }

        assertFalse(error.get());
        assertTrue(completed.get());
    }

    private void deleteQueue(String inputQueue, AdminChannel testChannel) throws IOException {
        testChannel.queueDelete(inputQueue, false, false);
    }

    private void deleteConnections(List<String> connectionName) throws Exception {
        for(String name: connectionName){
            final Response deleteResponse = httpClient
                    .prepareDelete("http://localhost:" + rabbitAdminPort + "/api/connections/" + name)
                    .setRealm(realm)
                    .execute().get();
            //TODO we need something like this, but not safe to crash other threads than main assertThat(deleteResponse.getStatusCode(), equalTo(204));
        }
    }

    private int countConsumers() throws Exception {
        final Response response = httpClient
                .prepareGet("http://localhost:" + rabbitAdminPort + "/api/channels")
                .setRealm(realm)
                .execute().get();
        ObjectMapper mapper = new ObjectMapper();
        int consumers = 0;
        final List<Map<String,Object>> list = mapper.readValue(response.getResponseBody(), List.class);
        for(Map<String,Object> entry : list){
            consumers+= (Integer)entry.get("consumer_count");
        }
        return consumers;
    }

    private List<String> getQueueNames() throws Exception{
        final Response response = httpClient
                .prepareGet("http://localhost:" + rabbitAdminPort + "/api/queues")
                .setRealm(realm)
                .execute().get();
        ObjectMapper mapper = new ObjectMapper();
        List<String> queues = new ArrayList<>();
        final List<Map<String,Object>> list = mapper.readValue(response.getResponseBody(), List.class);
        for(Map<String,Object> entry : list){
            queues.add(entry.get("name").toString());
        }
        return queues;
    }

    //TODO add more metadata for the connections so we can identify them when they should not be there..
    private List<String> getConnectionNames() throws Exception {
        final Response response = httpClient
                .prepareGet("http://localhost:" + rabbitAdminPort + "/api/connections")
                .setRealm(realm)
                .execute().get();
        ObjectMapper mapper = new ObjectMapper();
        List<String> connections = new ArrayList<>();
        final List<Map<String,Object>> list = mapper.readValue(response.getResponseBody(), List.class);
        for(Map<String,Object> entry : list){
            connections.add("[ name="+entry.get("name")+", channels="+entry.get("channels")+", connected_at="+new DateTime(entry.get("connected_at"))+"]");
        }
        return connections;
    }

    private void waitForNumQueuesToBePresent(int numQueues) throws Exception {
        for (int i = 1; i <= CONNECTION_MAX_ATTEMPT; i++) {
            try {
                List<String> queueNames = getQueueNames();
                if (queueNames.size()==numQueues) {
                    log.infoWithParams("Correct number of queues found.", "numQueues", queueNames.size(), "expected", numQueues, "names", queueNames);
                    break;
                }else{
                    log.infoWithParams("Wrong number of queues found.", "numQueues", queueNames.size(), "expected", numQueues, "names", queueNames);
                    Thread.sleep(CONNECTION_BACKOFF_TIME);
                }
            } catch (Exception ignored) {
                log.infoWithParams("Failed to create connection.. will try again ", "attempt", i, "max-attempts", CONNECTION_MAX_ATTEMPT);
                Thread.sleep(CONNECTION_BACKOFF_TIME);
                if (i == CONNECTION_MAX_ATTEMPT) {
                    throw ignored;
                }
            }
        }
    }

    private void waitForNMessages(int nrMessages) {
        waitForNMessages(messagesSeen, nrMessages);
    }

    private void waitForNMessages(Collection collection, int nrMessages) {
        int i = 0;
        while (collection.size() < nrMessages) {
            synchronized (collection) {
                try {
                    collection.wait(10);
                }catch (InterruptedException ignored) {}
            }
            if(i%1000 == 0){
                log.infoWithParams("Waiting for messages", "waitingFor", nrMessages - collection.size());
            }
            i++;
        }
    }

    private Subscription startConsuming(Observable<Message> con) {
        log.infoWithParams("Starting up consumer");
        return con
                .doOnNext(message -> {
                    log.traceWithParams("Got message", "basicProperties", message.basicProperties);
                    synchronized (messagesSeen) {
                        messagesSeen.add(Integer.valueOf(message.basicProperties.getMessageId()));
                        messagesSeen.notifyAll();
                    }
                    message.acknowledger.ack();
                })
                .subscribeOn(Schedulers.io())
                .onErrorResumeNext(throwable -> {
                    log.errorWithParams("Error", throwable);
                    return rx.Observable.empty();
                })
                .subscribe();
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

    public rx.Observable<Message> createConsumer() throws InterruptedException {
        for (int i = 0; i < CONNECTION_MAX_ATTEMPT; i++) {
            try {
                return consumerFactory.createConsumer(inputQueue);
            } catch (Exception e) {
                log.errorWithParams("failed to create consumer", e);
                Thread.sleep(CONNECTION_BACKOFF_TIME);
            }

        }
        throw new RuntimeException("Failed to connect to Rabbit");
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

    public static Integer msgToInteger(Message message) {
        return Integer.valueOf(message.basicProperties.getMessageId());
    }

    private Observable<PublishedMessage> sendNMessagesAsync(int numMessages, int offset, RabbitPublisher publisher) {
        final List<Observable<PublishedMessage>> sendCallbacks = new ArrayList<>();
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
                            .map(aVoid -> new PublishedMessage(id, false))
                            .onErrorReturn(throwable -> {
                                log.errorWithParams("Failed message.", throwable);
                                return new PublishedMessage(id, true);
                            })
                            .toObservable());
        }
        return Observable.merge(sendCallbacks);
    }


    public void declareQueueAndExchange(AdminChannel sendChannel) throws IOException {
        sendChannel.exchangeDeclare(inputExchange, "topic", true, false, false, new HashMap<>());
        declareAndBindQueue(sendChannel);
    }

    private void declareAndBindQueue(AdminChannel sendChannel) throws IOException {
        sendChannel.queueDeclare(inputQueue, true, false, false, new HashMap<>());
        sendChannel.queueBind(inputQueue, inputExchange, "#", new HashMap<>());
    }

    private ChannelFactory getDroppingAndExceptionThrowingChannelFactory(final int dropAtMessageN, final int exceptionAtMessageN) {
        return new ChannelFactory() {
            final AtomicInteger publishCount = new AtomicInteger(0);
            @Override
            public ConsumeChannel createConsumeChannel(String queue) throws IOException {return null;}

            @Override
            public ConsumeChannel createConsumeChannel(String exchange, String routingKey) throws IOException {
                return null;
            }

            @Override
            public PublishChannel createPublishChannel() throws IOException {
                final PublishChannel delegate = channelFactory.createPublishChannel();
                return new PublishChannel() {
                    @Override
                    public void addConfirmListener(ConfirmListener listener) {
                        delegate.addConfirmListener(listener);
                    }

                    @Override
                    public void basicPublish(String exchange, String routingKey, AMQP.BasicProperties props, byte[] body) throws IOException {
                        log.infoWithParams("Publishing message", "id", props.getMessageId());
                        final int count = publishCount.incrementAndGet();
                        if(count == dropAtMessageN){
                            log.infoWithParams("Dropping message", "id", props.getMessageId());
                        }
                        else if(count == exceptionAtMessageN){
                            throw new IOException("expected");
                        }
                        else{
                            delegate.basicPublish(exchange,routingKey,props,body);
                        }
                    }

                    @Override
                    public long getNextPublishSeqNo() {
                        return delegate.getNextPublishSeqNo();
                    }

                    @Override
                    public boolean waitForConfirms() throws InterruptedException {
                        return delegate.waitForConfirms();
                    }

                    @Override
                    public boolean waitForConfirms(long timeout) throws InterruptedException, TimeoutException {
                        return delegate.waitForConfirms(timeout);
                    }

                    @Override
                    public void confirmSelect() throws IOException {
                        delegate.confirmSelect();
                    }

                    @Override
                    public void close() {
                        delegate.close();
                    }

                    @Override
                    public void closeWithError() {
                        delegate.closeWithError();
                    }

                    @Override
                    public boolean isOpen() {
                        return delegate.isOpen();
                    }

                    @Override
                    public int getChannelNumber() {
                        return delegate.getChannelNumber();
                    }
                };
            }

            @Override
            public AdminChannel createAdminChannel() throws IOException {
                return null;
            }
        };
    }

    static class PublishedMessage implements Comparable<PublishedMessage>{
        final Integer id;
        final boolean failed;
        PublishedMessage(Integer id, boolean failed) {
            this.id = id;
            this.failed = failed;
        }

        @Override
        public int compareTo(PublishedMessage o) {
            return id.compareTo(o.id);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PublishedMessage that = (PublishedMessage) o;

            return !(id != null ? !id.equals(that.id) : that.id != null);

        }

        @Override
        public int hashCode() {
            return id != null ? id.hashCode() : 0;
        }
    }

}
