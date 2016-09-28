package com.meltwater.rxrabbit.java7;


import com.meltwater.rxrabbit.AdminChannel;
import com.meltwater.rxrabbit.BrokerAddresses;
import com.meltwater.rxrabbit.ConnectionSettings;
import com.meltwater.rxrabbit.Message;
import com.meltwater.rxrabbit.RabbitPublisher;
import com.meltwater.rxrabbit.RoutingKey;
import com.meltwater.rxrabbit.impl.DefaultChannelFactory;
import com.meltwater.rxrabbit.util.Logger;
import rx.Single;
import rx.functions.Action1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static com.meltwater.rxrabbit.extra.ApplicationIdGenerator.generateApplicationId;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static rx.Observable.from;

public class ExampleAppTest {

    private static final Logger log = new Logger(ExampleAppTest.class);
    private static TestContainers testContainers = new TestContainers(ExampleAppTest.class);

    static BrokerAddresses addresses;

    static String outputQueue = "example-out";
    static String inputExchange = "example.in";
    static String deadLetterExchange = "example.dl";
    static String deadLetterQueue = "example-dl";
    static String outputExchange;
    static String inputQueue;
    private static MeltwaterPublisherSettings publisherSettings;
    private static MeltwaterConsumerSettings consumerSettings;
    private static ConnectionSettings connectionSettings;

    private static final long CONNECTION_BACKOFF_TIME = 10_000;
    private static final int CONNECTION_MAX_ATTEMPT = 20;
    private static MeltwaterChannelFactory channelFactory;
    private static String appname;
    private static String appInstance;
    private static String version;
    private static String statsDHost;
    private static int statsDport;

    @Before
    public void setup() throws Exception {
        killAndRestartRabbitDocker();

        Properties prop = new Properties();
        prop.load(ExampleApp.class.getResourceAsStream("/default.properties"));
        prop.putAll(System.getProperties());

        addresses = new BrokerAddresses(prop.getProperty("rabbit.broker.uris"));
        connectionSettings = new JSONSettingsFactory().createConnectionSettings(prop.getProperty("rabbit.connection.settings"));
        consumerSettings = new JSONSettingsFactory().createConsumerSettings(prop.getProperty("rabbit.consume.settings"));
        publisherSettings = new JSONSettingsFactory().createPublisherSettings(prop.getProperty("rabbit.publish.settings"));

        outputExchange = prop.getProperty("rabbit.output.exchange");
        inputQueue = prop.getProperty("rabbit.input.queue");

        //Create and start the app
        appname = "simple-consume-publish-test";
        appInstance = "0";
        version = "0.0.0";
        statsDHost = prop.getProperty("metrics.hostname");
        statsDport = Integer.parseInt(prop.getProperty("metrics.port").replaceAll("/udp",""));

        channelFactory = new MeltwaterChannelFactory(addresses, appname, appInstance, version, connectionSettings);

        //Set up queues and exchanges (this should typically be done by docker or puppet)

        AdminChannel ch;
        for (int i = 1; i <= CONNECTION_MAX_ATTEMPT; i++) {
            try {
                ch = new DefaultChannelFactory(addresses,connectionSettings).createAdminChannel();
                ch.exchangeDeclare(outputExchange,"topic", true, false, false, new HashMap<String,Object>());
                ch.exchangeDeclare(inputExchange,"topic", true, false, false, new HashMap<String, Object>());
                ch.exchangeDeclare(deadLetterExchange, "topic", true, false, false, new HashMap<String, Object>());
                ch.queueDeclare(deadLetterQueue, true, false, false, new HashMap<String, Object>());
                ch.queueDeclare(outputQueue, true, false, false, new HashMap<String, Object>());
                ch.queueBind(outputQueue, outputExchange, "#", new HashMap<String, Object>());
                ch.queueBind(deadLetterQueue, deadLetterExchange, "#", new HashMap<String, Object>());
                HashMap<String, Object> inputQArgs = new HashMap<>();
                inputQArgs.put("x-dead-letter-exchange", deadLetterExchange);
                ch.queueDeclare(inputQueue, true, false, false, inputQArgs);
                ch.queueBind(inputQueue, inputExchange, "#", new HashMap<String, Object>());
                ch.close();
                break;
            } catch (Exception ignored) {
                log.infoWithParams("Failed to create connection.. will try again ", "attempt", i, "max-attempts", CONNECTION_MAX_ATTEMPT);
                Thread.sleep(CONNECTION_BACKOFF_TIME);
                if(i==CONNECTION_MAX_ATTEMPT) throw ignored;
            }
        }

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
        final ExampleApp exampleApp = new ExampleApp(
                inputQueue,
                outputExchange,
                consumerSettings,
                publisherSettings,
                connectionSettings,
                addresses,
                appname,
                appInstance,
                version,
                statsDHost,
                statsDport);

        exampleApp.start();

        int nrToPublish = 5_000;
        publishDocuments(nrToPublish);
        Set<String> idSet = consumeDocumentsAndGetIds(nrToPublish);
        assertThat(idSet.size(), is(nrToPublish));

        exampleApp.stop();
    }

    private Set<String> consumeDocumentsAndGetIds(int nrToPublish) throws InterruptedException {
        final SortedSet<String> out = Collections.synchronizedSortedSet(new TreeSet<String>());
        MeltwaterConsumerFactory consumerFactory = new MeltwaterConsumerFactory(channelFactory, appname, appInstance, version, statsDHost, statsDport, consumerSettings);
        consumerFactory.createConsumer(outputQueue)
                .doOnNext(new Action1<Message>() {
                    @Override
                    public void call(Message message) {
                        message.acknowledger.ack();
                    }
                })
                .flatMap(new Func1<Message, Observable<Document>>() {
                    @Override
                    public Observable<Document> call(Message message) {
                        return Single.just(message)
                                .flatMap(new ConvertToQuiddity<Document>())
                                .toObservable()
                                .doOnNext(new Action1<Document>() {
                                    @Override
                                    public void call(Document document) {
                                        out.add(document.getId());
                                    }
                                });
                    }
                })
                .take(nrToPublish)
                .timeout(100, TimeUnit.SECONDS)
                .toBlocking()
                .last();

        return out;
    }

    private void publishDocuments(int nrToPublish) throws IOException {
        List<String> ids = new ArrayList<>();
        for (int i=1; i<=nrToPublish; i++){
            ids.add(""+i);
        }

        MeltwaterPublisherFactory publisherFactory = new MeltwaterPublisherFactory(channelFactory,
                appname,
                appInstance,
                version,
                statsDHost,
                statsDport,
                publisherSettings);
        final RabbitPublisher publish = publisherFactory.createPublisher();

        final DefaultQuiddityObjectFactory factory = new DefaultQuiddityObjectFactory();
        final PublishQuiddity<Document> publishToRabbit =
                new PublishQuiddity<>(
                        generateApplicationId(appname,appInstance,version),
                        inputExchange,
                        publish,
                        new RoutingKeyGenerator<Document>() {
                            @Override
                            public RoutingKey routingKey(Document data) {
                                return new RoutingKey("key");
                            }
                        }
                );

        from(ids)
                .flatMap(new Func1<String, Observable<Document>>() {
                    @Override
                    public Observable<Document> call(String id) {
                        Document doc = factory.create(Document.class);
                        doc.setId(id);
                        return Single.just(doc).flatMap(publishToRabbit).toObservable();
                    }
                })
                .toBlocking()
                .last();

        publish.close();
    }

    private static void killAndRestartRabbitDocker() throws Exception {
        testContainers.resetAll(false);

        String rabbitTcpPort = testContainers.rabbit().tcpPort();

        System.setProperty("rabbit.broker.uris", "amqp://guest:guest@docker.local:" + rabbitTcpPort);
    }
}