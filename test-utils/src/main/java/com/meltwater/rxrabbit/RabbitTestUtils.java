package com.meltwater.rxrabbit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.meltwater.rxrabbit.docker.DockerContainers;
import com.meltwater.rxrabbit.impl.DefaultChannelFactory;
import com.meltwater.rxrabbit.util.Logger;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Realm;
import com.ning.http.client.Response;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class RabbitTestUtils {

    private static final Logger log = new Logger(RabbitTestUtils.class);

    private static final int CONNECTION_BACKOFF_TIME = 500;
    private static final int CONNECTION_MAX_ATTEMPT = 20;

    public static final Realm realm = new Realm.RealmBuilder()
            .setPrincipal("guest")
            .setPassword("guest")
            .setUsePreemptiveAuth(true)
            .setScheme(Realm.AuthScheme.BASIC)
            .build();

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

    public static void waitForAllConnectionsToClose(DefaultChannelFactory channelFactory, DockerContainers dockerContainers) throws InterruptedException {
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


    public static void waitForNumQueuesToBePresent(int numQueues, AsyncHttpClient httpClient, String rabbitAdminPort) throws Exception {
        for (int i = 1; i <= CONNECTION_MAX_ATTEMPT; i++) {
            try {
                List<String> queueNames = getQueueNames(httpClient, rabbitAdminPort);
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

    public static rx.Observable<Message> createConsumer(ConsumerFactory consumerFactory, String inputQueue) throws InterruptedException {
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


    public static  List<String> getQueueNames(AsyncHttpClient httpClient, String rabbitAdminPort) throws Exception{
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
    public static  List<String> getConnectionNames(AsyncHttpClient httpClient, String rabbitAdminPort) throws Exception {
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

    public static int countConsumers(AsyncHttpClient httpClient, String rabbitAdminPort) throws Exception {
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

    public static void declareQueueAndExchange(AdminChannel sendChannel, String inputQueue, Exchange inputExchange) throws IOException {
        sendChannel.exchangeDeclare(inputExchange.name, "topic", true, false, false, new HashMap<>());
        declareAndBindQueue(sendChannel, inputQueue, inputExchange);
    }

    public static void declareAndBindQueue(AdminChannel sendChannel, String inputQueue, Exchange inputExchange) throws IOException {
        sendChannel.queueDeclare(inputQueue, true, false, false, new HashMap<>());
        sendChannel.queueBind(inputQueue, inputExchange.name, "#", new HashMap<>());
    }
}
