package com.meltwater.rxrabbit.impl;

import com.google.common.collect.Collections2;
import com.meltwater.rxrabbit.AdminChannel;
import com.meltwater.rxrabbit.BrokerAddresses;
import com.meltwater.rxrabbit.ChannelFactory;
import com.meltwater.rxrabbit.ChannelWrapper;
import com.meltwater.rxrabbit.ConnectionSettings;
import com.meltwater.rxrabbit.ConsumeChannel;
import com.meltwater.rxrabbit.PublishChannel;
import com.meltwater.rxrabbit.util.Logger;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.impl.AMQConnection;
import rx.functions.Func2;

import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeoutException;

public class DefaultChannelFactory implements ChannelFactory {

    private static final Logger log = new Logger(DefaultChannelFactory.class);

    private final Map<ChannelType, ConnectionInfo> conToChannel = new HashMap<>();

    private final BrokerAddresses addresses;
    private final ConnectionSettings settings;

    public DefaultChannelFactory(BrokerAddresses addresses, ConnectionSettings settings) {
        assert addresses!=null;
        assert settings!=null;
        assert !addresses.getAddresses().isEmpty();
        this.addresses = addresses;
        this.settings = settings;
    }

    public synchronized List<ConnectionInfo> getOpenConnections(){
        return new ArrayList<>(Collections2.filter(conToChannel.values(), c -> c.connection.isOpen()));
    }

    @Override
    public ConsumeChannel createConsumeChannel(final String queue)throws IOException {
        assert queue!=null;
        return (ConsumeChannel)createChannel(
                ChannelType.consume,
                (hashCode, innerChannel) -> new ConsumeChannelImpl(innerChannel, queue, hashCode, ChannelType.consume, DefaultChannelFactory.this)
        );
    }

    @Override
    public ConsumeChannel createConsumeChannel(String exchange, String routingkey) throws IOException {
        assert exchange!=null;
        assert routingkey!=null;
        return (ConsumeChannel)createChannel(
                ChannelType.consume,
                (hashCode, innerChannel) -> {
                    try {
                        return new ConsumeChannelImpl(innerChannel, exchange, routingkey, hashCode, ChannelType.consume, DefaultChannelFactory.this);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
        );
    }

    @Override
    public PublishChannel createPublishChannel()throws IOException {
        return (PublishChannel)createChannel(
                ChannelType.publish,
                (hashCode, innerChannel) -> new PublishChannelImpl(innerChannel, hashCode, ChannelType.publish, DefaultChannelFactory.this)
        );
    }

    @Override
    public AdminChannel createAdminChannel() throws IOException {
        return (AdminChannel)createChannel(
                ChannelType.admin,
                (hashCode, innerChannel) -> new AdminChannelImpl(innerChannel, hashCode, ChannelType.admin, DefaultChannelFactory.this)
        );
    }

    public synchronized void closeChannelWitError(ChannelImpl channel) {
        if(channel == null){
            //TODO not covered in tests still valid?
            return;
        }
        final ConnectionInfo connectionInfo = conToChannel.get(channel.channelType);
        if (connectionInfo == null) {
            return;
        }
        if(!connectionInfo.channels.isEmpty()){
            for (ChannelImpl ch : new ArrayList<>(connectionInfo.channels)) {
                closeChannel(ch);
            }
        }
    }

    public synchronized void closeChannel(ChannelImpl channel) {
        if(channel == null){
            //TODO not covered in tests still valid?
            return;
        }
        ConnectionInfo info = conToChannel.get(channel.channelType);
        if(info==null){
            return;
        }
        info.channels.remove(channel);
        final boolean channelIsOpen = channel.isOpen();
        if(channelIsOpen){
            try {
                channel.actuallyClose();
            } catch (Exception e) {
                log.warnWithParams("Unexpected error when closing channel.", e,
                        "wasOpen", channelIsOpen,
                        "isOpen", channel.isOpen());
            }
        }

        log.infoWithParams("Closed and disposed "+channel.channelType+" channel.",
                "channelType", channel.channelType,
                "channelNr", channel.getChannelNumber(),
                "wasOpen", channelIsOpen);

        Set<ChannelType> connectionsToClose = new HashSet<>();
        for (Map.Entry<ChannelType,ConnectionInfo> c : conToChannel.entrySet()) {
            if (c.getValue().channels.isEmpty()){
                connectionsToClose.add(c.getKey());
            }
        }
        for(ChannelType type: connectionsToClose){
            final ConnectionInfo connectionInfo = conToChannel.remove(type);
            final Connection connection = connectionInfo.connection;
            boolean connectionIsOpen = connection.isOpen();
            if(connectionIsOpen){
                try {
                    connection.close();
                } catch (Exception e) {
                    log.warnWithParams("Unexpected error when closing connection.", e,
                            "wasOpen", connectionIsOpen,
                            "isOpen", connection.isOpen());
                }
            }
            log.infoWithParams("Closed and disposed "+connectionInfo.type+" connection.",
                    "connectionType", connectionInfo.type,
                    "properties", connectionInfo.clientProperties,
                    "wasOpen", connectionIsOpen);
        }
    }

    private synchronized ChannelWrapper createChannel(ChannelType type, Func2<Integer, Channel, ChannelImpl> channelFunction) throws IOException {
        Channel innerChannel = getOrCreateConnection(type).createChannel();
        ConnectionInfo info = conToChannel.get(type);
        ChannelImpl channel = channelFunction.call(innerChannel.hashCode(), innerChannel);
        info.channels.add(channel);
        log.infoWithParams("Successfully created "+type+" channel.",
                "channel", channel,
                "properties", info.clientProperties);
        return channel;
    }


    private synchronized Connection getOrCreateConnection(ChannelType connectionType) throws IOException {
        if (conToChannel.containsKey(connectionType)) {
            if (conToChannel.get(connectionType).connection.isOpen()) {
                return conToChannel.get(connectionType).connection;
            }else{
                //TODO not covered in tests
                conToChannel.remove(connectionType);
            }
        }

        return createConnection(connectionType);
    }

    private synchronized Connection createConnection(ChannelType connectionType) throws ConnectionFailureException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date startTime = new Date();
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        settings.getClient_properties().put("connection_type", connectionType.toString());
        settings.getClient_properties().put("connect_time", sdf.format(startTime)+"Z");

        ConnectionFactory cf = new ConnectionFactory();
        cf.setRequestedHeartbeat(settings.getHeartbeat());
        cf.setConnectionTimeout(settings.getConnection_timeout_millis());
        cf.setShutdownTimeout(settings.getShutdown_timeout_millis());
        cf.setRequestedFrameMax(settings.getFrame_max());
        cf.setHandshakeTimeout(settings.getHandshake_timeout_millis());
        cf.setClientProperties((Map)settings.getClient_properties());
        //cf.setSocketConfigurator(); NOTE is this worth investigating??
        cf.setRequestedChannelMax(0);//Hard coded ..
        cf.setAutomaticRecoveryEnabled(false);//Hard coded ..
        cf.setTopologyRecoveryEnabled(false);//Hard coded ..

        Exception lastException = null;
        Connection connection = null;
        for (BrokerAddresses.BrokerAddress address : addresses) {
            cf.setPassword(address.password);
            cf.setUsername(address.username);
            cf.setPort(address.port);
            cf.setHost(address.host);
            cf.setVirtualHost(address.virtualHost);
            try {
                if(address.scheme.toLowerCase().equals("amqps")){
                    cf.useSslProtocol();
                    cf.setSocketFactory(SSLSocketFactory.getDefault()); //Because rabbit uses NoopTrustStore by default...
                }
                log.infoWithParams("Creating "+connectionType+" connection to broker ...",
                        "address", address.toString(),
                        "settings", settings.toString());
                connection = cf.newConnection();
                boolean isOpen = connection.isOpen();
                if(!isOpen){
                    continue;
                }
                break;
            } catch (Exception e) {
                log.errorWithParams("Failed to createConnection to broker",
                        e,
                        "address", address.toString());
                lastException = e;
            }
        }

        if(connection == null){
            throw new ConnectionFailureException(cf, lastException);
        }

        conToChannel.put(connectionType,
                new ConnectionInfo(
                        connection,
                        new ArrayList<ChannelImpl>(),
                        settings.getClient_properties(),
                        connectionType)
        );
        log.infoWithParams("Successfully created "+connectionType+" connection to broker.",
                "address", addresses.get(0).toString(),
                "localPort", ((AMQConnection) connection).getLocalPort(),
                "settings", settings.toString());

        return connection;

    }

    public static class ConnectionInfo {
        final Connection connection;
        final List<ChannelImpl> channels;
        final Map<String, Object> clientProperties;
        final ChannelType type;
        ConnectionInfo(Connection connection, List<ChannelImpl> channels, Map<String,Object> clientProperties, ChannelType type) {
            this.connection = connection;
            this.channels = channels;
            this.clientProperties = clientProperties;
            this.type = type;
        }
        @Override
        public String toString() {
            return "Connection{" +
                    ", numChannels=" + channels.size() +
                    ", type=" + type +
                    ", properties='" + clientProperties.toString() + '\'' +
                    '}';
        }
    }

    static abstract class ChannelImpl implements ChannelWrapper {
        final Channel delegate;
        final int hashCode;
        final ChannelType channelType;
        final DefaultChannelFactory factory;

        ChannelImpl(Channel delegate, int hashCode, ChannelType channelType, DefaultChannelFactory factory) {
            this.delegate = delegate;
            this.hashCode = hashCode;
            this.channelType = channelType;
            this.factory = factory;
        }

        public void actuallyClose() throws IOException, TimeoutException {
            delegate.close();
        }
        @Override
        public boolean isOpen() {
            return delegate.isOpen() && delegate.getConnection().isOpen();
        }
        @Override
        public int getChannelNumber() {
            return delegate.getChannelNumber();
        }
        @Override
        public void close() {
            factory.closeChannel(this);
        }
        @Override
        public void closeWithError() {
            factory.closeChannelWitError(this);
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ChannelImpl that = (ChannelImpl) o;
            return hashCode == that.hashCode;
        }
        @Override
        public int hashCode() {
            return hashCode;
        }
        @Override
        public String toString() {
            return "{" +
                    "channelType=" + channelType +
                    ", channelNo=" + delegate.getChannelNumber() +
                    ", localPort=" + ((AMQConnection) delegate.getConnection()).getLocalPort() +
                    '}';
        }
    }

    static class AdminChannelImpl extends ChannelImpl implements AdminChannel {
        AdminChannelImpl(Channel delegate, int hashCode, ChannelType channelType, DefaultChannelFactory factory) {
            super(delegate, hashCode, channelType, factory);
        }

        @Override
        public Connection getConnection() {
            return delegate.getConnection();
        }

        @Override
        public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) throws IOException {
            return delegate.exchangeDeclare(exchange,type,durable,autoDelete,internal,arguments);
        }

        @Override
        public void exchangeDeclareNoWait(String exchange, String type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) throws IOException {
            delegate.exchangeDeclareNoWait(exchange,type,durable,autoDelete,internal,arguments);
        }

        @Override
        public AMQP.Exchange.DeclareOk exchangeDeclarePassive(String name) throws IOException {
            return delegate.exchangeDeclarePassive(name);
        }

        @Override
        public AMQP.Exchange.DeleteOk exchangeDelete(String exchange, boolean ifUnused) throws IOException {
            return delegate.exchangeDelete(exchange,ifUnused);
        }

        @Override
        public void exchangeDeleteNoWait(String exchange, boolean ifUnused) throws IOException {
            delegate.exchangeDelete(exchange,ifUnused);
        }

        @Override
        public AMQP.Exchange.DeleteOk exchangeDelete(String exchange) throws IOException {
            return delegate.exchangeDelete(exchange);
        }

        @Override
        public AMQP.Queue.DeclareOk queueDeclare(String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments) throws IOException {
            return delegate.queueDeclare(queue,durable,exclusive,autoDelete,arguments);
        }

        @Override
        public void queueDeclareNoWait(String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments) throws IOException {
            delegate.queueDeclare(queue,durable,exclusive,autoDelete,arguments);
        }

        @Override
        public AMQP.Queue.DeclareOk queueDeclarePassive(String queue) throws IOException {
            return delegate.queueDeclarePassive(queue);
        }

        @Override
        public AMQP.Queue.DeleteOk queueDelete(String queue, boolean ifUnused, boolean ifEmpty) throws IOException {
            return delegate.queueDelete(queue, ifUnused, ifEmpty);
        }

        @Override
        public void queueDeleteNoWait(String queue, boolean ifUnused, boolean ifEmpty) throws IOException {
            delegate.queueDelete(queue, ifUnused, ifEmpty);
        }

        @Override
        public AMQP.Queue.BindOk queueBind(String queue, String exchange, String routingKey, Map<String, Object> arguments) throws IOException {
            return delegate.queueBind(queue,exchange,routingKey,arguments);
        }

        @Override
        public void queueBindNoWait(String queue, String exchange, String routingKey, Map<String, Object> arguments) throws IOException {
            delegate.queueBind(queue,exchange,routingKey,arguments);
        }

        @Override
        public AMQP.Queue.UnbindOk queueUnbind(String queue, String exchange, String routingKey, Map<String, Object> arguments) throws IOException {
            return delegate.queueUnbind(queue,exchange,routingKey,arguments);
        }

        @Override
        public AMQP.Queue.PurgeOk queuePurge(String queue) throws IOException {
            return delegate.queuePurge(queue);
        }

        @Override
        public GetResponse basicGet(String queue, boolean autoAck) throws IOException {
            return delegate.basicGet(queue,autoAck);
        }
    }

    static class PublishChannelImpl extends ChannelImpl implements PublishChannel {

        PublishChannelImpl(Channel delegate, int hashCode, ChannelType channelType, DefaultChannelFactory factory) {
            super(delegate, hashCode, channelType, factory);
        }

        @Override
        public void addConfirmListener(ConfirmListener confirmListener) {
            delegate.addConfirmListener(confirmListener);
        }

        @Override
        public long getNextPublishSeqNo() {
            return delegate.getNextPublishSeqNo();
        }

        @Override
        public void basicPublish(String exchange, String routingKey, AMQP.BasicProperties props, byte[] payload) throws IOException {
            delegate.basicPublish(exchange,routingKey,props,payload);
        }

        @Override
        public boolean waitForConfirms(long closeTimeoutMillis) throws InterruptedException, TimeoutException {
            return delegate.waitForConfirms(closeTimeoutMillis);
        }

        @Override
        public void confirmSelect() throws IOException {
            delegate.confirmSelect();
        }

        @Override
        public boolean waitForConfirms() throws InterruptedException {
            return delegate.waitForConfirms();
        }
    }

    static class ConsumeChannelImpl extends ChannelImpl implements ConsumeChannel {

        private final String queue;

        ConsumeChannelImpl(Channel delegate, String queue, int hashCode, ChannelType channelType, DefaultChannelFactory factory) {
            super(delegate, hashCode, channelType, factory);
            this.queue = queue;
        }

        ConsumeChannelImpl( Channel delegate, String exchange, String routingKey,int hashCode, ChannelType channelType, DefaultChannelFactory factory) throws IOException {
            super(delegate, hashCode, channelType, factory);
            AMQP.Queue.DeclareOk q = delegate.queueDeclare();
            delegate.queueBind(q.getQueue(), exchange, routingKey);
            this.queue = q.getQueue();
        }

        @Override
        public String getQueue() {
            return queue;
        }

        @Override
        public void basicCancel(String consumerTag) throws IOException {
            delegate.basicCancel(consumerTag);
        }

        @Override
        public void basicAck(long deliveryTag, boolean multiple) throws IOException {
            delegate.basicAck(deliveryTag, multiple);
        }

        @Override
        public void basicNack(long deliveryTag, boolean multiple) throws IOException {
            delegate.basicNack(deliveryTag, multiple, false);
        }

        @Override
        public void basicConsume(String consumerTag, Consumer consumer) throws IOException {
            delegate.basicConsume(queue, false, consumerTag, consumer);
        }

        @Override
        public void basicQos(int prefetchCount) throws IOException {
            delegate.basicQos(prefetchCount);
        }
    }

    enum ChannelType {
        publish,
        consume,
        admin
    }

    private class ConnectionFailureException extends IOException {
        public ConnectionFailureException(ConnectionFactory cf, Exception e) {
            super(
                String.format(
                    "Error while connecting to broker. host='%s' port=%d virtualHost='%s' username='%s'",
                    cf.getHost(), cf.getPort(), cf.getVirtualHost(), cf.getUsername()
                ),
                e
            );
        }
    }
}
