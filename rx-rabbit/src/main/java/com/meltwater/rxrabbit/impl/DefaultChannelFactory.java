package com.meltwater.rxrabbit.impl;

import com.meltwater.rxrabbit.AdminChannel;
import com.meltwater.rxrabbit.BrokerAddresses;
import com.meltwater.rxrabbit.ChannelFactory;
import com.meltwater.rxrabbit.ChannelWrapper;
import com.meltwater.rxrabbit.ConsumeChannel;
import com.meltwater.rxrabbit.PublishChannel;
import com.meltwater.rxrabbit.RabbitSettings;
import com.meltwater.rxrabbit.util.Logger;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.impl.AMQConnection;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class DefaultChannelFactory implements ChannelFactory {

    private static final Logger log = new Logger(DefaultChannelFactory.class);

    private final Map<ChannelType, ConnectionInfo> conToChannel = new HashMap<>();

    private final BrokerAddresses addresses;
    private final RabbitSettings settings;

    public DefaultChannelFactory(BrokerAddresses addresses, RabbitSettings settings) {
        assert !addresses.getAddresses().isEmpty();
        this.addresses = addresses;
        this.settings = settings;
    }

    public List<ConnectionInfo> getOpenConnections(){
        return conToChannel.values().stream().filter(c -> c.connection.isOpen()).collect(Collectors.toList());
    }

    @Override
    public ConsumeChannel createConsumeChannel(final String queue)throws IOException, TimeoutException {
        //TODO use the queue and locate the correct host to connect to
        return (ConsumeChannel)createChannel(ChannelType.consume, queue);
    }

    @Override
    public PublishChannel createPublishChannel()throws IOException, TimeoutException {
        //TODO use the exchange and locate the correct host to connect to
        return (PublishChannel)createChannel(ChannelType.publish, null);
    }

    @Override
    public AdminChannel createAdminChannel() throws IOException, TimeoutException {
        return (AdminChannel)createChannel(ChannelType.admin, null);
    }

    public synchronized void closeChannelWitError(ChannelImpl channel) {
        if(channel == null){
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
                    "name",  connectionInfo.name,
                    "connectTime", connectionInfo.startTime,
                    "connectionType", connectionInfo.type,
                    "wasOpen", connectionIsOpen);
        }
    }

    private synchronized ChannelWrapper createChannel(ChannelType type, String queue) throws IOException, TimeoutException {
        Channel innerChannel = getOrCreateConnection(type).createChannel();
        ConnectionInfo info = conToChannel.get(type);

        final int hashCode = innerChannel.hashCode();
        ChannelImpl channel = null;
        switch (type){
            case consume  : channel = new ConsumeChannelImpl(innerChannel, queue, hashCode, type, this); break;
            case publish  : channel = new PublishChannelImpl(innerChannel, hashCode, type, this); break;
            case admin    : channel = new AdminChannelImpl(innerChannel, hashCode, type, this); break;
        }

        info.channels.add(channel);
        log.infoWithParams("Successfully created "+type+" channel.",
                "channel", channel,
                "connectionName", info.name,
                "connectTime", info.startTime);
        return channel;
    }


    private synchronized Connection getOrCreateConnection(ChannelType connectionType) throws IOException, TimeoutException {
        if (conToChannel.containsKey(connectionType)) {
            if (conToChannel.get(connectionType).connection.isOpen()) {
                return conToChannel.get(connectionType).connection;
            }else{
                conToChannel.remove(connectionType);
            }
        }
        String connectionName = settings.app_instance_id + "-" + connectionType;
        DateTime startTime = new DateTime(DateTimeZone.UTC);

        Map<String, Object> clientProperties = new HashMap<>(); //TODO allow the user to add more properties to this map...
        clientProperties.put("app_id", settings.app_instance_id);
        clientProperties.put("name", connectionName);
        clientProperties.put("connect_time", startTime.toString());
        clientProperties.put("connection_type", connectionType.toString());

        //TODO use all the values in addresses (connect to them in random? order (or use a configurable/programmable strategy??) )
        ConnectionFactory cf = new ConnectionFactory();
        cf.setPassword(addresses.get(0).password);
        cf.setUsername(addresses.get(0).username);
        cf.setPort(addresses.get(0).port);
        cf.setHost(addresses.get(0).host);
        cf.setVirtualHost(addresses.get(0).virtualHost);
        cf.setRequestedHeartbeat(settings.heartbeat);
        cf.setConnectionTimeout(settings.connection_timeout_millis);
        cf.setShutdownTimeout(settings.shutdown_timeout_millis);
        cf.setRequestedFrameMax(settings.frame_max);
        cf.setHandshakeTimeout(settings.handshake_timeout_millis);
        cf.setClientProperties(clientProperties);
        //cf.setSocketConfigurator(); NOTE is this worth investigating??
        cf.setRequestedChannelMax(0);//Hard coded ..
        cf.setAutomaticRecoveryEnabled(false);//Hard coded ..
        cf.setTopologyRecoveryEnabled(false);//Hard coded ..

        Connection connection = cf.newConnection();
        conToChannel.put(connectionType,
                new ConnectionInfo(
                        connection,
                        new ArrayList<>(),
                        settings.app_instance_id,
                        startTime,
                        connectionType,
                        connectionName)
        );
        log.infoWithParams("Successfully created "+connectionType+" connection to broker.",
                "address", addresses.get(0).toString(),
                "localPort", ((AMQConnection) connection).getLocalPort(),
                "name", connectionName,
                "connectTime", startTime.toString(),
                "connectionType", connectionType,
                "settings", settings.toString());
        return connection;
    }

    public static class ConnectionInfo {
        final Connection connection;
        final List<ChannelImpl> channels;
        final String appId;
        final DateTime startTime;
        final ChannelType type;
        final String name;
        ConnectionInfo(Connection connection, List<ChannelImpl> channels, String appId, DateTime startTime, ChannelType type, String name) {
            this.connection = connection;
            this.channels = channels;
            this.appId = appId;
            this.startTime = startTime;
            this.type = type;
            this.name = name;
        }
        @Override
        public String toString() {
            return "ConnectionInfo{" +
                    ", channels=" + channels.size() +
                    ", startTime=" + startTime +
                    ", type=" + type +
                    ", name='" + name + '\'' +
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
}
