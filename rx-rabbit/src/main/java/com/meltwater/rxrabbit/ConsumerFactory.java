package com.meltwater.rxrabbit;

import com.meltwater.rxrabbit.impl.DefaultChannelFactory;
import com.meltwater.rxrabbit.impl.SingleChannelConsumer;
import com.meltwater.rxrabbit.util.DockerAwareHostnameProvider;
import com.meltwater.rxrabbit.util.Logger;
import com.timgroup.statsd.NoOpStatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import java.net.InetAddress;
import java.util.List;

public class ConsumerFactory {

    private final static Logger logger = new Logger(ConsumerFactory.class);

    private RabbitSettings settings;
    private ChannelFactory channelFactory;
    private Scheduler consumerScheduler = Schedulers.io();
    private String statsDHost;
    private int statsDPort = 8125;

    public ConsumerFactory(){

    }

    public ConsumerFactory(List<BrokerAddress> brokerAddresses, RabbitSettings settings) {
        this.settings = settings;
        this.channelFactory = new DefaultChannelFactory(brokerAddresses, settings);
    }

    public ConsumerFactory(ChannelFactory channelFactory, RabbitSettings settings) {
        this.settings = settings;
        this.channelFactory = channelFactory;
    }

    public Observable<Message> createConsumer(String queue) {
        StatsDClient statsDClient;
        if(statsDHost == null){
            logger.warnWithParams("Creating consumer with no-op StatsDClient");
            statsDClient = new NoOpStatsDClient();
        }
        else {
            String prefix = DockerAwareHostnameProvider.getApplicationHostName() +  "." + settings.appId + "." + queue;
            logger.infoWithParams("Creating consumer with StatsDClient",
                    "statsDHost", statsDHost,
                    "statsDPort", statsDPort,
                    "metricsPrefix", prefix);
            statsDClient = new NonBlockingStatsDClient(prefix, statsDHost,statsDPort);
        }
        return new SingleChannelConsumer(
                channelFactory, queue,
                settings.appId+"-consumer", 3, settings.close_timeout_millis, consumerScheduler,
                statsDClient).consume();
    }

    public ConsumerFactory setSettings(RabbitSettings settings) {
        this.settings = settings;
        return this;
    }

    public ConsumerFactory setChannelFactory(ChannelFactory channelFactory) {
        this.channelFactory = channelFactory;
        return this;
    }

    public ConsumerFactory setConsumerScheduler(Scheduler consumerScheduler) {
        this.consumerScheduler = consumerScheduler;
        return this;
    }

    public ConsumerFactory setStatsDPort(int statsDPort){
        this.statsDPort = statsDPort;
        return this;
    }

    public ConsumerFactory setStatsDHost(String statsDHost){
        this.statsDHost = statsDHost;
        return this;
    }

}
