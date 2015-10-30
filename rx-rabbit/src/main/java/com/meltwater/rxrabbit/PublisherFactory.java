package com.meltwater.rxrabbit;

import com.meltwater.rxrabbit.impl.DefaultChannelFactory;
import com.meltwater.rxrabbit.impl.SingleChannelPublisher;
import com.meltwater.rxrabbit.util.DockerAwareHostnameProvider;
import com.meltwater.rxrabbit.util.Logger;
import com.timgroup.statsd.NoOpStatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import java.util.List;

public class PublisherFactory {

    private final Logger log = new Logger(PublisherFactory.class);

    private RabbitSettings settings;
    private ChannelFactory channelFactory;
    private Scheduler producerScheduler = Schedulers.io();
    private int maxRetryAttempts = 5;

    private int publishTimeoutSeconds = 60; //five minutes
    private int cleanupTimeoutCacheIntervalSec = 15;
    private int statsDPort = 8125;
    private String statsDHost;

    public PublisherFactory(){

    }

    public PublisherFactory(List<BrokerAddress> brokerAddresses, RabbitSettings settings) {
        this.settings = settings;
        this.channelFactory = new DefaultChannelFactory(brokerAddresses, settings);
    }

    public PublisherFactory(ChannelFactory channelFactory, RabbitSettings settings) {
        this.settings = settings;
        this.channelFactory = channelFactory;
    }

    public RabbitPublisher createPublisher(String exchange) {
        StatsDClient statsDClient;

        if(statsDHost == null){
            log.warnWithParams("Creating publisher with no-op StatsDClient");
            statsDClient = new NoOpStatsDClient();
        }
        else {
            String prefix = DockerAwareHostnameProvider.getApplicationHostName() +  "." + settings.appId + "." + exchange;
            log.infoWithParams("Creating publisher with StatsDClient",
                    "statsDHost", statsDHost,
                    "statsDPort", statsDPort,
                    "metricsPrefix", prefix);
            statsDClient = new NonBlockingStatsDClient(prefix, statsDHost,statsDPort);
        }
        return new SingleChannelPublisher(
                channelFactory,
                exchange,
                maxRetryAttempts,
                producerScheduler,
                statsDClient,
                publishTimeoutSeconds,
                cleanupTimeoutCacheIntervalSec);
    }

    public PublisherFactory setSettings(RabbitSettings settings) {
        this.settings = settings;
        return this;
    }

    public PublisherFactory setChannelFactory(ChannelFactory channelFactory) {
        this.channelFactory = channelFactory;
        return this;
    }

    public PublisherFactory setProducerScheduler(Scheduler producerScheduler) {
        this.producerScheduler = producerScheduler;
        return this;
    }

    public PublisherFactory setStatsDPort(int statsDPort){
        assert statsDPort>0;
        this.statsDPort = statsDPort;
        return this;
    }

    public PublisherFactory setStatsDHost(String statsDHost){
        assert statsDHost!=null;
        this.statsDHost = statsDHost;
        return this;
    }

    public PublisherFactory setMaxRetryAttempts(int maxRetryAttempts){
        assert maxRetryAttempts >0;
        this.maxRetryAttempts = maxRetryAttempts;
        return this;
    }

    public PublisherFactory setPublishTimeoutSeconds(int publishTimeoutSeconds) {
        this.publishTimeoutSeconds = publishTimeoutSeconds;
        return this;
    }

}
