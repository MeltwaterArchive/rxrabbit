package com.meltwater.rxrabbit;

import com.meltwater.rxrabbit.impl.DefaultChannelFactory;
import com.meltwater.rxrabbit.impl.RoundRobinPublisher;
import com.meltwater.rxrabbit.impl.SingleChannelPublisher;
import com.meltwater.rxrabbit.util.DockerAwareHostnameProvider;
import com.meltwater.rxrabbit.util.Logger;
import com.timgroup.statsd.NoOpStatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import java.util.ArrayList;
import java.util.List;

/**
 * Can create {@link RabbitPublisher}s using the supplied {@link RabbitSettings} and {@link BrokerAddresses} settings.
 */
public class PublisherFactory {

    private final Logger log = new Logger(PublisherFactory.class);

    private RabbitSettings settings;
    private ChannelFactory channelFactory;
    private Scheduler producerScheduler = Schedulers.io();

    //TODO remove statsd from here (use an interface instead)
    private int statsDPort = 8125;
    private String statsDHost;

    public PublisherFactory(BrokerAddresses brokerAddresses, RabbitSettings settings) {
        this.settings = settings;
        this.channelFactory = new DefaultChannelFactory(brokerAddresses, settings);
    }

    //TODO weird that exchange is specified both here and when publishing...
    public RabbitPublisher createPublisher(String exchange) {
        StatsDClient statsDClient;

        if(statsDHost == null){
            log.warnWithParams("Creating publisher with no-op metrics client.",
                    "publishChannels", settings.num_channels,
                    "publisherConfirms", settings.publisher_confirms
            );
            statsDClient = new NoOpStatsDClient();
        }
        else {
            String prefix = DockerAwareHostnameProvider.getApplicationHostName() +  "." + settings.app_instance_id + "." + exchange;
            log.infoWithParams("Creating publisher with statsD metrics client",
                    "publishChannels", settings.num_channels,
                    "publisherConfirms", settings.publisher_confirms,
                    "statsDHost", statsDHost,
                    "statsDPort", statsDPort,
                    "metricsPrefix", prefix);
            statsDClient = new NonBlockingStatsDClient(prefix, statsDHost,statsDPort);
        }
        List<RabbitPublisher> publishers = new ArrayList<>();
        for(int i=0; i<settings.num_channels; i++){
            publishers.add(new SingleChannelPublisher(
                    channelFactory,
                    settings.publisher_confirms,
                    settings.retry_count,
                    producerScheduler,
                    statsDClient,
                    settings.publish_timeout_secs,
                    settings.close_timeout_millis,
                    1));
        }
        return new RoundRobinPublisher(publishers);
    }

    public PublisherFactory setSettings(RabbitSettings settings) {
        this.settings = settings;
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

}
