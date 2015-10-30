package com.meltwater.rxrabbit;

import com.meltwater.rxrabbit.impl.DefaultChannelFactory;
import com.meltwater.rxrabbit.impl.SingleChannelConsumer;
import com.meltwater.rxrabbit.util.Logger;
import com.timgroup.statsd.NoOpStatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import java.util.ArrayList;
import java.util.List;

import static com.meltwater.rxrabbit.util.DockerAwareHostnameProvider.getApplicationHostName;

public class ConsumerFactory {

    private final static Logger logger = new Logger(ConsumerFactory.class);

    private Scheduler consumerScheduler = Schedulers.io();
    private ChannelFactory channelFactory;
    private RabbitSettings settings;
    private int statsDPort = 8125;
    private String statsDHost;

    public ConsumerFactory(){}

    public ConsumerFactory(BrokerAddresses brokerAddresses, RabbitSettings settings) {
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
            logger.warnWithParams("Creating consumer with no-op metrics client.",
                "consumeChannels", settings.num_channels,
                "preFetch", settings.pre_fetch_count
            );
            statsDClient = new NoOpStatsDClient();
        } else {
            String prefix = getApplicationHostName() +  "." + settings.app_instance_id + "." + queue;
            logger.infoWithParams("Creating consumer with statsD metrics client",
                    "consumeChannels", settings.num_channels,
                    "preFetch", settings.pre_fetch_count,
                    "statsDHost", statsDHost,
                    "statsDPort", statsDPort,
                    "metricsPrefix", prefix);
            statsDClient = new NonBlockingStatsDClient(prefix, statsDHost,statsDPort);
        }
        List<Observable<Message>> consumers = new ArrayList<>();
        for(int i=0; i<settings.num_channels; i++){
            consumers.add(new SingleChannelConsumer(
                    channelFactory,
                    queue,
                    settings.app_instance_id +"+"+queue+"-consumer",
                    settings.retry_count,
                    settings.close_timeout_millis,
                    consumerScheduler,
                    statsDClient).consume());
        }
        return Observable.merge(consumers);
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
