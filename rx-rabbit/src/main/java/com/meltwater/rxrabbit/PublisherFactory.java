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
 * Can create {@link RabbitPublisher}s.
 *
 * Note that a single {@link ChannelFactory} is backing all the {@link RabbitPublisher}s created by this factory.
 *
 * So if the {@link DefaultChannelFactory} is used it means that all publishers will share the same {@link com.rabbitmq.client.Connection}
 * but use different {@link com.rabbitmq.client.Channel}s.
 */
public class PublisherFactory {

    private final Logger log = new Logger(PublisherFactory.class);

    private final ChannelFactory channelFactory;
    private final RabbitSettings settings;
    private final BrokerAddresses brokerAddresses;

    private PublishEventListener publishEventListener = new PublishEventListener() {};
    private Scheduler producerScheduler = Schedulers.io();

    public PublisherFactory(BrokerAddresses brokerAddresses, RabbitSettings settings) {
        this.brokerAddresses = brokerAddresses;
        this.settings = settings;
        this.channelFactory = new DefaultChannelFactory(brokerAddresses, settings);
    }

    public PublisherFactory setProducerScheduler(Scheduler producerScheduler) {
        this.producerScheduler = producerScheduler;
        return this;
    }

    public PublisherFactory setPublishEventListener(PublishEventListener publishEventListener) {
        this.publishEventListener = publishEventListener;
        return this;
    }

    public RabbitPublisher createPublisher() {
        log.infoWithParams("Creating publisher.",
                "publishChannels", settings.num_channels,
                "publisherConfirms", settings.publisher_confirms,
                "publishEventListener", publishEventListener);
        List<RabbitPublisher> publishers = new ArrayList<>();
        for(int i=0; i<settings.num_channels; i++){
            publishers.add(new SingleChannelPublisher(
                    channelFactory,
                    settings.publisher_confirms,
                    settings.retry_count,
                    producerScheduler,
                    publishEventListener,
                    settings.publish_timeout_secs,
                    settings.close_timeout_millis,
                    1));
        }
        return new RoundRobinPublisher(publishers);
    }

}
