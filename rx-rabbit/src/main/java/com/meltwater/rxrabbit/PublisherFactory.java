package com.meltwater.rxrabbit;

import com.meltwater.rxrabbit.impl.DefaultChannelFactory;
import com.meltwater.rxrabbit.impl.RoundRobinPublisher;
import com.meltwater.rxrabbit.impl.SingleChannelPublisher;
import com.meltwater.rxrabbit.util.Logger;
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

    private final PublisherSettings settings;
    private final ChannelFactory channelFactory;

    private PublishEventListener publishEventListener = new PublishEventListener() {};
    private Scheduler observeOnScheduler = Schedulers.computation();

    public PublisherFactory(ChannelFactory channelFactory, PublisherSettings settings) {
        this.channelFactory = channelFactory;
        this.settings = settings;
    }

    public PublisherFactory setObserveOnScheduler(Scheduler observeOnScheduler) {
        this.observeOnScheduler = observeOnScheduler;
        return this;
    }

    public PublisherFactory setPublishEventListener(PublishEventListener publishEventListener) {
        this.publishEventListener = publishEventListener;
        return this;
    }

    public RabbitPublisher createPublisher() {
        log.infoWithParams("Creating publisher.",
                "publishChannels", settings.getNum_channels(),
                "publisherConfirms", settings.isPublisher_confirms(),
                "publishEventListener", publishEventListener);
        List<RabbitPublisher> publishers = new ArrayList<>();
        for(int i=0; i<settings.getNum_channels(); i++){
            publishers.add(new SingleChannelPublisher(
                    channelFactory,
                    settings.isPublisher_confirms(),
                    settings.getRetry_count(),
                    observeOnScheduler,
                    publishEventListener,
                    settings.getPublish_timeout_secs(),
                    settings.getClose_timeout_millis(),
                    1));
        }
        return new RoundRobinPublisher(publishers);
    }

}
