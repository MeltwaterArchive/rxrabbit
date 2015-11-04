package com.meltwater.rxrabbit;

import com.meltwater.rxrabbit.impl.DefaultChannelFactory;
import com.meltwater.rxrabbit.impl.SingleChannelConsumer;
import com.meltwater.rxrabbit.util.Logger;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import java.util.ArrayList;
import java.util.List;

/**
 * Can create {@link Observable}s that streams the messages delivered to the connected rabbit queue.
 *
 * Note that a single {@link ChannelFactory} is backing all the {@link Observable}s created by this factory,
 * so if the {@link DefaultChannelFactory} is used it means that all observables will share the same {@link com.rabbitmq.client.Connection}
 * but use different {@link com.rabbitmq.client.Channel}s.
 *
 */
public class ConsumerFactory {

    private final static Logger log = new Logger(ConsumerFactory.class);

    private final ChannelFactory channelFactory;
    private final ConsumerSettings settings;

    private ConsumeEventListener consumeEventListener = new ConsumeEventListener() {};
    private Scheduler consumerScheduler = Schedulers.io();

    public ConsumerFactory(ChannelFactory channelFactory, ConsumerSettings settings) {
        this.settings = settings;
        this.channelFactory = channelFactory;
    }

    public ConsumerFactory setConsumeEventListener(ConsumeEventListener consumeEventListener) {
        this.consumeEventListener = consumeEventListener;
        return this;
    }

    public ConsumerFactory setConsumerScheduler(Scheduler consumerScheduler) {
        this.consumerScheduler = consumerScheduler;
        return this;
    }

    public Observable<Message> createConsumer(String queue) {
        log.infoWithParams("Creating publisher.",
                "consumeChannels", settings.getNum_channels(),
                "preFetch", settings.getPre_fetch_count(),
                "consumeEventListener", consumeEventListener);

        SingleChannelConsumer consumer = new SingleChannelConsumer(
                channelFactory,
                queue,
                settings.getPre_fetch_count(),
                settings.getConsumer_tag_prefix() + "{" + queue + "}-consumer",
                settings.getRetry_count(),
                settings.getClose_timeout_millis(),
                consumerScheduler,
                consumeEventListener);
        List<Observable<Message>> consumers = new ArrayList<>();
        for(int i=0; i<settings.getNum_channels(); i++){
            consumers.add(consumer.consume());
        }
        return Observable.merge(consumers);
    }

}
