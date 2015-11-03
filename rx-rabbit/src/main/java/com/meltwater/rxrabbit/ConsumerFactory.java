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
 * Can create {@link Observable} that streams the messages delivered to the connected rabbit queue.
 *
 * Note that a single {@link ChannelFactory} is backing all the {@link Observable}s created by this factory.
 *
 * So if the {@link DefaultChannelFactory} is used it means that all observables will share the same {@link com.rabbitmq.client.Connection}
 * but use different {@link com.rabbitmq.client.Channel}s.
 *
 */
public class ConsumerFactory {

    private final static Logger log = new Logger(ConsumerFactory.class);

    private final ChannelFactory channelFactory;
    private final RabbitSettings settings;
    private final BrokerAddresses brokerAddresses;

    private ConsumeEventListener consumeEventListener = new ConsumeEventListener() {};
    private Scheduler consumerScheduler = Schedulers.io();

    public ConsumerFactory(BrokerAddresses brokerAddresses, RabbitSettings settings) {
        this.brokerAddresses = brokerAddresses;
        this.settings = settings;
        this.channelFactory = new DefaultChannelFactory(brokerAddresses, settings);
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
                "consumeChannels", settings.num_channels,
                "preFetch", settings.pre_fetch_count,
                "consumeEventListener", consumeEventListener);

        SingleChannelConsumer consumer = new SingleChannelConsumer(
                channelFactory,
                queue,
                settings.pre_fetch_count,
                settings.app_instance_id + "{" + queue + "}-consumer",
                settings.retry_count,
                settings.close_timeout_millis,
                consumerScheduler,
                consumeEventListener);
        List<Observable<Message>> consumers = new ArrayList<>();
        for(int i=0; i<settings.num_channels; i++){
            consumers.add(consumer.consume());
        }
        //TODO is this merge useful here? (can also be done by application code..)
        //TODO if we keep this add to the javadoc!
        return Observable.merge(consumers);
    }

}
