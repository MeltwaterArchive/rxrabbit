package com.meltwater.rxrabbit;

import com.meltwater.rxrabbit.impl.ConnectionRetryHandler;
import com.meltwater.rxrabbit.impl.DefaultChannelFactory;
import com.meltwater.rxrabbit.impl.SingleChannelConsumer;
import com.meltwater.rxrabbit.util.Logger;
import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.schedulers.Schedulers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Can create {@link Observable}s that streams the messages delivered to the connected rabbit queue.
 *
 * Note that a single {@link ChannelFactory} is backing all the {@link Observable}s created by this factory,
 * so if the {@link DefaultChannelFactory} is used it means that all observables will share the same {@link com.rabbitmq.client.Connection}
 * but use different {@link com.rabbitmq.client.Channel}s.
 *
 */
public class DefaultConsumerFactory implements ConsumerFactory {

    private final static Logger log = new Logger(DefaultConsumerFactory.class);

    private final ChannelFactory channelFactory;
    private final ConsumerSettings settings;

    private ConsumeEventListener consumeEventListener = getConsumeEventListener();

    private Scheduler consumerObserveOnScheduler = Schedulers.computation();

    public DefaultConsumerFactory(ChannelFactory channelFactory, ConsumerSettings settings) {
        this.settings = settings;
        this.channelFactory = channelFactory;
    }

    public DefaultConsumerFactory setConsumeEventListener(ConsumeEventListener consumeEventListener) {
        this.consumeEventListener = consumeEventListener;
        return this;
    }

    public ConsumerFactory setConsumerObserveOnScheduler(Scheduler consumerObserveOnScheduler) {
        this.consumerObserveOnScheduler = consumerObserveOnScheduler;
        return this;
    }

    @Override
    public Observable<Message> createConsumer(String queue) {
        return createConsumer(queue, settings.getRetry_count());
    }

    @Override
    public Observable<Message> createConsumer(final String exchange, final String routingKey) {
        final ConnectionRetryHandler retryHandler = new ConnectionRetryHandler(settings.getRetry_count());
        return Observable.create(new Observable.OnSubscribe<Message>() {
            @Override
            public void call(Subscriber<? super Message> subscriber) {
                try {
                    final ConsumeChannel setupChannel = channelFactory.createConsumeChannel(exchange,routingKey);
                    final AtomicBoolean setupChannelClosed = new AtomicBoolean(false);
                    //Note we are setting re-try to 0 here so we get errors immediately and
                    //can re-create the queue+binding before re-connecting the consumer
                    createConsumer(setupChannel.getQueue(), 0)
                            //we can not close the 'temp' channel before we are sure that the consumer has created
                            //a channel otherwise the connection will be closed and the temp queue removed
                            .doOnNext(message -> {
                                if (!setupChannelClosed.get()){
                                    setupChannelClosed.set(true);
                                    setupChannel.close();
                                }
                            })
                            .doOnUnsubscribe(setupChannel::close)
                            .doOnError(throwable -> setupChannel.closeWithError())
                            .subscribe(subscriber);
                }catch (Exception e){
                    log.errorWithParams("Unexpected error when registering the rabbit consumer on the broker.",
                            "error", e);
                    subscriber.onError(e);
                }
            }
        })
                .retryWhen(retryHandler);
    }

    private ConsumeEventListener getConsumeEventListener() {
        return new NoopConsumeEventListener();
    }

    private Observable<Message> createConsumer(String queue, int reTryCount) {
        log.infoWithParams("Creating consumer.",
                "consumeChannels", settings.getNum_channels(),
                "preFetch", settings.getPre_fetch_count(),
                "consumeEventListener", consumeEventListener);
        SingleChannelConsumer consumer = new SingleChannelConsumer(
                channelFactory,
                queue,
                settings.getPre_fetch_count(),
                settings.getConsumer_tag_prefix() + "-consumer",
                reTryCount,
                settings.getClose_timeout_millis(),
                consumerObserveOnScheduler,
                consumeEventListener);
        List<Observable<Message>> consumers = new ArrayList<>();
        for(int i=0; i<settings.getNum_channels(); i++){
            consumers.add(consumer.consume());
        }
        return Observable.merge(consumers);
    }

}
