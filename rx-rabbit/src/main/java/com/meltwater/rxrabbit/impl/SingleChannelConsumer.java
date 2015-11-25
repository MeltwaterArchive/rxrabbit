package com.meltwater.rxrabbit.impl;

import com.google.common.collect.Collections2;
import com.meltwater.rxrabbit.Acknowledger;
import com.meltwater.rxrabbit.ChannelFactory;
import com.meltwater.rxrabbit.ConsumeChannel;
import com.meltwater.rxrabbit.ConsumeEventListener;
import com.meltwater.rxrabbit.Message;
import com.meltwater.rxrabbit.util.Fibonacci;
import com.meltwater.rxrabbit.util.Logger;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BasicProperties;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.meltwater.rxrabbit.ConsumerSettings.RETRY_FOREVER;
import static rx.Observable.create;

/**
 * A rabbit consumer that is using one {@link ConsumeChannel} and one {@link Consumer} at the time.
 *
 * If any io error is encountered in the communication with RabbitMQ then the {@link ConsumeChannel} will be discarded
 * and a re-connect loop will be initiated. The re-connect loop can continue forever or stop after N attempts according to config.
 *
 * The messages delivered from RabbitMQ are wrapped up into a {@link Message} object and handed over to the caller
 * on a {@link Scheduler} of your choice.
 *
 */
public class SingleChannelConsumer implements RabbitConsumer {

    private final static AtomicInteger consumerCount = new AtomicInteger();

    private final static Logger log = new Logger(SingleChannelConsumer.class);
    public static final int UNACKED_WARNING_TIME_MS = 6 * 60 * 1000; //5 minutes

    private final ConsumeEventListener metricsReporter;
    private final ChannelFactory channelFactory;
    private final long closeTimeout;
    private final int maxReconnectAttempts;
    private final Scheduler observeOnScheduler;
    private final String tagPrefix;
    private final String queue;
    private final int preFetchCount;

    /**
     *
     * @param channelFactory used to create new channels when needed
     * @param queue the queue to consume from
     * @param preFetchCount the rabbit preFetchCount
     * @param tagPrefix a prefix for the consumer tag, an ever increasing integer will be appended at the end
     * @param maxReconnectAttempts how many times to try re-connects. 0 or negative number for trying forever
     * @param closeTimeout the max time to wait before aborting/crashing close procedures
     * @param observeOnScheduler the scheduler that the onNext(Message) will be called on
     * @param consumeEventListener event listener that will be notified about message receive, failures and ack/nack events
     */
    public SingleChannelConsumer(ChannelFactory channelFactory,
                                 String queue,
                                 int preFetchCount,
                                 String tagPrefix,
                                 int maxReconnectAttempts,
                                 long closeTimeout,
                                 Scheduler observeOnScheduler,
                                 ConsumeEventListener consumeEventListener) {
        this.queue = queue;
        this.channelFactory = channelFactory;
        this.preFetchCount = preFetchCount;
        this.observeOnScheduler = observeOnScheduler;
        this.closeTimeout = closeTimeout;
        this.tagPrefix = tagPrefix;
        this.maxReconnectAttempts = maxReconnectAttempts;
        this.metricsReporter = consumeEventListener;
    }

    @Override
    public synchronized Observable<Message> consume() {
        return Observable.defer(this::createObservable).observeOn(observeOnScheduler);
    }

    private static class RetryHandler implements Func1<Observable<? extends Throwable>, Observable<?>> {
        private final AtomicInteger connectAttempt = new AtomicInteger();
        private final int maxReconnectAttempts;

        public RetryHandler(int maxReconnectAttempts) {
            this.maxReconnectAttempts = maxReconnectAttempts;
        }

        @Override
        public Observable<?> call(Observable<? extends Throwable> observable) {
            return observable.flatMap(throwable -> {
                int conAttempt = connectAttempt.get();
                if (maxReconnectAttempts == RETRY_FOREVER || conAttempt < maxReconnectAttempts) {
                    final int delaySec = Fibonacci.getDelaySec(conAttempt);
                    connectAttempt.incrementAndGet();
                    log.infoWithParams("Scheduling attempting to restart consumer",
                            "attempt", connectAttempt,
                            "delaySeconds", delaySec);
                    return Observable.timer(delaySec, TimeUnit.SECONDS);
                } else {
                    return Observable.error(throwable);
                }
            });
        }

        public void reset() {
            connectAttempt.set(0);
        }
    }

    private Observable<Message> createObservable() {
        final AtomicReference<InternalConsumer> consumerRef = new AtomicReference<>(null);
        final RetryHandler retryHandler = new RetryHandler(maxReconnectAttempts);

        return create(new Observable.OnSubscribe<Message>() {
            @Override
            public void call(Subscriber<? super Message> subscriber) {
                if (!subscriber.isUnsubscribed()) {
                    try {
                        startConsuming(subscriber, consumerRef);
                    } catch (Exception e) {
                        log.errorWithParams("Unexpected error when registering the rabbit consumer on the broker.",
                                "",
                                "error", e);
                        subscriber.onError(e);
                    }
                }
            }
        })
                // If we ever successfully get a message, we should reset the error handler
                .doOnNext(message -> retryHandler.reset())
                // On error, make sure to close the existing channel with an error before using the retryHandler
                .doOnError(throwable -> terminate(consumerRef))
                .retryWhen(retryHandler)
                // handle back pressure by buffering
                .onBackpressureBuffer()
                // If someone unsubscribes, close the channel cleanly
                .doOnUnsubscribe(() -> close(consumerRef));
    }

    private synchronized void terminate(AtomicReference<InternalConsumer> consumer) {
        if(consumer.get()!=null) {
            consumer.get().closeWithError();
        }
    }

    private synchronized void close(AtomicReference<InternalConsumer> consumer) {
        log.infoWithParams("Un-subscribed invoked. Stopping the consumer and closing the channel.");
        if (consumer.get() != null) {
            consumer.get().close();
        }
    }

    private synchronized void startConsuming(Subscriber<? super Message> subscriber,
                                             AtomicReference<InternalConsumer> consumerRef) throws IOException, TimeoutException {
        ConsumeChannel channel = channelFactory.createConsumeChannel(queue);
        channel.basicQos(preFetchCount);
        int consumerCount = SingleChannelConsumer.consumerCount.incrementAndGet();
        String consumerTag = this.tagPrefix + "-" + consumerCount;
        log.infoWithParams("Starting up consumer.",
                "queue", queue,
                "deliveryOffset", consumerRef.get() == null ? 0 : consumerRef.get().deliveryOffset.get(),
                "consumerTag", consumerTag
        );
        InternalConsumer cons;
        String threadNamePrefix = "consume-thread-" + consumerCount;
        if (consumerRef.get()==null) {
            cons = new InternalConsumer(channel, subscriber, closeTimeout, threadNamePrefix, metricsReporter, new AtomicLong(), new AtomicLong());
        }else{
            cons = new InternalConsumer(consumerRef.get(), threadNamePrefix, channel, subscriber);
        }
        channel.basicConsume(
                consumerTag,
                cons);
        consumerRef.set(cons);
    }

    static class InternalConsumer implements Consumer {

        private final ConsumeEventListener consumeEventListener;
        private final Subscriber<? super Message> subscriber;
        private final Scheduler.Worker ackWorker;
        private final Scheduler.Worker deliveryWorker;
        private final Scheduler.Worker unackedMessagesWorker;

        private final long closeTimeout;

        private final AtomicBoolean stopping = new AtomicBoolean(false);
        private final AtomicLong deliveryOffset;
        private final AtomicLong largestSeenDeliverTag;

        private final ConsumeChannel channel;
        private String consumerTag;
        private final Map<Long,Long> unackedMessages = new ConcurrentHashMap<>();

        public InternalConsumer(ConsumeChannel channel,
                                Subscriber<? super Message> subscriber,
                                long closeTimeout,
                                String threadNamePrefix,
                                ConsumeEventListener consumeEventListener,
                                AtomicLong deliveryOffset,
                                AtomicLong largestSeenDeliverTag) {
            this.channel = channel;
            this.closeTimeout = closeTimeout;
            this.subscriber = subscriber;
            this.consumeEventListener = consumeEventListener;
            this.deliveryOffset = deliveryOffset;
            this.largestSeenDeliverTag = largestSeenDeliverTag;
            this.deliveryWorker = Schedulers.io().createWorker();
            deliveryWorker.schedule(() -> Thread.currentThread().setName(threadNamePrefix+"-delivery"));
            this.ackWorker = Schedulers.io().createWorker();
            ackWorker.schedule(() -> Thread.currentThread().setName(threadNamePrefix+"-ack"));
            this.unackedMessagesWorker = Schedulers.io().createWorker();
            unackedMessagesWorker.schedule(() -> Thread.currentThread().setName(threadNamePrefix+"-unacked"));
            deliveryOffset.set(largestSeenDeliverTag.get());
            unackedMessagesWorker.schedulePeriodically(this::logUnackedMessages, 1, 1, TimeUnit.MINUTES);

        }

        private void logUnackedMessages() {
            Collection<Long> oldMessages = Collections2.filter(unackedMessages.values(),
                    createdAt -> createdAt <= System.currentTimeMillis() - UNACKED_WARNING_TIME_MS);

            if(oldMessages.size() > 0){
                log.warnWithParams("Long-lived un-acked messages found",
                        "nrMessages", oldMessages.size(),
                        "olderThanMs", UNACKED_WARNING_TIME_MS);
            }
        }

        public InternalConsumer(InternalConsumer that, String threadNamePrefix, ConsumeChannel channel, Subscriber<? super Message> subscriber) {
            this(channel, subscriber, that.closeTimeout, threadNamePrefix, that.consumeEventListener, that.deliveryOffset, that.largestSeenDeliverTag);
        }

        @Override
        public void handleConsumeOk(String consumerTag) {
            Thread.currentThread().setName("rabbit-internal-thread");
            this.consumerTag = consumerTag;
            log.infoWithParams("Consumer registered and ready to receive messages.",
                    "channel", channel.toString(),
                    "queue", channel.getQueue(),
                    "consumerTag", consumerTag);
        }

        @Override
        public void handleCancelOk(String consumerTag) {
            log.infoWithParams("Consumer successfully stopped. It will not receive any more messages.",
                    "channel", channel.toString(),
                    "queue", channel.getQueue(),
                    "consumerTag", consumerTag);
        }

        @Override
        public void handleCancel(String consumerTag) throws IOException {
            log.warnWithParams("Consumer stopped unexpectedly. It will not receive any more messages.",
                    "channel", channel.toString(),
                    "queue", channel.getQueue(),
                    "consumerTag", consumerTag);
        }

        @Override
        public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
            log.errorWithParams("The rabbit connection was unexpectedly disconnected.", sig,
                    "channel", channel.toString(),
                    "queue", channel.getQueue(),
                    "consumerTag", consumerTag);
            subscriber.onError(sig);
        }

        @Override
        public void handleRecoverOk(String consumerTag) {}

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties headers, byte[] body) throws IOException {
            deliveryWorker.schedule(() -> {
                if (!subscriber.isUnsubscribed() && !stopping.get()) {
                    long internalDeliverTag = envelope.getDeliveryTag() + deliveryOffset.get();
                    if (internalDeliverTag > largestSeenDeliverTag.get()){
                        largestSeenDeliverTag.set(internalDeliverTag);
                    }
                    log.traceWithParams("Consumer received message",
                            "messageId", headers.getMessageId(),
                            "externalDeliveryTag", envelope.getDeliveryTag(),
                            "internalDeliveryTag", internalDeliverTag,
                            "largestSeenDeliverTag", largestSeenDeliverTag.get(),
                            "messageHeaders", headers);
                    unackedMessages.put(internalDeliverTag,System.currentTimeMillis());
                    final Envelope internalEnvelope = new Envelope(internalDeliverTag, envelope.isRedeliver(), envelope.getExchange(), envelope.getRoutingKey());
                    Acknowledger acknowledger = createAcknowledger(channel, internalEnvelope, headers, body);
                    Message message = new Message(acknowledger, internalEnvelope, headers, body);
                    consumeEventListener.received(message, unackedMessages.size());
                    try {
                        subscriber.onNext(message);
                    } catch (Exception e) {
                        log.errorWithParams("Unhandled error when sending message to subscriber. This should NEVER happen.", e,
                                "basicProperties", headers,
                                "body", new String(body, Charset.forName("utf-8")));
                        acknowledger.onFail();
                    }
                } else {
                    log.traceWithParams("Ignoring message received during shutdown.",
                            "channel", channel.toString(),
                            "deliveryTag", envelope.getDeliveryTag(),
                            "messageId", headers.getMessageId(),
                            "basicProperties", headers.toString());
                }
            });
        }

        private Acknowledger createAcknowledger(final ConsumeChannel channel,
                                                final Envelope envelope,
                                                final BasicProperties headers,
                                                final byte[] payload) {
            final long processingStart = System.currentTimeMillis();
            final long deliveryTag = envelope.getDeliveryTag();
            return new Acknowledger() {
                @Override
                public void onDone() {
                    long ackStart = System.currentTimeMillis();
                    Message message = new Message(this, envelope, headers, payload);
                    ackWorker.schedule(() -> {
                        try {
                            //TODO should we add multi ack here ??
                            final long currentDeliveryOffset = deliveryOffset.get();
                            if(currentDeliveryOffset >= deliveryTag){
                                consumeEventListener.ignoredAck(message);
                            }
                            else {
                                long actualDeliverTag = deliveryTag - currentDeliveryOffset;
                                consumeEventListener.beforeAck(message);
                                channel.basicAck(actualDeliverTag, false);
                            }
                        } catch (Exception e) {
                            consumeEventListener.afterFailedAck(message, e, channel.isOpen());
                        } finally {
                            removeAndNotifyOutstanding(deliveryTag);
                        }
                        consumeEventListener.done(message, unackedMessages.size(), ackStart, processingStart);
                    });
                }

                @Override
                public void onFail() {
                    final long nackStart = System.currentTimeMillis();
                    Message message = new Message(this, envelope, headers, payload);
                    ackWorker.schedule(() -> {
                        try {
                            final long currentDeliveryOffset = deliveryOffset.get();
                            if(currentDeliveryOffset >= envelope.getDeliveryTag()){
                                consumeEventListener.ignoredNack(message);
                            }
                            else {
                                long actualDeliverTag = envelope.getDeliveryTag() - currentDeliveryOffset;
                                consumeEventListener.beforeNack(message);
                                channel.basicNack(actualDeliverTag, false);
                            }
                        } catch (Exception e) {
                            consumeEventListener.afterFailedNack(message, e, channel.isOpen());
                        } finally {
                            removeAndNotifyOutstanding(deliveryTag);
                        }
                        consumeEventListener.done(message, unackedMessages.size(), nackStart, processingStart);
                    });
                }

                private void removeAndNotifyOutstanding(Long deliveryTag) {
                    synchronized (unackedMessages) {
                        unackedMessages.remove(deliveryTag);
                        unackedMessages.notifyAll();
                    }
                }
            };
        }

        void close() {
            if (stopping.get()){
                log.infoWithParams("Already stopped, doing nothing.");
                return;
            }
            stopping.set(true);
            log.infoWithParams("Shutting down consumer. Waiting for outstanding acks before closing the channel.",
                    "channel", channel.toString(),
                    "consumerTag", consumerTag,
                    "unAckedMessages", unackedMessages.size());
            try {
                channel.basicCancel(consumerTag);
            } catch (Exception e) {
                log.warnWithParams("Unexpected error when canceling consumer", e,
                        "channel", channel.toString(),
                        "consumerTag", consumerTag,
                        "unAckedMessages", unackedMessages.size()
                );
            }
            long startTime = System.currentTimeMillis();
            final Scheduler.Worker closeProgressWorker = Schedulers.io().createWorker();
            closeProgressWorker.schedulePeriodically(() -> log.infoWithParams("Closing down consumer, waiting for outstanding acks",
                    "unAckedMessages", unackedMessages.size(),
                    "millisWaited", System.currentTimeMillis() - startTime,
                    "closeTimeout", closeTimeout), 5,5,TimeUnit.SECONDS);
            synchronized (unackedMessages) {

                while (unackedMessages.size() > 0) {
                    try {
                        unackedMessages.wait(100);
                    } catch (InterruptedException ignored) {
                        log.warnWithParams("Close interrupted with un-acked messages still pending",
                                "consumerTag", consumerTag,
                                "unAckedMessages", unackedMessages.size());
                        break;
                    }
                    //TODO maybe log the shutdown progress here??
                    long timeWaited = System.currentTimeMillis() - startTime;
                    if (closeTimeout > 0 && timeWaited >= closeTimeout) {
                        log.warnWithParams("Close timeout reached with un-acked messages still pending",
                                "channel", channel.toString(),
                                "consumerTag", consumerTag,
                                "millisWaited", timeWaited,
                                "unAckedMessages", unackedMessages.size());
                        break;
                    }
                }
            }
            log.infoWithParams("Closing the channel and stopping workers.");
            closeProgressWorker.unsubscribe();
            deliveryWorker.unsubscribe();
            ackWorker.unsubscribe();
            channel.close();
        }

        void closeWithError() {
            channel.closeWithError();
        }
    }
}


