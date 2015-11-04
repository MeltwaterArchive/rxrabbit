package com.meltwater.rxrabbit.impl;

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

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static rx.Observable.create;

//TODO javadoc!!
public class SingleChannelConsumer implements RabbitConsumer {

    private final static AtomicInteger consumerCount = new AtomicInteger();
    private final static Logger log = new Logger(SingleChannelConsumer.class);

    private final ConsumeEventListener metricsReporter;
    private final ChannelFactory channelFactory;
    private final long closeTimeout;
    private final int maxReconnectAttempts;
    private final Scheduler scheduler;
    private final String tagPrefix;
    private final String queue;
    private final int preFetchCount;

    public SingleChannelConsumer(ChannelFactory channelFactory,
                                 String queue,
                                 int preFetchCount, String tagPrefix,
                                 int maxReconnectAttempts,
                                 long closeTimeout,
                                 Scheduler scheduler,
                                 ConsumeEventListener metricsReporter) {
        this.queue = queue;
        this.channelFactory = channelFactory;
        this.preFetchCount = preFetchCount;
        this.scheduler = scheduler;
        this.closeTimeout = closeTimeout;
        this.tagPrefix = tagPrefix;
        this.maxReconnectAttempts = maxReconnectAttempts;
        this.metricsReporter = metricsReporter;
    }

    @Override
    public synchronized Observable<Message> consume() {
        final AtomicBoolean running = new AtomicBoolean(false);
        final AtomicInteger connectAttempt = new AtomicInteger();
        final AtomicReference<InternalConsumer> consumerRef = new AtomicReference<>(null);

        return create(
                new Observable.OnSubscribe<Message>() {
                    @Override
                    public void call(Subscriber<? super Message> subscriber) {
                        if (!subscriber.isUnsubscribed()) {
                            if (running.get()) {
                                subscriber.onError(new IllegalStateException("This observable can only be subscribed to once."));
                            } else {
                                running.set(true);
                            }
                            try {
                                startConsuming(subscriber, consumerRef);
                                connectAttempt.set(0);
                            } catch (Exception e) {
                                log.errorWithParams("Unexpected error when registering the rabbit consumer", e);
                                subscriber.onError(e);
                            }
                        }
                    }
                })
                .retryWhen(observable -> observable.flatMap(throwable -> {
                    if (throwable instanceof IllegalStateException) {
                        return Observable.error(throwable);
                    }
                    terminate(running, consumerRef);
                    int conAttempt = connectAttempt.get();
                    if (maxReconnectAttempts <= 0 || conAttempt < maxReconnectAttempts) {
                        final int delaySec = Fibonacci.getDelaySec(conAttempt);
                        connectAttempt.incrementAndGet();
                        log.infoWithParams("Scheduling attempting to restart consumer",
                                "attempt", connectAttempt,
                                "delaySeconds", delaySec);
                        return Observable.timer(delaySec, TimeUnit.SECONDS);
                    } else {
                        return Observable.error(throwable);
                    }
                }))
                .onBackpressureBuffer()
                .doOnUnsubscribe(() -> close(consumerRef));
    }

    private synchronized void terminate(AtomicBoolean started, AtomicReference<InternalConsumer> consumer) {
        if(consumer.get()!=null) {
            consumer.get().closeWitError();
        }
        started.set(false);
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
        String consumerTag = this.tagPrefix + "-" + consumerCount.incrementAndGet();
        log.infoWithParams("Starting up consumer.",
                "queue", queue,
                "deliveryOffset", consumerRef.get() == null ? 0 : consumerRef.get().deliveryOffset.get(),
                "consumerTag", consumerTag
        );
        InternalConsumer cons;
        if (consumerRef.get()==null) {
            cons = new InternalConsumer(channel, subscriber, scheduler, closeTimeout, metricsReporter, new AtomicLong(), new AtomicLong());
        }else{
            cons = new InternalConsumer(consumerRef.get(), channel, subscriber, scheduler);
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

        private Scheduler scheduler;
        private final long closeTimeout;

        private final AtomicBoolean stopping = new AtomicBoolean(false);
        private final AtomicLong outstandingAcks = new AtomicLong(0);
        private final AtomicLong deliveryOffset;
        private final AtomicLong largestSeenDeliverTag;

        private final ConsumeChannel channel;
        private String consumerTag;

        public InternalConsumer(ConsumeChannel channel,
                                Subscriber<? super Message> subscriber,
                                Scheduler scheduler,
                                long closeTimeout,
                                ConsumeEventListener consumeEventListener,
                                AtomicLong deliveryOffset,
                                AtomicLong largestSeenDeliverTag) {
            this.channel = channel;
            this.scheduler = scheduler;
            this.closeTimeout = closeTimeout;
            this.subscriber = subscriber;
            this.consumeEventListener = consumeEventListener;
            this.deliveryOffset = deliveryOffset;
            this.largestSeenDeliverTag = largestSeenDeliverTag;
            this.deliveryWorker = scheduler.createWorker(); //TODO name the threads better
            deliveryWorker.schedule(() -> Thread.currentThread().setName("rabbit-consumer"));
            this.ackWorker = scheduler.createWorker(); //TODO name the threads better
            ackWorker.schedule(() -> Thread.currentThread().setName("rabbit-acknowledge"));
            deliveryOffset.set(largestSeenDeliverTag.get());
        }

        public InternalConsumer(InternalConsumer that, ConsumeChannel channel, Subscriber<? super Message> subscriber, Scheduler scheduler) {
            this(channel, subscriber, scheduler, that.closeTimeout, that.consumeEventListener, that.deliveryOffset, that.largestSeenDeliverTag);
        }

        @Override
        public void handleConsumeOk(String consumerTag) {
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
                    outstandingAcks.incrementAndGet();
                    final Envelope internalEnvelope = new Envelope(internalDeliverTag, envelope.isRedeliver(), envelope.getExchange(), envelope.getRoutingKey());
                    Acknowledger acknowledger = createAcknowledger(channel, internalEnvelope, headers, body);
                    Message message = new Message(acknowledger, internalEnvelope, headers, body);
                    consumeEventListener.received(message, outstandingAcks.get());
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
            return new Acknowledger() {
                @Override
                public void onDone() {
                    long ackStart = System.currentTimeMillis();
                    Message message = new Message(this, envelope, headers, payload);
                    ackWorker.schedule(() -> {
                        try {
                            //TODO should we add multi ack here ??
                            final long currentDeliveryOffset = deliveryOffset.get();
                            if(currentDeliveryOffset >= envelope.getDeliveryTag()){
                                consumeEventListener.ignoredAck(message);
                            }
                            else {
                                long actualDeliverTag = envelope.getDeliveryTag() - currentDeliveryOffset;
                                consumeEventListener.beforeAck(message);
                                channel.basicAck(actualDeliverTag, false);
                            }
                        } catch (Exception e) {
                            consumeEventListener.afterFailedAck(message, e, channel.isOpen());
                        } finally {
                            decrementAndReportOutstanding();
                        }
                        consumeEventListener.done(message, outstandingAcks.get(), ackStart, processingStart);
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
                            decrementAndReportOutstanding();
                        }
                        consumeEventListener.done(message, outstandingAcks.get(), nackStart, processingStart);
                    });
                }

                private void decrementAndReportOutstanding() {
                    synchronized (outstandingAcks) {
                        outstandingAcks.decrementAndGet();
                        outstandingAcks.notifyAll();
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
                    "unAckedMessages", outstandingAcks.get());
            try {
                channel.basicCancel(consumerTag);
            } catch (Exception e) {
                log.warnWithParams("Unexpected error when canceling consumer", e,
                        "channel", channel.toString(),
                        "consumerTag", consumerTag,
                        "unAckedMessages", outstandingAcks.get()
                );
            }
            long startTime = System.currentTimeMillis();
            final Scheduler.Worker closeProgressWorker = scheduler.createWorker();
            closeProgressWorker.schedule(() -> Thread.currentThread().setName("consumer-close-progress"));
            closeProgressWorker.schedulePeriodically(() -> {
                log.infoWithParams("Closing down consumer, waiting for outstanding acks",
                        "unAckedMessages", outstandingAcks.get(),
                        "millisWaited", System.currentTimeMillis() - startTime,
                        "closeTimeout", closeTimeout);
            }, 5,5,TimeUnit.SECONDS);
            synchronized (outstandingAcks) {

                while (outstandingAcks.get() > 0) {
                    try {
                        outstandingAcks.wait(100);
                    } catch (InterruptedException ignored) {
                        log.warnWithParams("Close interrupted with un-acked messages still pending",
                                "consumerTag", consumerTag,
                                "unAckedMessages", outstandingAcks.get());
                        break;
                    }
                    //TODO maybe log shutdown the progress here??
                    long timeWaited = System.currentTimeMillis() - startTime;
                    if (closeTimeout > 0 && timeWaited >= closeTimeout) {
                        log.warnWithParams("Close timeout reached with un-acked messages still pending",
                                "channel", channel.toString(),
                                "consumerTag", consumerTag,
                                "millisWaited", timeWaited,
                                "unAckedMessages", outstandingAcks.get());
                        break;
                    }
                }
            }
            log.infoWithParams("Closing the channel and stopping workers.");
            closeProgressWorker.unsubscribe();
            channel.close();
        }

        void closeWitError() {
            channel.closeWithError();
        }
    }
}


