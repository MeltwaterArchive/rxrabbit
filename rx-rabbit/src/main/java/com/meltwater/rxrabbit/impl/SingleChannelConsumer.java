package com.meltwater.rxrabbit.impl;

import com.meltwater.rxrabbit.Acknowledger;
import com.meltwater.rxrabbit.ChannelFactory;
import com.meltwater.rxrabbit.ConsumeChannel;
import com.meltwater.rxrabbit.Message;
import com.meltwater.rxrabbit.metrics.RxRabbitMetricsReporter;
import com.meltwater.rxrabbit.metrics.RxStatsDMetricsReporter;
import com.meltwater.rxrabbit.util.Fibonacci;
import com.meltwater.rxrabbit.util.Logger;
import com.rabbitmq.client.*;
import com.timgroup.statsd.StatsDClient;
import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;

import java.io.IOException;
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

    private final RxRabbitMetricsReporter metricsReporter; //TODO use an event callback interface here that handles metrics
    private final ChannelFactory channelFactory;
    private final long closeTimeout;
    private final int maxReconnectAttempts;
    private final Scheduler scheduler;
    private final String tagPrefix;
    private final String queue;

    public SingleChannelConsumer(ChannelFactory channelFactory,
                                 String queue,
                                 String tagPrefix,
                                 int maxReconnectAttempts,
                                 long closeTimeout,
                                 Scheduler scheduler,
                                 StatsDClient statsDClient) {
        this.queue = queue;
        this.channelFactory = channelFactory;
        this.scheduler = scheduler;
        this.closeTimeout = closeTimeout;
        this.tagPrefix = tagPrefix;
        this.maxReconnectAttempts = maxReconnectAttempts;
        this.metricsReporter = new RxStatsDMetricsReporter(statsDClient, "rabbit-consume");
    }

    @Override
    public synchronized Observable<Message> consume() {
        final AtomicBoolean running = new AtomicBoolean(false);
        final AtomicInteger connectAttempt = new AtomicInteger();
        final AtomicReference<InternalConsumer> consumerRef = new AtomicReference<>(null);

        return create(new Observable.OnSubscribe<Message>() {
            @Override
            public void call(Subscriber<? super Message> subscriber) {
                if (!subscriber.isUnsubscribed()) {
                    if (running.get()) {
                        subscriber.onError(new IllegalStateException("This observable can only be subscribed to once."));
                    }else{
                        running.set(true);
                    }
                    try {
                        startConsuming(subscriber, consumerRef);
                        connectAttempt.set(0);
                    } catch (Exception e) {
                        log.errorWithParams("Unexpected error when registering the rabbit consumer", e);
                        subscriber.onError(e); //TODO filter stack trace
                    }
                }
            }
        })
                .retryWhen(observable -> observable.flatMap(throwable -> {
                    if (throwable instanceof IllegalStateException) {
                        return Observable.error(throwable);
                    }
                    terminate(running,consumerRef);
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
                .doOnUnsubscribe(() -> {
                    close(consumerRef);
                });
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
                queue,
                consumerTag,
                cons);
        consumerRef.set(cons);
    }

    static class InternalConsumer implements Consumer {

        private final RxRabbitMetricsReporter metricsReporter;
        private final Subscriber<? super Message> subscriber;
        private final Scheduler.Worker ackWorker;
        private final Scheduler.Worker deliveryWorker;

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
                                RxRabbitMetricsReporter metricsReporter,
                                AtomicLong deliveryOffset,
                                AtomicLong largestSeenDeliverTag) {
            this.channel = channel;
            this.closeTimeout = closeTimeout;
            this.subscriber = subscriber;
            this.metricsReporter = metricsReporter;
            this.deliveryOffset = deliveryOffset;
            this.largestSeenDeliverTag = largestSeenDeliverTag;
            this.deliveryWorker = scheduler.createWorker(); //TODO name the threads
            this.ackWorker = scheduler.createWorker(); //TODO name the threads
            deliveryOffset.set(largestSeenDeliverTag.get());
        }


        public InternalConsumer(InternalConsumer that, ConsumeChannel channel, Subscriber<? super Message> subscriber, Scheduler scheduler) {
            this(channel, subscriber, scheduler, that.closeTimeout, that.metricsReporter, that.deliveryOffset, that.largestSeenDeliverTag);
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
        public void handleCancel(String consumerTag) throws IOException {}

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
                Thread.currentThread().setName("rabbit-consumer"); //TODO multiple consumer threads
                if (!subscriber.isUnsubscribed() && !stopping.get()) {
                    long internalDeliverTag = envelope.getDeliveryTag() + deliveryOffset.get();
                    if (internalDeliverTag > largestSeenDeliverTag.get()){
                        largestSeenDeliverTag.set(internalDeliverTag);
                    }
                    log.traceWithParams("Consumer received message","messageId",
                            headers.getMessageId(),
                            "externalDeliveryTag", envelope.getDeliveryTag(),
                            "internalDeliveryTag", internalDeliverTag,
                            "largestSeenDeliverTag", largestSeenDeliverTag.get(),
                            "messageHeaders", headers);
                    outstandingAcks.incrementAndGet();
                    metricsReporter.reportCount("received", 1);
                    metricsReporter.reportCount("received-bytes", body.length);
                    final Envelope internalEnvelope = new Envelope(internalDeliverTag, envelope.isRedeliver(), envelope.getExchange(), envelope.getRoutingKey());
                    Acknowledger acknowledger = createAcknowledger(channel, internalEnvelope, headers);
                    try {
                        subscriber.onNext(new Message(acknowledger, internalEnvelope, headers, body));
                    } catch (Exception e) {
                        log.errorWithParams("Unhandled error when sending message to subscriber", e,
                                "basicProperties", headers);
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

        //TODO ignore acks when channel shut down
        private Acknowledger createAcknowledger(final ConsumeChannel channel,
                                                final Envelope envelope,
                                                final BasicProperties headers) {
            final long processingStart = System.currentTimeMillis();
            return new Acknowledger() {
                @Override
                public void onDone() {
                    ackWorker.schedule(() -> {
                        Thread.currentThread().setName("rabbit-acknowledger");
                        long ackStart = System.currentTimeMillis();
                        try {
                            //TODO add multi ack here??
                            //TODO check if the connection is open... log error/warn (but NO stack trace) and drop if its closed msgAndChannel.getChannel().getConnection().isOpen()
                            final long currentDeliveryOffset = deliveryOffset.get();
                            if(currentDeliveryOffset >= envelope.getDeliveryTag()){
                                log.debugWithParams("Received ack for zombie message, ignoring",
                                        "channel", channel.toString(),
                                        "deliveryTag", envelope.getDeliveryTag(),
                                        "messageId", headers.getMessageId(),
                                        "basicProperties", headers.toString());
                            }
                            else {
                                long actualDeliverTag = envelope.getDeliveryTag() - currentDeliveryOffset;
                                channel.basicAck(actualDeliverTag, false);
                                metricsReporter.reportCount("ack");
                                log.traceWithParams("Acknowledged message on channel.",
                                        "channel", channel.toString(),
                                        "deliveryTag", envelope.getDeliveryTag(),
                                        "messageId", headers.getMessageId(),
                                        "basicProperties", headers.toString(),
                                        "processingTook", System.currentTimeMillis() - processingStart,
                                        "ackTook", System.currentTimeMillis() - ackStart);
                            }
                        } catch (Exception e) {
                            //TODO if we get errors here .. stop the consumer and kill the channel!!
                            metricsReporter.reportCount("ack/nack-fail");
                            if (!channel.isOpen()) {
                                log.infoWithParams("Failed to ack on closed connection.",
                                        "channel", channel.toString(),
                                        "deliveryTag", envelope.getDeliveryTag(),
                                        "messageId", headers.getMessageId(),
                                        "basicProperties", headers.toString(),
                                        "processingTook", System.currentTimeMillis() - processingStart,
                                        "ackTook", System.currentTimeMillis() - ackStart);
                            } else {
                                log.errorWithParams("Could not acknowledge message.", e,
                                        "channel", channel.toString(),
                                        "deliveryTag", envelope.getDeliveryTag(),
                                        "messageId", headers.getMessageId(),
                                        "basicProperties", headers.toString(),
                                        "processingTook", System.currentTimeMillis() - processingStart,
                                        "ackTook", System.currentTimeMillis() - ackStart);
                            }
                        } finally {
                            decrementAndReport(ackStart);
                        }
                    });
                }

                @Override
                public void onFail() {
                    final long nackStart = System.currentTimeMillis();
                    ackWorker.schedule(() -> {
                        try {
                            final long currentDeliveryOffset = deliveryOffset.get();
                            if(currentDeliveryOffset >= envelope.getDeliveryTag()){
                                log.debugWithParams("Received nack for zombie message, ignoring",
                                        "channel", channel,
                                        "deliveryTag", envelope.getDeliveryTag(),
                                        "messageId", headers.getMessageId(),
                                        "basicProperties", headers.toString());
                            }
                            else {
                                long actualDeliverTag = envelope.getDeliveryTag() - currentDeliveryOffset;
                                channel.basicNack(actualDeliverTag, false);
                                metricsReporter.reportCount("nack");
                                log.traceWithParams("Received nack on channel.",
                                        "channel", channel,
                                        "deliveryTag", envelope.getDeliveryTag(),
                                        "basicProperties", headers.toString());
                            }
                        } catch (Exception e) {
                            //TODO if we get errors here .. stop the consumer and kill the channel!!
                            metricsReporter.reportCount("ack/nack-fail");
                            if (!channel.isOpen()) {
                                log.infoWithParams("Failed to nack on closed connection.",
                                        "channel", channel,
                                        "deliveryTag", envelope.getDeliveryTag(),
                                        "basicProperties", headers.toString());
                            } else {
                                log.errorWithParams("Could not report fail for message.", e,
                                        "channel", channel,
                                        "deliveryTag", envelope.getDeliveryTag(),
                                        "basicProperties", headers.toString());
                            }
                        } finally {
                            decrementAndReport(nackStart);
                        }
                    });
                }

                private void decrementAndReport(long ackNackStart) {
                    metricsReporter.reportCount("done");
                    metricsReporter.reportTime("ack/nack-took-ms", System.currentTimeMillis() - ackNackStart);
                    metricsReporter.reportTime("processing-took-ms", System.currentTimeMillis() - processingStart);
                    synchronized (outstandingAcks) {
                        outstandingAcks.decrementAndGet();
                        outstandingAcks.notifyAll();
                    }
                }
            };
        }

        String getConsumerTag() {
            return consumerTag;
        }

        void close() {
            if (stopping.get()){
                log.infoWithParams("Already stopped, doing nothing.");
                return;
            }
            stopping.set(true);
            log.infoWithParams("Shutting down consumer. Waiting for outstanding acks before closing the channel.",
                    "channel", channel.toString(),
                    "consumerTag", getConsumerTag(),
                    "unAckedMessages", outstandingAcks.get());
            try {
                channel.basicCancel(getConsumerTag());
            } catch (Exception e) {
                log.warnWithParams("Unexpected error when canceling consumer", e,
                        "channel", channel.toString(),
                        "consumerTag", getConsumerTag(),
                        "unAckedMessages", outstandingAcks.get()
                );
            }
            long startTime = System.currentTimeMillis();
            synchronized (outstandingAcks) {
                while (outstandingAcks.get() > 0) {
                    try {
                        outstandingAcks.wait(100);
                    } catch (InterruptedException ignored) {
                        log.warnWithParams("Close interrupted with un-acked messages still pending",
                                "consumerTag", getConsumerTag(),
                                "unAckedMessages", outstandingAcks.get());
                        break;
                    }
                    //TODO maybe log the progress here
                    long timeWaited = System.currentTimeMillis() - startTime;
                    if (closeTimeout > 0 && timeWaited >= closeTimeout) {
                        log.warnWithParams("Close timeout reached with un-acked messages still pending",
                                "channel", channel.toString(),
                                "consumerTag", getConsumerTag(),
                                "millisWaited", timeWaited,
                                "unAckedMessages", outstandingAcks.get());
                        break;
                    }
                }
            }
            log.infoWithParams("Closing the channel and stopping workers.");
            channel.close();
        }

        void closeWitError() {
            channel.closeWithError();
        }
    }
}


