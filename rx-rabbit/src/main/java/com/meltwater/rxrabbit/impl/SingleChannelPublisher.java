package com.meltwater.rxrabbit.impl;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.meltwater.rxrabbit.ChannelFactory;
import com.meltwater.rxrabbit.PublishChannel;
import com.meltwater.rxrabbit.RabbitPublisher;
import com.meltwater.rxrabbit.metrics.RxRabbitMetricsReporter;
import com.meltwater.rxrabbit.metrics.RxStatsDMetricsReporter;
import com.meltwater.rxrabbit.util.DelaySequence;
import com.meltwater.rxrabbit.util.Logger;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConfirmListener;
import com.timgroup.statsd.StatsDClient;
import rx.Scheduler;
import rx.Single;
import rx.SingleSubscriber;
import rx.Subscription;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

//TODO javadoc
//TODO needs some code cleanup and better method names etc..
public class SingleChannelPublisher implements RabbitPublisher{

    private static final Logger log = new Logger(SingleChannelPublisher.class);

    private final String exchange;
    private final int maxRetries;
    private final long closeTimeoutMillis;

    private final ChannelFactory channelFactory;
    private final RxRabbitMetricsReporter metricsReporter; //TODO use an event callback interface here that handles metrics

    private final Scheduler.Worker publishWorker;
    private final Scheduler.Worker ackWorker;

    private final Cache<Long, UnconfirmedMessage> tagToMessage;
    private final AtomicLong largestSeqSeen = new AtomicLong(0);
    private final AtomicLong seqOffset = new AtomicLong(0);

    private PublishChannel channel = null;

    public SingleChannelPublisher(ChannelFactory channelFactory,
                                  String exchange,
                                  int maxRetries,
                                  Scheduler scheduler,
                                  StatsDClient statsDClient,
                                  long confirmsTimeoutSec,
                                  long closeTimeoutMillis,
                                  long cleanupTimeoutCacheIntervalSec) {
        this.channelFactory = channelFactory;
        this.exchange = exchange;
        this.maxRetries = maxRetries;
        this.closeTimeoutMillis = closeTimeoutMillis;
        this.publishWorker = scheduler.createWorker();
        publishWorker.schedule(() -> Thread.currentThread().setName("rabbit-send-thread")); //TODO thread name
        this.ackWorker = scheduler.createWorker();
        ackWorker.schedule(() -> Thread.currentThread().setName("rabbit-confirm-thread")); //TODO thread name

        this.metricsReporter = new RxStatsDMetricsReporter(statsDClient, "rabbit-publish");

        this.tagToMessage = CacheBuilder.<Long, UnconfirmedMessage>newBuilder()
                .expireAfterAccess(confirmsTimeoutSec, TimeUnit.SECONDS)
                .removalListener(notification -> {
                    if (notification.getCause().equals(RemovalCause.EXPIRED)) {
                        UnconfirmedMessage message = (UnconfirmedMessage) notification.getValue();
                        if (message != null) { //TODO how can this be null??
                            ackWorker.schedule(() -> {
                                log.warnWithParams("Message did not receive publish-confirm in time",
                                        "messageId", message.props.getMessageId());
                                message.nack(new TimeoutException("Message did not receive publish confirm in time"));
                            });
                        }
                    }
                })
                .build();

        Scheduler.Worker cacheCleanupWorker = scheduler.createWorker();
        cacheCleanupWorker.schedule(() -> Thread.currentThread().setName("cache-cleanup")); //TODO thread name
        cacheCleanupWorker.schedulePeriodically(() -> {
                    log.debugWithParams("Expiring old timeout cache values",
                            "cacheSize", tagToMessage.size()
                    );
                    tagToMessage.cleanUp();
                },
                cleanupTimeoutCacheIntervalSec,
                cleanupTimeoutCacheIntervalSec, TimeUnit.SECONDS);
    }


    @Override
    public Single<Void> publish(String routingKey, AMQP.BasicProperties props, byte[] payload){
        return publish(routingKey, props, payload, 1, 0);
    }

    private Single<Void> publish(final String routingKey, final AMQP.BasicProperties props, final byte[] payload, int attempt, int delaySec){
        return Single.<Void>create(subscriber -> schedulePublish(routingKey, props, payload, attempt, delaySec, subscriber, metricsReporter));
    }

    private Subscription schedulePublish(String routingKey,
                                         AMQP.BasicProperties props,
                                         byte[] payload, int attempt,
                                         int delaySec, SingleSubscriber<? super Void> subscriber,
                                         RxRabbitMetricsReporter metricsReporter) {

        long schedulingStart = System.currentTimeMillis();
        return publishWorker.schedule(() -> {
            synchronized (this) {    //TODO make this method prettier..
                final long publishStart = System.currentTimeMillis();
                UnconfirmedMessage message = new UnconfirmedMessage(subscriber, routingKey, props, payload, schedulingStart, attempt);
                long seqNo = 0;
                try {
                    seqNo = getChannel().getNextPublishSeqNo();
                } catch (Exception e) {
                    log.errorWithParams("Error when creating channel. The connection and the channel is now considered broken.", e,
                            "exchange", exchange,
                            "routingKey", routingKey,
                            "basicProperties", props);
                    closeWithError();
                    message.nack(e);
                    return;
                }
                final long internalSeqNr = seqNo + seqOffset.get();
                if (largestSeqSeen.get() < internalSeqNr) {
                    largestSeqSeen.set(internalSeqNr);
                }
                try {
                    log.traceWithParams("Publishing message.",
                            "exchange", exchange,
                            "routingKey", routingKey,
                            "attemptNo", attempt,
                            "basicProperties", props
                    );
                    //TODO handle publishConfirm = false
                    getChannel().basicPublish(exchange, routingKey, props, payload);
                    tagToMessage.put(internalSeqNr, message);
                    metricsReporter.reportCount("sent");
                    metricsReporter.reportCount("sent-bytes", payload.length);
                    message.setBasicPublishDone();
                    metricsReporter.reportCount("basic-publish-done");
                    metricsReporter.reportTime("basic-publish-took-ms", System.currentTimeMillis() - publishStart);
                } catch (Exception e) {
                    //TODO look at the error and do different things depending on the type??
                    log.errorWithParams("Error when calling basicPublish. The connection and the channel is now considered broken.", e,
                            "exchange", exchange,
                            "routingKey", routingKey,
                            "basicProperties", props);
                    closeWithError();
                    message.nack(e);
                }
            }
        }, delaySec, TimeUnit.SECONDS);
    }

    private synchronized PublishChannel getChannel() throws IOException, TimeoutException {
        if (channel==null){
            for (int i = 0; i < maxRetries; i++) {
                try {
                    Thread.sleep(DelaySequence.getDelayMillis(i));
                    log.infoWithParams("Creating publish channel.", "exchange", exchange);
                    this.channel = channelFactory.createPublishChannel(exchange);
                    channel.addConfirmListener(confirmListener());
                    break;
                } catch (Exception ignored) {
                    log.warnWithParams("Failed to create connection. will try again.",
                            "attempt", i,
                            "maxAttempts", maxRetries,
                            "secsUntilNextAttempt", DelaySequence.getDelaySec(i));
                }
            }
        }
        if (channel==null){
            throw new TimeoutException("Failed to create channel after "+maxRetries+" attempts.");
        }
        return channel;
    }

    private synchronized void closeWithError() {
        if (channel!=null) {
            channel.closeWithError();
            channel = null;
        }
        seqOffset.set(largestSeqSeen.get());
        //ackWorker.unsubscribe(); TODO I don't think we can do this since we need to nack all messages.
        //publishWorker.unsubscribe();
    }

    private ConfirmListener confirmListener() {
        return new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) {
                ackWorker.schedule(() -> {
                    for (Long k : getAllTags(deliveryTag, multiple)) {
                        log.traceWithParams("Handle confirm-ack for delivery tag",
                                "deliveryTag", deliveryTag,
                                "tag", k,
                                "multiple", multiple);
                        final UnconfirmedMessage remove = tagToMessage.getIfPresent(k);
                        if(remove != null){
                            tagToMessage.invalidate(k);
                            remove.ack();
                        }
                    }
                });
            }
            @Override
            public void handleNack(long deliveryTag, boolean multiple) {
                ackWorker.schedule(() -> {
                    for (Long k : getAllTags(deliveryTag, multiple)) {
                        log.traceWithParams("Handle confirm-nack for delivery tag",
                                "deliveryTag", deliveryTag,
                                "tag", k,
                                "multiple", multiple);
                        final UnconfirmedMessage remove = tagToMessage.getIfPresent(k);
                        if(remove != null){
                            tagToMessage.invalidate(k);
                            remove.nack(new IOException("Publisher sent nack on confirm return. deliveryTag=" + deliveryTag));
                        }
                    }
                });
            }
        };
    }

    private Collection<Long> getAllTags(long deliveryTag, boolean multiple) {
        final long currOffset = seqOffset.get();
        long internalTag = deliveryTag + currOffset;
        Collection<Long> confirmedTags = new ArrayList<>();
        if (multiple) {
            confirmedTags = new ArrayList<>(new TreeMap<>(tagToMessage.asMap())
                    .tailMap(currOffset) //Since we don't want to ack old messages
                    .headMap(internalTag)
                    .keySet());
        }
        confirmedTags.add(internalTag);
        return confirmedTags;
    }


    @Override
    public void close() throws IOException {
        //TODO add logging ??
        //TODO what to do with the non returned Singles???
        try {
            //TODO read the boolean returned from waitFor..
            if (closeTimeoutMillis >0){
                boolean allConfirmed = channel.waitForConfirms(closeTimeoutMillis);
            }else {
                channel.waitForConfirms();
            }
        } catch (Exception e) {
            log.warnWithParams("Error when waiting for confirms during publisher close.");
            //TODO send onError to all subscribers
        }finally {
            if(channel != null){
                channel.close();
            }
        }
        publishWorker.unsubscribe();
        ackWorker.unsubscribe();
    }


    private class UnconfirmedMessage {

        final byte[] payload;
        final String routingKey;
        final AMQP.BasicProperties props;
        final SingleSubscriber<? super Void> subscriber;
        final int attempt;
        private final long schedulingStart;
        private long basicPublishDone;

        public UnconfirmedMessage(SingleSubscriber<? super Void> subscriber, String routingKey, AMQP.BasicProperties props, byte[] payload, long schedulingStart, int attempt) {
            this.payload = payload;
            this.subscriber = subscriber;
            this.routingKey = routingKey;
            this.props = props;
            this.schedulingStart = schedulingStart;
            this.attempt = attempt;
        }

        public void setBasicPublishDone(){
            this.basicPublishDone = System.currentTimeMillis();
        }

        public void ack() {
            final long  tookTotal = System.currentTimeMillis() - schedulingStart;
            final long tookConfirm = System.currentTimeMillis() - basicPublishDone;

            log.traceWithParams("Got successful confirm for published message.",
                    "exchange", exchange,
                    "routingKey", routingKey,
                    "attemptNo", attempt,
                    "tookTotal", tookTotal,
                    "tookConfirm", tookConfirm,
                    "basicProperties", props);
            subscriber.onSuccess(null);
        }

        public void nack(Exception e) {
            if (attempt<maxRetries) {
                int delaySec = DelaySequence.getDelaySec(attempt);
                log.warnWithParams("Could not publish message to exchange. Will re-try the publish in a while.",
                        "messageId", props.getMessageId(),
                        "error", e,
                        "delaySec", delaySec,
                        "exchange", exchange,
                        "routingKey", routingKey,
                        "failedAttempts", attempt,
                        "basicProperties", props
                );
                schedulePublish(routingKey, props, payload, attempt + 1, delaySec, subscriber, metricsReporter);
            }else{
                if(basicPublishDone > 0){
                    //this means that the message was published, but not confirmed
                    metricsReporter.reportTime("publish-confirm-took-ms", System.currentTimeMillis() - basicPublishDone);
                    metricsReporter.reportCount("confirmed-failed");
                } else {
                    metricsReporter.reportCount("basic-publish-fail");
                }
                final long tookTotal = System.currentTimeMillis() - schedulingStart;
                metricsReporter.reportTime("publish-total-took-ms", tookTotal);
                metricsReporter.reportCount("fail");
                log.errorWithParams("Could not publish message to exchange. Giving up and reporting error.", e,
                        "messageId", props.getMessageId(),
                        "exchange", exchange,
                        "routingKey", routingKey,
                        "failedAttempts", attempt,
                        "tookMs", tookTotal,
                        "basicProperties", props
                );
                subscriber.onError(e);
            }
        }
    }

}
