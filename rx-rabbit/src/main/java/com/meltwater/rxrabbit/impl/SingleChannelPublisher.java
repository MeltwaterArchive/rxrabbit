package com.meltwater.rxrabbit.impl;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalNotification;
import com.meltwater.rxrabbit.ChannelFactory;
import com.meltwater.rxrabbit.Exchange;
import com.meltwater.rxrabbit.Payload;
import com.meltwater.rxrabbit.PublishChannel;
import com.meltwater.rxrabbit.RabbitPublisher;
import com.meltwater.rxrabbit.RoutingKey;
import com.meltwater.rxrabbit.metrics.RxRabbitMetricsReporter;
import com.meltwater.rxrabbit.metrics.RxStatsDMetricsReporter;
import com.meltwater.rxrabbit.util.Fibonacci;
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
public class SingleChannelPublisher implements RabbitPublisher {

    private static final Logger log = new Logger(SingleChannelPublisher.class);

    private final int maxRetries;
    private final boolean publisherConfirms;
    private final long closeTimeoutMillis;

    private final ChannelFactory channelFactory;
    private final RxRabbitMetricsReporter metricsReporter; //TODO use an event callback interface here that handles metrics

    private final Scheduler.Worker ackWorker;
    private final Scheduler.Worker publishWorker;
    private final Scheduler.Worker cacheCleanupWorker;

    private final Cache<Long, UnconfirmedMessage> tagToMessage;
    private final AtomicLong largestSeqSeen = new AtomicLong(0);
    private final AtomicLong seqOffset = new AtomicLong(0);


    private PublishChannel channel = null;

    public SingleChannelPublisher(ChannelFactory channelFactory,
                                  boolean publisherConfirms,
                                  int maxRetries,
                                  Scheduler scheduler,
                                  StatsDClient statsDClient,
                                  long confirmsTimeoutSec,
                                  long closeTimeoutMillis,
                                  long cacheCleanupTriggerSecs) {
        this.channelFactory = channelFactory;
        this.publisherConfirms = publisherConfirms;
        this.maxRetries = maxRetries;
        this.closeTimeoutMillis = closeTimeoutMillis;

        this.metricsReporter = new RxStatsDMetricsReporter(statsDClient, "rabbit-publish");

        this.publishWorker = scheduler.createWorker();
        publishWorker.schedule(() -> Thread.currentThread().setName("rabbit-send-thread")); //TODO thread name
        this.ackWorker = scheduler.createWorker();
        ackWorker.schedule(() -> Thread.currentThread().setName("rabbit-confirm-thread")); //TODO thread name
        this.cacheCleanupWorker = scheduler.createWorker();
        cacheCleanupWorker.schedule(() -> Thread.currentThread().setName("cache-cleanup")); //TODO thread name

        if (publisherConfirms) {
            this.tagToMessage = CacheBuilder.<Long, UnconfirmedMessage>newBuilder()
                    .expireAfterAccess(confirmsTimeoutSec, TimeUnit.SECONDS)
                    .removalListener(this::handleRemove)
                    .build();
            cacheCleanupWorker.schedulePeriodically(tagToMessage::cleanUp, cacheCleanupTriggerSecs, cacheCleanupTriggerSecs, TimeUnit.SECONDS);
        }else{
            this.tagToMessage = null;
        }
    }


    @Override
    public Single<Void> call(Exchange exchange, RoutingKey routingKey, AMQP.BasicProperties basicProperties, Payload payload) {
        return publish(exchange, routingKey, basicProperties, payload, 1, 0);
    }


    private Single<Void> publish(Exchange exchange, RoutingKey routingKey, AMQP.BasicProperties props, Payload payload, int attempt, int delaySec){
        return Single.<Void>create(subscriber -> schedulePublish(exchange, routingKey, props, payload, attempt, delaySec, subscriber, metricsReporter));
    }

    private Subscription schedulePublish(Exchange exchange,
                                         RoutingKey routingKey,
                                         AMQP.BasicProperties props,
                                         Payload payload,
                                         int attempt,
                                         int delaySec,
                                         SingleSubscriber<? super Void> subscriber,
                                         RxRabbitMetricsReporter metricsReporter) {
        long schedulingStart = System.currentTimeMillis();
        return publishWorker.schedule(() -> basicPublish(exchange, routingKey, props, payload, attempt, subscriber, metricsReporter, schedulingStart), delaySec, TimeUnit.SECONDS);
    }

    private synchronized void basicPublish(Exchange exchange, RoutingKey routingKey, AMQP.BasicProperties props, Payload payload, int attempt, SingleSubscriber<? super Void> subscriber, RxRabbitMetricsReporter metricsReporter, long schedulingStart) {
        final long publishStart = System.currentTimeMillis();
        UnconfirmedMessage message = new UnconfirmedMessage(subscriber,
                exchange.name,
                routingKey.value,
                props, payload.data,
                schedulingStart,
                attempt);
        long seqNo;
        try {
            seqNo = getChannel().getNextPublishSeqNo();
        } catch (Exception e) {
            log.errorWithParams("Error when creating channel. The connection and the channel is now considered broken.", e,
                    "exchange", exchange,
                    "routingKey", routingKey,
                    "basicProperties", props);
            closeChannelWithError();
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
            getChannel().basicPublish(exchange.name, routingKey.value, props, payload.data);
            message.setPublishCompletedTime(System.currentTimeMillis());

            if (publisherConfirms) {
                message.setPublished(true);
                tagToMessage.put(internalSeqNr, message);
            }else{
                message.ack();
            }

            metricsReporter.reportCount("sent");
            metricsReporter.reportCount("sent-bytes", payload.data.length);
            metricsReporter.reportCount("basic-publish-done");
            metricsReporter.reportTime("basic-publish-took-ms", System.currentTimeMillis() - publishStart);
            //TODO add gauge for outstanding
        } catch (Exception e) {
            //TODO look at the error and do different things depending on the type??
            log.errorWithParams("Error when calling basicPublish. The connection and the channel is now considered broken.", e,
                    "exchange", exchange,
                    "routingKey", routingKey,
                    "basicProperties", props);
            closeChannelWithError();
            message.nack(e);
        }
    }

    private synchronized PublishChannel getChannel() throws IOException, TimeoutException {
        if (channel==null){
            for (int i = 0; i < maxRetries || maxRetries<=0; i++) {
                try {
                    Thread.sleep(Fibonacci.getDelayMillis(i));
                    log.infoWithParams("Creating publish channel.");
                    this.channel = channelFactory.createPublishChannel();
                    channel.addConfirmListener(confirmListener());
                    break;
                } catch (Exception ignored) {
                    log.warnWithParams("Failed to create connection. will try again.",
                            "attempt", i,
                            "maxAttempts", maxRetries,
                            "secsUntilNextAttempt", Fibonacci.getDelaySec(i));
                }
            }
        }
        if (channel==null){
            throw new TimeoutException("Failed to create channel after "+maxRetries+" attempts.");
        }
        return channel;
    }

    private synchronized void closeChannelWithError() {
        if (channel!=null) {
            channel.closeWithError();
            channel = null;
        }
        seqOffset.set(largestSeqSeen.get());
    }

    private void handleRemove(RemovalNotification<Long, UnconfirmedMessage> notification) {
        if (notification.getCause().equals(RemovalCause.EXPIRED)) {
            UnconfirmedMessage message = notification.getValue();
            if (message != null) { //TODO how can this be null??
                ackWorker.schedule(() -> {
                    if (message.published) {
                        log.warnWithParams("Message did not receive publish-confirm in time", "messageId", message.props.getMessageId());
                    } //TODO send metric on the timeout event
                    message.nack(new TimeoutException("Message did not receive publish confirm in time"));
                });
            }
        }
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
        cacheCleanupWorker.unsubscribe();
    }


    private class UnconfirmedMessage {

        final byte[] payload;
        final String routingKey;
        final String exchange;
        final AMQP.BasicProperties props;
        final SingleSubscriber<? super Void> subscriber;
        final int attempt;

        private boolean published = false;
        private long publishCompletedTime;

        private final long messageCreationTime;

        public UnconfirmedMessage(SingleSubscriber<? super Void> subscriber,
                                  String exchange,
                                  String routingKey,
                                  AMQP.BasicProperties props,
                                  byte[] payload,
                                  long messageCreationTime,
                                  int attempt) {
            this.exchange = exchange;
            this.payload = payload;
            this.subscriber = subscriber;
            this.routingKey = routingKey;
            this.props = props;
            this.messageCreationTime = messageCreationTime;
            this.attempt = attempt;
        }

        public void setPublishCompletedTime(long time){
            this.publishCompletedTime = time;
        }

        public void ack() {
            final long tookTotal = System.currentTimeMillis() - messageCreationTime;
            final long tookConfirm = System.currentTimeMillis() - publishCompletedTime;

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
            if (attempt<maxRetries || maxRetries<=0) {
                int delaySec = Fibonacci.getDelaySec(attempt);
                log.warnWithParams("Could not publish message to exchange. Will re-try the publish in a while.",
                        "messageId", props.getMessageId(),
                        "error", e,
                        "delaySec", delaySec,
                        "exchange", exchange,
                        "routingKey", routingKey,
                        "failedAttempts", attempt,
                        "basicProperties", props
                );
                schedulePublish(new Exchange(exchange), new RoutingKey(routingKey), props, new Payload(payload), attempt + 1, delaySec, subscriber, metricsReporter);
            }else{
                //TODO add gauge for outstanding
                if(publishCompletedTime > 0){
                    //this means that the message was published, but not confirmed
                    metricsReporter.reportTime("publish-confirm-took-ms", System.currentTimeMillis() - publishCompletedTime);
                    metricsReporter.reportCount("confirmed-failed");
                } else {
                    metricsReporter.reportCount("basic-publish-fail");
                }
                final long tookTotal = System.currentTimeMillis() - messageCreationTime;
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

        public void setPublished(boolean published) {
            this.published = published;
        }
    }

}
