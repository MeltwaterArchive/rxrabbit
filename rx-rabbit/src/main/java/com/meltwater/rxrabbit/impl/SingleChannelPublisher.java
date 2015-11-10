package com.meltwater.rxrabbit.impl;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalNotification;
import com.meltwater.rxrabbit.ChannelFactory;
import com.meltwater.rxrabbit.Exchange;
import com.meltwater.rxrabbit.Payload;
import com.meltwater.rxrabbit.PublishChannel;
import com.meltwater.rxrabbit.PublishEvent;
import com.meltwater.rxrabbit.PublishEventListener;
import com.meltwater.rxrabbit.RabbitPublisher;
import com.meltwater.rxrabbit.RoutingKey;
import com.meltwater.rxrabbit.util.Fibonacci;
import com.meltwater.rxrabbit.util.Logger;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConfirmListener;
import rx.Scheduler;
import rx.Single;
import rx.SingleSubscriber;
import rx.Subscription;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.meltwater.rxrabbit.PublisherSettings.RETRY_FOREVER;

//TODO javadoc
public class SingleChannelPublisher implements RabbitPublisher {

    private static final AtomicLong publisherInstanceNr = new AtomicLong();
    private static final Logger log = new Logger(SingleChannelPublisher.class);

    private final int maxRetries;
    private final boolean publisherConfirms;
    private Scheduler observeOnScheduler;
    private final long closeTimeoutMillis;

    private final ChannelFactory channelFactory;
    private final PublishEventListener metricsReporter;

    private final Scheduler.Worker ackWorker;
    private final Scheduler.Worker publishWorker;
    private final Scheduler.Worker cacheCleanupWorker;

    private final Cache<Long, UnconfirmedMessage> tagToMessage;
    private final AtomicLong largestSeqSeen = new AtomicLong(0);
    private final AtomicLong seqOffset = new AtomicLong(0);

    private PublishChannel channel = null;
    private AtomicBoolean closed = new AtomicBoolean(false);

    public SingleChannelPublisher(ChannelFactory channelFactory,
                                  boolean publisherConfirms,
                                  int maxRetries,
                                  Scheduler observeOnScheduler,
                                  PublishEventListener metricsReporter,
                                  long confirmsTimeoutSec,
                                  long closeTimeoutMillis,
                                  long cacheCleanupTriggerSecs) {
        this.channelFactory = channelFactory;
        this.publisherConfirms = publisherConfirms;
        this.maxRetries = maxRetries;
        this.observeOnScheduler = observeOnScheduler;
        this.closeTimeoutMillis = closeTimeoutMillis;
        this.metricsReporter = metricsReporter;

        this.publishWorker = Schedulers.io().createWorker();
        final long instanceNr = publisherInstanceNr.incrementAndGet();
        publishWorker.schedule(() -> Thread.currentThread().setName("rabbit-send-thread-"+instanceNr));
        this.ackWorker = Schedulers.io().createWorker();
        ackWorker.schedule(() -> Thread.currentThread().setName("rabbit-confirm-thread-"+instanceNr));
        this.cacheCleanupWorker = Schedulers.io().createWorker();
        cacheCleanupWorker.schedule(() -> Thread.currentThread().setName("cache-cleanup-"+instanceNr));
        this.tagToMessage = CacheBuilder.<Long, UnconfirmedMessage>newBuilder()
                .expireAfterAccess(confirmsTimeoutSec, TimeUnit.SECONDS)
                .removalListener(this::handleCacheRemove)
                .build();
        if (publisherConfirms) {
            cacheCleanupWorker.schedulePeriodically(tagToMessage::cleanUp, cacheCleanupTriggerSecs, cacheCleanupTriggerSecs, TimeUnit.SECONDS);
        }
    }


    @Override
    public synchronized void close() throws IOException {
        closed.set(true);
        log.infoWithParams("Closing publisher.", "nonConfirmedMessages", tagToMessage.asMap().size());
        try {
            if (channel != null && publisherConfirms) {
                boolean allConfirmed = closeTimeoutMillis > 0 ? channel.waitForConfirms(closeTimeoutMillis) : channel.waitForConfirms();
                if (allConfirmed) {
                    log.infoWithParams("All published messages successfully confirmed.");
                }
            }
        } catch (Exception e) {
            log.warnWithParams("Error when waiting for confirms.",
                    "channelId", channel!=null?channel.getChannelNumber()+"":"null",
                    "closeTimeoutMillis", closeTimeoutMillis,
                    "nonConfirmedMessages", tagToMessage.asMap().size(),
                    "error", e);
        }finally {
            cacheCleanupWorker.unsubscribe();
            //TODO not covered in tests - add test!
            if (tagToMessage.asMap().size()>0) {
                log.warnWithParams("Not all messages were confirmed during the close timeout",
                        "closeTimeoutMillis", closeTimeoutMillis,
                        "nonConfirmedMessages", tagToMessage.asMap().size());
                for (Map.Entry<Long,UnconfirmedMessage> entry :tagToMessage.asMap().entrySet()){
                    entry.getValue().nack(new IllegalStateException("The publisher is closed and will not accept any more messages."));
                }
                tagToMessage.invalidateAll();
                tagToMessage.cleanUp();
            }
            if(channel != null){
                channel.close();
            }
            publishWorker.unsubscribe();
            ackWorker.unsubscribe();
        }
    }

    @Override
    public Single<Void> call(Exchange exchange, RoutingKey routingKey, AMQP.BasicProperties basicProperties, Payload payload) {
        return Single.<Void>create(subscriber -> schedulePublish(exchange, routingKey, basicProperties, payload, 1, 0, subscriber))
                .observeOn(observeOnScheduler);
    }

    public Subscription schedulePublish(Exchange exchange,
                                        RoutingKey routingKey,
                                        AMQP.BasicProperties props,
                                        Payload payload,
                                        int attempt,
                                        int delaySec,
                                        SingleSubscriber<? super Void> subscriber) {
        if (closed.get()) subscriber.onError(new IllegalStateException("The publisher is closed and will not accept any more messages."));
        long schedulingStart = System.currentTimeMillis();
        return publishWorker.schedule(() -> basicPublish(exchange, routingKey, props, payload, attempt, subscriber, schedulingStart), delaySec, TimeUnit.SECONDS);
    }

    private synchronized void basicPublish(Exchange exchange, RoutingKey routingKey, AMQP.BasicProperties props, Payload payload, int attempt, SingleSubscriber<? super Void> subscriber, long schedulingStart) {
        final long publishStart = System.currentTimeMillis();
        UnconfirmedMessage message = new UnconfirmedMessage(this, subscriber,
                exchange,
                routingKey,
                props,
                payload,
                schedulingStart,
                publishStart,
                attempt);
        long seqNo;
        try {
            seqNo = getChannel().getNextPublishSeqNo();
        } catch (Exception error) {
            handleChannelException(exchange,
                    routingKey,
                    props,
                    message,
                    error,
                    "Error when creating channel. The connection and the channel is now considered broken.");
            return;
        }
        final long internalSeqNr = seqNo + seqOffset.get();
        if (largestSeqSeen.get() < internalSeqNr) {
            largestSeqSeen.set(internalSeqNr);
        }
        try {
            beforePublish(message);
            getChannel().basicPublish(exchange.name, routingKey.value, props, payload.data);
            message.setPublishCompletedAtTimestamp(System.currentTimeMillis());
            message.setPublished(true);
            afterPublish(message);
            if (publisherConfirms) {
                tagToMessage.put(internalSeqNr, message);
            }else{
                message.ack();
            }
        } catch (Exception error) {
            handleChannelException(exchange,
                    routingKey,
                    props,
                    message,
                    error,
                    "Error when calling basicPublish. The connection and the channel is now considered broken.");
        }
    }

    private synchronized PublishChannel getChannel() throws IOException, TimeoutException {
        if (channel==null){
            for (int i = 0; i < maxRetries || maxRetries==RETRY_FOREVER; i++) {
                try {
                    Thread.sleep(Fibonacci.getDelayMillis(i));
                    log.infoWithParams("Creating publish channel.");
                    this.channel = channelFactory.createPublishChannel();
                    if (publisherConfirms){
                        channel.confirmSelect();
                        channel.addConfirmListener(new InternalConfirmListener(ackWorker,this));
                    }
                    break;
                } catch (Exception ignored) {
                    log.warnWithParams("Failed to create connection. Will try to re-connect again.",
                            "attempt", i,
                            "maxAttempts", maxRetries,
                            "secsUntilNextAttempt", Fibonacci.getDelaySec(i+1));
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

    private synchronized Collection<Long> getAllPreviousTags(long deliveryTag, boolean multiple) {
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

    private void handleChannelException(Exchange exchange, RoutingKey routingKey, AMQP.BasicProperties props, UnconfirmedMessage message, Exception e, String logMsg) {
        //TODO should we look at the error and do different things depending on the type??
        log.errorWithParams(logMsg,
                "exchange", exchange,
                "error", e,
                "routingKey", routingKey,
                "basicProperties", props);
        closeChannelWithError();
        message.nack(e);
    }

    private void handleCacheRemove(RemovalNotification<Long, UnconfirmedMessage> notification) {
        if (notification.getCause().equals(RemovalCause.EXPIRED)) {
            UnconfirmedMessage message = notification.getValue();
            if (message != null) { //TODO figure out why this can be null??
                ackWorker.schedule(() -> {
                    if (message.published) {
                        log.warnWithParams("Message did not receive publish-confirm in time", "messageId", message.props.getMessageId());
                    }
                    message.nack(new TimeoutException("Message did not receive publish confirm in time"));
                });
            }
        }
    }

    private int getMaxRetries() {
        return maxRetries;
    }

    private void beforePublish(UnconfirmedMessage message){
        metricsReporter.beforePublish(getEvent(message));
    }

    private void afterPublish(UnconfirmedMessage message) {
        metricsReporter.afterPublish(getEvent(message));
    }

    private void afterFinalFail(UnconfirmedMessage message, Exception e) {
        metricsReporter.afterFinalFail(getEvent(message), e);
    }

    private void afterIntermediateFail(UnconfirmedMessage message, Exception e, int delaySec) {
        metricsReporter.afterIntermediateFail(getEvent(message), e, delaySec);
    }

    private void afterAck(UnconfirmedMessage message) {
        metricsReporter.afterConfirm(getEvent(message));
    }

    private PublishEvent getEvent(UnconfirmedMessage message) {
        return new PublishEvent(message.payload,
                message.exchange,
                message.routingKey,
                message.props,
                message.attempt,
                publisherConfirms,
                message.createdAtTimestamp,
                message.publishedAtTimestamp,
                message.publishCompletedAtTimestamp);
    }

    static class InternalConfirmListener implements ConfirmListener{

        final Scheduler.Worker ackWorker;
        final SingleChannelPublisher publisher;

        InternalConfirmListener(Scheduler.Worker ackWorker, SingleChannelPublisher publisher) {
            this.ackWorker = ackWorker;
            this.publisher = publisher;
        }

        @Override
        public void handleAck(long deliveryTag, boolean multiple) {
            ackWorker.schedule(() -> {
                for (Long k : publisher.getAllPreviousTags(deliveryTag, multiple)) {
                    log.traceWithParams("Handling confirm-ack for delivery tag",
                            "deliveryTag", deliveryTag,
                            "tag", k,
                            "multiple", multiple);
                    final UnconfirmedMessage remove = publisher.tagToMessage.getIfPresent(k);
                    if(remove != null){
                        publisher.tagToMessage.invalidate(k);
                        remove.ack();
                    }
                }
            });
        }
        @Override
        public void handleNack(long deliveryTag, boolean multiple) {
            //TODO not covered in tests -  add test!
            ackWorker.schedule(() -> {
                for (Long k : publisher.getAllPreviousTags(deliveryTag, multiple)) {
                    log.traceWithParams("Handling confirm-nack for delivery tag",
                            "deliveryTag", deliveryTag,
                            "tag", k,
                            "multiple", multiple);
                    final UnconfirmedMessage remove = publisher.tagToMessage.getIfPresent(k);
                    if(remove != null){
                        publisher.tagToMessage.invalidate(k);
                        remove.nack(new IOException("Publisher sent nack on confirm return. deliveryTag=" + deliveryTag));
                    }
                }
            });
        }

    }

    static class UnconfirmedMessage {
        final SingleChannelPublisher publisher;
        final Payload payload;
        final RoutingKey routingKey;
        final Exchange exchange;
        final AMQP.BasicProperties props;
        final SingleSubscriber<? super Void> subscriber;
        final long createdAtTimestamp;
        final long publishedAtTimestamp;
        final int attempt;

        boolean published = false;
        long publishCompletedAtTimestamp;

        UnconfirmedMessage(SingleChannelPublisher publisher,
                           SingleSubscriber<? super Void> subscriber,
                           Exchange exchange,
                           RoutingKey routingKey,
                           AMQP.BasicProperties props,
                           Payload payload,
                           long createdAtTimestamp,
                           long publishedAtTimestamp,
                           int attempt) {
            this.publisher = publisher;
            this.exchange = exchange;
            this.payload = payload;
            this.subscriber = subscriber;
            this.routingKey = routingKey;
            this.props = props;
            this.createdAtTimestamp = createdAtTimestamp;
            this.publishedAtTimestamp = publishedAtTimestamp;
            this.attempt = attempt;
        }

        public void setPublishCompletedAtTimestamp(long time) {
            this.publishCompletedAtTimestamp = time;
        }

        public void setPublished(boolean published) {
            this.published = published;
        }

        public void ack() {
            publisher.afterAck(this);
            subscriber.onSuccess(null);
        }

        public void nack(Exception e) {
            double maxRetries = publisher.getMaxRetries();
            if (attempt < maxRetries || maxRetries == RETRY_FOREVER) {
                int delaySec = Fibonacci.getDelaySec(attempt);
                publisher.afterIntermediateFail(this, e, delaySec);
                publisher.schedulePublish(exchange, routingKey, props, payload, attempt + 1, delaySec, subscriber);
            } else {
                publisher.afterFinalFail(this, e);
                subscriber.onError(e);
            }
        }

    }
}
