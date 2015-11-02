package com.meltwater.rxrabbit;

import com.rabbitmq.client.AMQP;

public class PublishEvent {

    public final Payload payload;
    public final Exchange exchange;
    public final RoutingKey routingKey;
    public final AMQP.BasicProperties basicProperties;
    public final int attempt;
    public final boolean publisherConfirms;
    public final long createdAtTimestamp;
    public final long publishedAtTimestamp;
    public final long publishCompletedAtTimestamp;

    public PublishEvent(Payload payload,
                        Exchange exchange,
                        RoutingKey routingKey,
                        AMQP.BasicProperties basicProperties,
                        int attempt,
                        boolean publisherConfirms,
                        long createdAtTimestamp,
                        long publishedAtTimestamp,
                        long publishCompletedAtTimestamp) {
        this.payload = payload;
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.basicProperties = basicProperties;
        this.attempt = attempt;
        this.publisherConfirms = publisherConfirms;
        this.createdAtTimestamp = createdAtTimestamp;
        this.publishedAtTimestamp = publishedAtTimestamp;
        this.publishCompletedAtTimestamp = publishCompletedAtTimestamp;
    }
}
