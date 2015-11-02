package com.meltwater.rxrabbit.impl;

import com.google.common.collect.Iterables;
import com.meltwater.rxrabbit.Exchange;
import com.meltwater.rxrabbit.Payload;
import com.meltwater.rxrabbit.RabbitPublisher;
import com.meltwater.rxrabbit.RoutingKey;
import com.rabbitmq.client.AMQP;
import rx.Single;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class RoundRobinPublisher implements RabbitPublisher {

    private final List<RabbitPublisher> backingPublishers;
    private final Iterator<RabbitPublisher> publisherIterator;

    public RoundRobinPublisher(List<RabbitPublisher> backingPublishers) {
        assert !backingPublishers.isEmpty();
        this.backingPublishers = backingPublishers;
        this.publisherIterator = Iterables.cycle(backingPublishers).iterator();
    }

    @Override
    public Single<Void> call(Exchange exchange, RoutingKey routingKey, AMQP.BasicProperties basicProperties, Payload payload) {
        return publisherIterator.next().call(exchange, routingKey, basicProperties, payload);
    }

    @Override
    public void close() throws IOException {
        for (RabbitPublisher backingPublisher : backingPublishers) {
            backingPublisher.close();
        }
    }

}
