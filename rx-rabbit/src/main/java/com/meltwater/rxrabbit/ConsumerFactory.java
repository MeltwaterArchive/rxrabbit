package com.meltwater.rxrabbit;

import rx.Observable;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public interface ConsumerFactory {

    /**
     * Creates a consumer connected to an already existing non exclusive queue and starts consuming messages from it.
     *
     * If the underlying {@link com.rabbitmq.client.Consumer}, {@link com.rabbitmq.client.Channel}, {@link com.rabbitmq.client.Connection}
     * for some reason is broken and/or disconnected then the observable will NOT report an error at first, unless explicitly told to do so.
     * Instead it will repeatedly try to re-connect to the queue (with increasing back-off delay) until it succeeds or the configured number
     * of re-tries are exceeded, then {@link rx.Subscriber#onError(Throwable)} will be called with the last error encountered.
     *
     * @param queue the queue that will be consumed
     *
     * @return an observable that will deliver the messages from the given queue
     */
    Observable<Message> createConsumer(String queue);

    /**
     * First declares a server-named exclusive, autodelete, non-durable queue bound to the given exchange and with the supplied routing key.
     * Then it creates an consumer on the newly created queue and starts consuming messages from it.
     *
     * If the underlying {@link com.rabbitmq.client.Consumer}, {@link com.rabbitmq.client.Channel}, {@link com.rabbitmq.client.Connection}
     * for some reason is broken and/or disconnected then the observable will NOT report an error at first, unless explicitly told to do so.
     * Instead it will repeatedly try to re-connect to the queue (with increasing back-off delay) until it succeeds or the configured number
     * of re-tries are exceeded, then {@link rx.Subscriber#onError(Throwable)} will be called with the last error encountered.
     *
     * If it succeeds to re-connect to the broker then a NEW server-named exclusive, autodelete, non-durable queue will be declared and bound
     * to the exchange and the message flow will continue.
     *
     * @param exchange the exchange to connect to
     * @param routingKey the routing key to use for the binding between the temporary queue and the exchange
     *
     * @return an observable that will deliver the messages delivered to the given exchange matching the routingKey pattern
     */
    Observable<Message> createConsumer(String exchange, String routingKey);
}
