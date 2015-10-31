package com.meltwater.rxrabbit;

import com.rabbitmq.client.AMQP;
import rx.Observable;
import rx.Single;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * A publisher can publish amqp messages to an amqp exchange.
 * It is up to the implementation to decide if publisher confirms should be used or not.
 */
public interface RabbitPublisher extends Closeable {

    /**
     * @return the exchange this publisher publishes messages to.
     */
    String getExchange();

    /**
     * Publishes messages to an amqp exchange
     *
     * @see PublishChannel#basicPublish(String, String, AMQP.BasicProperties, byte[])
     * @see com.rabbitmq.client.Channel#basicPublish(String, String, AMQP.BasicProperties, byte[])
     *
     * @param routingKey the routing key
     * @param props other properties for the message - durability, routing headers etc
     * @param payload the message body
     *
     * NOTE to implementors: It is expected that a call to this method returns (almost) immediately
     *                without doing any blocking IO on the calling thread.
     *
     * @return a single object that will return a Void value if the publish was successful
     * (including broker confirmation if that is enabled) or an exception if something goes wrong.
     */
    Single<Void> publish(String routingKey, AMQP.BasicProperties props, byte[] payload);

}
