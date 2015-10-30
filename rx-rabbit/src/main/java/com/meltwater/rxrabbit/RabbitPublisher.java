package com.meltwater.rxrabbit;

import com.rabbitmq.client.AMQP;
import rx.Observable;
import rx.Single;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

public interface RabbitPublisher extends Closeable {

    Single<Void> publish(String routingKey, AMQP.BasicProperties props, byte[] payload);

}
