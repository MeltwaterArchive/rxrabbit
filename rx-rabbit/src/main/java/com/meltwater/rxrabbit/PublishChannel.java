package com.meltwater.rxrabbit;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConfirmListener;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public interface PublishChannel extends ChannelWrapper{

    String getExchange();

    void addConfirmListener(ConfirmListener confirmListener);

    long getNextPublishSeqNo();

    Void basicPublish(String exchange, String routingKey, AMQP.BasicProperties props, byte[] payload) throws IOException;

    void waitForConfirms(long closeTimeoutMillis) throws InterruptedException, TimeoutException;

    void waitForConfirms() throws InterruptedException;
}
