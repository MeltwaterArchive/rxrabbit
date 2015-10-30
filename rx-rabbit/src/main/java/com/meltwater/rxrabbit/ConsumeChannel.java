package com.meltwater.rxrabbit;

import com.rabbitmq.client.Consumer;

import java.io.IOException;

public interface ConsumeChannel extends ChannelWrapper{

    String getQueue();

    void basicCancel(String consumerTag) throws IOException;

    void basicAck(long deliveryTag, boolean multiple) throws IOException;

    void basicNack(long deliveryTag, boolean multiple) throws IOException;

    void basicConsume(String queue, String consumerTag, Consumer consumer) throws IOException;
}
