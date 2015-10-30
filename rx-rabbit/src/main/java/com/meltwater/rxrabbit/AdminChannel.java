package com.meltwater.rxrabbit;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;

import java.io.IOException;
import java.util.Map;

public interface AdminChannel extends ChannelWrapper {

    Connection getConnection();

    AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) throws IOException;

    void exchangeDeclareNoWait(String exchange, String type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) throws IOException;

    AMQP.Exchange.DeclareOk exchangeDeclarePassive(String name) throws IOException;

    AMQP.Exchange.DeleteOk exchangeDelete(String exchange, boolean ifUnused) throws IOException;
    
    void exchangeDeleteNoWait(String exchange, boolean ifUnused) throws IOException;

    AMQP.Exchange.DeleteOk exchangeDelete(String exchange) throws IOException;

    AMQP.Queue.DeclareOk queueDeclare(String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments) throws IOException;

    void queueDeclareNoWait(String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments) throws IOException;

    AMQP.Queue.DeclareOk queueDeclarePassive(String queue) throws IOException;

    AMQP.Queue.DeleteOk queueDelete(String queue, boolean ifUnused, boolean ifEmpty) throws IOException;

    void queueDeleteNoWait(String queue, boolean ifUnused, boolean ifEmpty) throws IOException;

    AMQP.Queue.BindOk queueBind(String queue, String exchange, String routingKey, Map<String, Object> arguments) throws IOException;

    void queueBindNoWait(String queue, String exchange, String routingKey, Map<String, Object> arguments) throws IOException;

    AMQP.Queue.UnbindOk queueUnbind(String queue, String exchange, String routingKey, Map<String, Object> arguments) throws IOException;

    AMQP.Queue.PurgeOk queuePurge(String queue) throws IOException;

    GetResponse basicGet(String queue, boolean autoAck) throws IOException;

}
