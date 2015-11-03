package com.meltwater.rxrabbit;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;

import java.io.IOException;
import java.util.Map;

/**
 * Interface wrapping a {@link com.rabbitmq.client.Channel} that allows common admin operations - but no publishing or consume operations.
 *
 * @see ConsumeChannel
 * @see PublishChannel
 * @see com.rabbitmq.client.Channel
 */
public interface AdminChannel extends ChannelWrapper {

    //TODO add links to the Channel methods javadoc instead of copying it

    /**
     * Retrieve the connection which carries this channel.
     * @return the underlying {@link Connection}
     */
    Connection getConnection();


    /**
     * Declare an exchange, via an interface that allows the complete set of
     * arguments.
     * @see com.rabbitmq.client.AMQP.Exchange.Declare
     * @see com.rabbitmq.client.AMQP.Exchange.DeclareOk
     * @param exchange the name of the exchange
     * @param type the exchange type
     * @param durable true if we are declaring a durable exchange (the exchange will survive a server restart)
     * @param autoDelete true if the server should delete the exchange when it is no longer in use
     * @param internal true if the exchange is internal, i.e. can't be directly
     * published to by a client.
     * @param arguments other properties (construction arguments) for the exchange
     * @return a declaration-confirm method to indicate the exchange was successfully declared
     * @throws java.io.IOException if an error is encountered
     */
    AMQP.Exchange.DeclareOk exchangeDeclare(String exchange,
                                            String type,
                                            boolean durable,
                                            boolean autoDelete,
                                            boolean internal,
                                            Map<String, Object> arguments) throws IOException;

    /**
     * Like {@link AdminChannel#exchangeDeclare(String, String, boolean, boolean, boolean, java.util.Map)} but
     * sets nowait parameter to true and returns nothing (as there will be no response from
     * the server).
     *
     * @param exchange the name of the exchange
     * @param type the exchange type
     * @param durable true if we are declaring a durable exchange (the exchange will survive a server restart)
     * @param autoDelete true if the server should delete the exchange when it is no longer in use
     * @param internal true if the exchange is internal, i.e. can't be directly
     * published to by a client.
     * @param arguments other properties (construction arguments) for the exchange
     *
     * @throws java.io.IOException if an error is encountered
     */
    void exchangeDeclareNoWait(String exchange,
                               String type,
                               boolean durable,
                               boolean autoDelete,
                               boolean internal,
                               Map<String, Object> arguments) throws IOException;

    /**
     * Declare an exchange passively; that is, check if the named exchange exists.
     * @param name check the existence of an exchange named this
     * @throws IOException the server will raise a 404 channel exception if the named exchange does not exist.
     */
    AMQP.Exchange.DeclareOk exchangeDeclarePassive(String name) throws IOException;

    /**
     * Delete an exchange
     * @see com.rabbitmq.client.AMQP.Exchange.Delete
     * @see com.rabbitmq.client.AMQP.Exchange.DeleteOk
     * @param exchange the name of the exchange
     * @param ifUnused true to indicate that the exchange is only to be deleted if it is unused
     * @return a deletion-confirm method to indicate the exchange was successfully deleted
     * @throws java.io.IOException if an error is encountered
     */
    AMQP.Exchange.DeleteOk exchangeDelete(String exchange, boolean ifUnused) throws IOException;

    /**
     * Like {@link AdminChannel#exchangeDelete(String, boolean)} but sets nowait parameter to true
     * and returns void (as there will be no response from the server).
     * @see com.rabbitmq.client.AMQP.Exchange.Delete
     * @see com.rabbitmq.client.AMQP.Exchange.DeleteOk
     * @param exchange the name of the exchange
     * @param ifUnused true to indicate that the exchange is only to be deleted if it is unused
     * @throws java.io.IOException if an error is encountered
     */
    void exchangeDeleteNoWait(String exchange, boolean ifUnused) throws IOException;


    /**
     * Delete an exchange, without regard for whether it is in use or not
     * @see com.rabbitmq.client.AMQP.Exchange.Delete
     * @see com.rabbitmq.client.AMQP.Exchange.DeleteOk
     * @param exchange the name of the exchange
     * @return a deletion-confirm method to indicate the exchange was successfully deleted
     * @throws java.io.IOException if an error is encountered
     */
    AMQP.Exchange.DeleteOk exchangeDelete(String exchange) throws IOException;


    /**
     * Declare a queue
     * @see com.rabbitmq.client.AMQP.Queue.Declare
     * @see com.rabbitmq.client.AMQP.Queue.DeclareOk
     * @param queue the name of the queue
     * @param durable true if we are declaring a durable queue (the queue will survive a server restart)
     * @param exclusive true if we are declaring an exclusive queue (restricted to this connection)
     * @param autoDelete true if we are declaring an autodelete queue (server will delete it when no longer in use)
     * @param arguments other properties (construction arguments) for the queue
     * @return a declaration-confirm method to indicate the queue was successfully declared
     * @throws java.io.IOException if an error is encountered
     */
    AMQP.Queue.DeclareOk queueDeclare(String queue, boolean durable, boolean exclusive, boolean autoDelete,
                                      Map<String, Object> arguments) throws IOException;

    /**
     * Like {@link AdminChannel#queueDeclare(String, boolean, boolean, boolean, java.util.Map)} but sets nowait
     * flag to true and returns no result (as there will be no response from the server).
     * @param queue the name of the queue
     * @param durable true if we are declaring a durable queue (the queue will survive a server restart)
     * @param exclusive true if we are declaring an exclusive queue (restricted to this connection)
     * @param autoDelete true if we are declaring an autodelete queue (server will delete it when no longer in use)
     * @param arguments other properties (construction arguments) for the queue
     * @throws java.io.IOException if an error is encountered
     */
    void queueDeclareNoWait(String queue, boolean durable, boolean exclusive, boolean autoDelete,
                            Map<String, Object> arguments) throws IOException;

    /**
     * Declare a queue passively; i.e., check if it exists.  In AMQP
     * 0-9-1, all arguments aside from nowait are ignored; and sending
     * nowait makes this method a no-op, so we default it to false.
     * @see com.rabbitmq.client.AMQP.Queue.Declare
     * @see com.rabbitmq.client.AMQP.Queue.DeclareOk
     * @param queue the name of the queue
     * @return a declaration-confirm method to indicate the queue exists
     * @throws java.io.IOException if an error is encountered,
     * including if the queue does not exist and if the queue is
     * exclusively owned by another connection.
     */
    AMQP.Queue.DeclareOk queueDeclarePassive(String queue) throws IOException;

    /**
     * Delete a queue
     * @see com.rabbitmq.client.AMQP.Queue.Delete
     * @see com.rabbitmq.client.AMQP.Queue.DeleteOk
     * @param queue the name of the queue
     * @param ifUnused true if the queue should be deleted only if not in use
     * @param ifEmpty true if the queue should be deleted only if empty
     * @return a deletion-confirm method to indicate the queue was successfully deleted
     * @throws java.io.IOException if an error is encountered
     */
    AMQP.Queue.DeleteOk queueDelete(String queue, boolean ifUnused, boolean ifEmpty) throws IOException;

    /**
     * Like {@link AdminChannel#queueDelete(String, boolean, boolean)} but sets nowait parameter
     * to true and returns nothing (as there will be no response from the server).
     * @see com.rabbitmq.client.AMQP.Queue.Delete
     * @see com.rabbitmq.client.AMQP.Queue.DeleteOk
     * @param queue the name of the queue
     * @param ifUnused true if the queue should be deleted only if not in use
     * @param ifEmpty true if the queue should be deleted only if empty
     * @throws java.io.IOException if an error is encountered
     */
    void queueDeleteNoWait(String queue, boolean ifUnused, boolean ifEmpty) throws IOException;


    /**
     * Bind a queue to an exchange.
     * @see com.rabbitmq.client.AMQP.Queue.Bind
     * @see com.rabbitmq.client.AMQP.Queue.BindOk
     * @param queue the name of the queue
     * @param exchange the name of the exchange
     * @param routingKey the routine key to use for the binding
     * @param arguments other properties (binding parameters)
     * @return a binding-confirm method if the binding was successfully created
     * @throws java.io.IOException if an error is encountered
     */
    AMQP.Queue.BindOk queueBind(String queue, String exchange, String routingKey, Map<String, Object> arguments) throws IOException;

    /**
     * Same as {@link AdminChannel#queueDeclare(String, boolean, boolean, boolean, java.util.Map)} but sets nowait
     * parameter to true and returns void (as there will be no response
     * from the server).
     * @param queue the name of the queue
     * @param exchange the name of the exchange
     * @param routingKey the routine key to use for the binding
     * @param arguments other properties (binding parameters)
     * @throws java.io.IOException if an error is encountered
     */
    void queueBindNoWait(String queue, String exchange, String routingKey, Map<String, Object> arguments) throws IOException;

    /**
     * Unbind a queue from an exchange.
     * @see com.rabbitmq.client.AMQP.Queue.Unbind
     * @see com.rabbitmq.client.AMQP.Queue.UnbindOk
     * @param queue the name of the queue
     * @param exchange the name of the exchange
     * @param routingKey the routine key to use for the binding
     * @param arguments other properties (binding parameters)
     * @return an unbinding-confirm method if the binding was successfully deleted
     * @throws java.io.IOException if an error is encountered
     */
    AMQP.Queue.UnbindOk queueUnbind(String queue, String exchange, String routingKey, Map<String, Object> arguments) throws IOException;

    /**
     * Purges the contents of the given queue.
     * @see com.rabbitmq.client.AMQP.Queue.Purge
     * @see com.rabbitmq.client.AMQP.Queue.PurgeOk
     * @param queue the name of the queue
     * @return a purge-confirm method if the purge was executed successfully
     * @throws java.io.IOException if an error is encountered
     */
    AMQP.Queue.PurgeOk queuePurge(String queue) throws IOException;

    /**
     * Retrieve a message from a queue using {@link com.rabbitmq.client.AMQP.Basic.Get}
     * @see com.rabbitmq.client.AMQP.Basic.Get
     * @see com.rabbitmq.client.AMQP.Basic.GetOk
     * @see com.rabbitmq.client.AMQP.Basic.GetEmpty
     * @param queue the name of the queue
     * @param autoAck true if the server should consider messages
     * acknowledged once delivered; false if the server should expect
     * explicit acknowledgements
     * @return a {@link GetResponse} containing the retrieved message data
     * @throws java.io.IOException if an error is encountered
     */
    GetResponse basicGet(String queue, boolean autoAck) throws IOException;




}
