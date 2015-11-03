package com.meltwater.rxrabbit;

import com.rabbitmq.client.Consumer;

import java.io.IOException;
import java.util.Map;

/**
 * Interface wrapping a {@link com.rabbitmq.client.Channel} that allows consume operations.
 *
 * Note that an instance of this channel is tied to only one queue.
 *
 * @see PublishChannel
 * @see AdminChannel
 * @see com.rabbitmq.client.Channel
 */
public interface ConsumeChannel extends ChannelWrapper{

    /**
     *
     * @return the queue this channel is able to consume from
     */
    String getQueue();

    /**
     * Cancel a consumer. Calls the consumer's {@link Consumer#handleCancelOk}
     * method.
     *
     * @param consumerTag a client- or server-generated consumer tag to establish context
     * @throws IOException if an error is encountered, or if the consumerTag is unknown
     * @see com.rabbitmq.client.AMQP.Basic.Cancel
     * @see com.rabbitmq.client.AMQP.Basic.CancelOk
     */
    void basicCancel(String consumerTag) throws IOException;

    /**
     * Acknowledge one or several received
     * messages. Supply the deliveryTag from the {@link com.rabbitmq.client.AMQP.Basic.GetOk}
     * or {@link com.rabbitmq.client.AMQP.Basic.Deliver} method
     * containing the received message being acknowledged.
     * @see com.rabbitmq.client.AMQP.Basic.Ack
     * @param deliveryTag the tag from the received {@link com.rabbitmq.client.AMQP.Basic.GetOk} or {@link com.rabbitmq.client.AMQP.Basic.Deliver}
     * @param multiple true to acknowledge all messages up to and
     * including the supplied delivery tag; false to acknowledge just
     * the supplied delivery tag.
     * @throws java.io.IOException if an error is encountered
     */
    void basicAck(long deliveryTag, boolean multiple) throws IOException;

    /**
     * Reject one or several received messages. Re-queue is set to false.
     *
     * Supply the <code>deliveryTag</code> from the {@link com.rabbitmq.client.AMQP.Basic.GetOk}
     * or {@link com.rabbitmq.client.AMQP.Basic.GetOk} method containing the message to be rejected.
     * @see com.rabbitmq.client.AMQP.Basic.Nack
     * @param deliveryTag the tag from the received {@link com.rabbitmq.client.AMQP.Basic.GetOk} or {@link com.rabbitmq.client.AMQP.Basic.Deliver}
     * @param multiple true to reject all messages up to and including
     * the supplied delivery tag; false to reject just the supplied
     * delivery tag.
     * than discarded/dead-lettered
     * @throws java.io.IOException if an error is encountered
     */
    void basicNack(long deliveryTag, boolean multiple) throws IOException;


    /**
     * Start a non-nolocal, non-exclusive consumer with auto ack set to false
     * and empty parameter map.
     *
     * @param consumerTag a client-generated consumer tag to establish context
     * @param callback an interface to the consumer object
     *
     * @throws java.io.IOException if an error is encountered
     * @see com.rabbitmq.client.AMQP.Basic.Consume
     * @see com.rabbitmq.client.AMQP.Basic.ConsumeOk
     *
     * @see com.rabbitmq.client.Channel#basicConsume(String, boolean, String, boolean, boolean, Map, Consumer)
     */
    void basicConsume(String consumerTag, Consumer callback) throws IOException;

    /**
     * Request a specific prefetchCount "quality of service" settings
     * for this channel.
     *
     * @see com.rabbitmq.client.Channel#basicQos(int)
     * @param prefetchCount maximum number of messages that the server
     * will deliver, 0 if unlimited
     * @throws java.io.IOException if an error is encountered
     */
    void basicQos(int prefetchCount) throws IOException;
}
