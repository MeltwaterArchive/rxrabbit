package com.meltwater.rxrabbit;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConfirmListener;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Interface wrapping a {@link com.rabbitmq.client.Channel} that allows publish operations.
 *
 * @see ConsumeChannel
 * @see AdminChannel
 * @see com.rabbitmq.client.Channel
 */
public interface PublishChannel extends ChannelWrapper{

    String getExchange();

    /**
     * Add a {@link ConfirmListener}.
     * @param listener the listener to add
     */
    void addConfirmListener(ConfirmListener listener);

    /**
     * Publish a message.
     *
     * Publishing to a non-existent exchange will result in a channel-level
     * protocol exception, which closes the channel.
     *
     * Invocations of <code>Channel#basicPublish</code> will eventually block if a
     * <a href="http://www.rabbitmq.com/alarms.html">resource-driven alarm</a> is in effect.
     *
     * @see com.rabbitmq.client.AMQP.Basic.Publish
     * @see <a href="http://www.rabbitmq.com/alarms.html">Resource-driven alarms</a>.
     * @param exchange the exchange to publish the message to
     * @param routingKey the routing key
     * @param props other properties for the message - routing headers etc
     * @param body the message body
     * @throws java.io.IOException if an error is encountered
     */
    void basicPublish(String exchange, String routingKey, AMQP.BasicProperties props, byte[] body) throws IOException;


    /**
     * When in confirm mode, returns the sequence number of the next
     * message to be published.
     * @return the sequence number of the next message to be published
     */
    long getNextPublishSeqNo();

    /**
     * Wait until all messages published since the last call have been
     * either ack'd or nack'd by the broker.  Note, when called on a
     * non-Confirm channel, waitForConfirms throws an IllegalStateException.
     * @return whether all the messages were ack'd (and none were nack'd)
     * @throws java.lang.IllegalStateException
     */
    boolean waitForConfirms() throws InterruptedException;

    /**
     * Wait until all messages published since the last call have been
     * either ack'd or nack'd by the broker; or until timeout elapses.
     * If the timeout expires a TimeoutException is thrown.  When
     * called on a non-Confirm channel, waitForConfirms throws an
     * IllegalStateException.
     * @return whether all the messages were ack'd (and none were nack'd)
     * @throws java.lang.IllegalStateException
     */
    boolean waitForConfirms(long timeout) throws InterruptedException, TimeoutException;

}
