package com.meltwater.rxrabbit;

import com.rabbitmq.client.BasicProperties;
import com.rabbitmq.client.Envelope;

/**
 * This class wraps all the data delivered by the rabbitmq broker for a given message.
 *
 * @see ConsumerFactory#createConsumer(String)
 */
public class Message {

    /**
     * The acknowledger is used to tell the consumer that the message is either safe to acknowledge
     * or that the processing of it failed.
     *
     * NOTE the consuming code is expected to call one of the acknowledgers methods as so as it can.
     * Failure in doing so will in most cases make the message stream stop as rabbitmq will be waiting for acks
     * before delivering more messages.
     */
    public final Acknowledger acknowledger;

    /**
     * The message properties. FOr example messageId, content type, and custom headers.
     */
    public final BasicProperties basicProperties;

    /**
     * The message envelope metadata such as the deliveryTag and the exchange it came from and the routing key
     */
    public final Envelope envelope;

    /**
     * The message body
     */
    public final byte[] payload;

    public Message(Acknowledger acknowledger, Envelope envelope, BasicProperties basicProperties, byte[] payload) {
        this.acknowledger = acknowledger;
        this.envelope = envelope;
        this.basicProperties = basicProperties;
        this.payload = payload;
    }
}
