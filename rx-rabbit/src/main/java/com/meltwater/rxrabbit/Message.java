package com.meltwater.rxrabbit;

import com.rabbitmq.client.BasicProperties;
import com.rabbitmq.client.Envelope;

import java.util.Arrays;

/**
 * This class wraps all the data delivered by the rabbitmq broker for a given message.
 *
 * The message also contains an {@link Acknowledger} that must be used to report if the messages is should be acked or nacked.
 *
 * @see ConsumerFactory#createConsumer(String)
 */
public class Message {

    /**
     * The acknowledger is used to tell the consumer that the message is either safe to acknowledge
     * or that the processing of it failed.
     *
     * NOTE:
     * The consuming code is expected to call {@link Acknowledger#onDone()} or {@link Acknowledger#onFail()} as soon as it can.
     * Failure in doing so will in most cases make the message stream pause as rabbitmq will be waiting for acks
     * before delivering more messages.
     *
     * The only time the message flow will continue is if auto ack on delivery is used.
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return basicProperties.equals(message.basicProperties) && envelope.equals(message.envelope) && Arrays.equals(payload, message.payload);

    }

    @Override
    public int hashCode() {
        int result = basicProperties.hashCode();
        result = 31 * result + envelope.hashCode();
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }
}
