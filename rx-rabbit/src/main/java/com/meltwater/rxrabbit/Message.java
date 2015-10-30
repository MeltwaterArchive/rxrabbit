package com.meltwater.rxrabbit;

import com.rabbitmq.client.BasicProperties;
import com.rabbitmq.client.Envelope;

public class Message {

    public final Acknowledger acknowledger;
    public final BasicProperties basicProperties;
    public final Envelope envelope;
    public final byte[] payload;

    public Message(Acknowledger acknowledger, Envelope envelope, BasicProperties basicProperties, byte[] payload) {
        this.acknowledger = acknowledger;
        this.envelope = envelope;
        this.basicProperties = basicProperties;
        this.payload = payload;
    }
}
