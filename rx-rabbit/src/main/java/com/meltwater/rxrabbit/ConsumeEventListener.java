package com.meltwater.rxrabbit;

/**
 * Listener that get notified about consume and ack/nack events
 */
public interface ConsumeEventListener {

    void received(Message message, long unAckedMessages);

    void beforeAck(Message message);

    void beforeNack(Message message);

    void ignoredAck(Message message);

    void ignoredNack(Message message);

    void afterFailedAck(Message message, Exception error, boolean channelIsOpen);

    void afterFailedNack(Message message, Exception error, boolean channelIsOpen);

    void done(Message message, long unAckedMessages, long ackStartTimestamp, long processingStartTimestamp);
}
