package com.meltwater.rxrabbit;

public interface ConsumeEventListener {

    default void received(Message message, long unAckedMessages){}

    default void beforeAck(Message message){}

    default void beforeNack(Message message){}

    default void ignoredAck(Message message){}

    default void ignoredNack(Message message){}

    default void afterFailedAck(Message message, Exception error, boolean channelIsOpen){}

    default void afterFailedNack(Message message, Exception error, boolean channelIsOpen){}

    default void done(Message message, long unAckedMessages, long ackStartTimestamp, long processingStartTimestamp){}
}
