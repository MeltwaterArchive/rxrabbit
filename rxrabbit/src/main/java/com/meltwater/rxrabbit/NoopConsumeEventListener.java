package com.meltwater.rxrabbit;

public class NoopConsumeEventListener implements ConsumeEventListener {
    @Override
    public void received(Message message, long unAckedMessages) {
    }

    @Override
    public void beforeAck(Message message) {
    }

    @Override
    public void beforeNack(Message message) {
    }

    @Override
    public void ignoredAck(Message message) {
    }

    @Override
    public void ignoredNack(Message message) {
    }

    @Override
    public void afterFailedAck(Message message, Exception error, boolean channelIsOpen) {
    }

    @Override
    public void afterFailedNack(Message message, Exception error, boolean channelIsOpen) {
    }

    @Override
    public void done(Message message, long unAckedMessages, long ackStartTimestamp, long processingStartTimestamp) {
    }
}
