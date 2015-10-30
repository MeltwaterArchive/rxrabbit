package com.meltwater.rxrabbit;

/**
 * Interface wrapping a {@link com.rabbitmq.client.Channel}
 *
 * @see com.rabbitmq.client.Channel
 */
public interface ChannelWrapper {

    /**
     * Close this channel with the {@link com.rabbitmq.client.AMQP#REPLY_SUCCESS} close code
     * and message 'OK'.
     *
     * If this is the last open channel on the given {@link com.rabbitmq.client.Connection} then the connection will also be closed.
     */
    void close();

    /**
     * Close this channel with an error status.
     *
     * This method should be called even if the channel is known to be closed by an error to make
     * sure that the corresponding {@link com.rabbitmq.client.Connection} is also closed and all resources
     * are released.
     */
    void closeWithError();

    /**
     * Determine whether the channel is currently open.
     * Will return false if we are currently closing.
     *
     * Checking this method should be only for information,
     * because of the race conditions - state can change after the call.
     * Instead just execute and try to catch ShutdownSignalException
     * and IOException
     *
     * @return true when channel is open, false otherwise
     */
    boolean isOpen();

    /**
     * Retrieve this channel's channel number.
     * @return the channel number
     */
    int getChannelNumber();

}
