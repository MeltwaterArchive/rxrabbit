package com.meltwater.rxrabbit;

/**
 * Used to report if processing of a {@link Message} succeeded or not.
 */
public interface Acknowledger {

	/**
	 * Call to indicate that the operation succeeded.
	 *
	 * @see ConsumeChannel#basicAck(long, boolean)
	 */
	void onDone(); //TODO change to ack

	/**
	 * Call to indicate that the operation failed.
	 *
	 * @see ConsumeChannel#basicNack(long, boolean)
	 */
	void onFail(); //TODO change to reject


}
