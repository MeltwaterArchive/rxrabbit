package com.meltwater.rxrabbit;

/**
 * Used to report if processing of a {@link Message} succeeded or not.
 */
public interface Acknowledger {

	/**
	 * Call to indicate that the operation succeeded.
	 */
	void onDone();

	/**
	 * Call to indicate that the operation failed.
	 */
	void onFail();


}
