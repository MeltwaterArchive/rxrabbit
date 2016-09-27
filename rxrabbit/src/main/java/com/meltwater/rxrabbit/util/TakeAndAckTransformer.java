package com.meltwater.rxrabbit.util;

import com.meltwater.rxrabbit.Message;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static rx.Observable.just;

/**
 * This transformer can be used if you want to read from a rabbit queue 'for a while' and then stop reading, either
 * because there are no messages seen within a given timeout, or because the desired number of messages has been consumed.
 *
 * When either of the above conditions are satisfied, then the transformed observable will complete and the
 * source observable will be {@link Subscriber#unsubscribe()} from.
 *
 * It is possible to give 0 or a negative number for the max seen messages parameter which is interpreted as infinity.
 *
 * Also note that the messages that are sent to the transformed observable will be acknowledged before they are passed on.
 *
 * @see Observable#takeUntil(Func1)
 *
 * @see <a href="http://blog.danlew.net/2015/03/02/dont-break-the-chain/">http://blog.danlew.net/2015/03/02/dont-break-the-chain/</a>
 */
public class TakeAndAckTransformer implements Observable.Transformer<Message, Message>{

    private static final Message STOP = new StopMessage();

    private final long takeMax;
    private final long timeout;
    private final TimeUnit timeUnit;

    /**
     * @param takeMax
     *            the maximum number of messages to take before completing
     *            If 0 or a negative number is given for the takeMax it means to continue consuming forever until either the
     *            timeout occurs or some other error condition is encountered.
     * @param timeoutMillis
     *            maximum duration (in millis) between emitted items before a timeout occurs and the source observable is unsubscribed from
     */
    public TakeAndAckTransformer(long takeMax, long timeoutMillis) {
        this(takeMax, timeoutMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * @param timeout
     *            maximum duration between emitted items before a timeout occurs and the source observable is unsubscribed from
     * @param timeUnit
     *            the unit of time that applies to the {@code timeout} argument.
     */
    public TakeAndAckTransformer(long timeout, TimeUnit timeUnit) {
        this(0,timeout,timeUnit);
    }

    /**
     * @param takeMax
     *            the maximum number of messages to take before completing.
     *            If 0 or a negative number is given for the takeMax it means to continue consuming forever until either the
     *            timeout occurs or some other error condition is encountered.
     * @param timeout
     *            maximum duration between emitted items before a timeout occurs and the source observable is unsubscribed from
     * @param timeUnit
     *            the unit of time that applies to the {@code timeout} argument.
     */
    public TakeAndAckTransformer(long takeMax, long timeout, TimeUnit timeUnit) {
        assert timeout>0;
        this.timeout = timeout;
        this.timeUnit = timeUnit;
        this.takeMax = takeMax;
    }

    @Override
    public Observable<Message> call(Observable<Message> input) {
        final AtomicLong consumedCount = new AtomicLong(0);
        return input
                .doOnNext(message -> message.acknowledger.ack())
                .timeout(timeout, timeUnit, just(STOP))
                .takeUntil(message -> message == STOP || consumedEnough(consumedCount.incrementAndGet()) )
                .filter(message -> message != STOP);
    }

    private static class StopMessage extends Message {
        public StopMessage() {
            super(null, null, null, null);
        }
    }

    private boolean consumedEnough(long consumed) {
        return (takeMax > 0 && consumed >= takeMax);
    }

}
