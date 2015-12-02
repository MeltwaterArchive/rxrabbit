package com.meltwater.rxrabbit.impl;

import com.meltwater.rxrabbit.util.Fibonacci;
import com.meltwater.rxrabbit.util.Logger;
import rx.Observable;
import rx.functions.Func1;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.meltwater.rxrabbit.ConsumerSettings.RETRY_FOREVER;

public class ConnectionRetryHandler implements Func1<Observable<? extends Throwable>, Observable<?>> {

    private static final Logger log = new Logger(ConnectionRetryHandler.class);
    private final AtomicInteger connectAttempt = new AtomicInteger();
    private final int maxReconnectAttempts;

    public ConnectionRetryHandler(int maxReconnectAttempts) {
        this.maxReconnectAttempts = maxReconnectAttempts;
    }

    @Override
    public Observable<?> call(Observable<? extends Throwable> observable) {
        return observable.flatMap(throwable -> {
            int conAttempt = connectAttempt.get();
            if (maxReconnectAttempts == RETRY_FOREVER || conAttempt < maxReconnectAttempts) {
                final int delaySec = Fibonacci.getDelaySec(conAttempt);
                connectAttempt.incrementAndGet();
                log.infoWithParams("Scheduling attempting to restart consumer",
                        "attempt", connectAttempt,
                        "delaySeconds", delaySec);
                return Observable.timer(delaySec, TimeUnit.SECONDS);
            } else {
                return Observable.error(throwable);
            }
        });
    }

    public void reset() {
        connectAttempt.set(0);
    }
}
