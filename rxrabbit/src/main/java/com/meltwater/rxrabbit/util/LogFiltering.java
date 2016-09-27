package com.meltwater.rxrabbit.util;

import java.util.ArrayList;
import java.util.List;

public class LogFiltering {

    /**
     * This method will filter out rxjava parts of the stack trace to make error
     * messages more compact and more readable. The method is applied recursive
     * down to the root cause throwable.
     *
     * @param throwable the throwable that should have its stack trace filtered
     *
     * NOTE this method will remove the stack trace directly on the throwable
     * that is sent in as the argument
     *
     * @return the same throwable as given in the input the argument, but with filtered stack trace
     */
    static <T extends Throwable> T filterStackTrace(T throwable){

        if (throwable.getCause() != null) {
            filterStackTrace(throwable.getCause());
        }
        List<StackTraceElement> filteredTrace = new ArrayList<>();
        for (StackTraceElement e : throwable.getStackTrace()){
            if (!(e.getClassName().contains("rx.Observable") ||
                    e.getClassName().contains("rx.Single") ||
                    e.getClassName().contains("rx.observables") ||
                    e.getClassName().contains("rx.observers") ||
                    e.getClassName().contains("rx.subjects") ||
                    e.getClassName().contains("rx.subscriptions") ||
                    e.getClassName().contains("rx.Subscriber") ||
                    e.getClassName().contains("rx.internal") ||
                    e.getClassName().contains("rx.operators")))
            {

                filteredTrace.add(e);
            }
        }
        throwable.setStackTrace(filteredTrace.toArray(new StackTraceElement[filteredTrace.size()]));
        return throwable;
    }


}
