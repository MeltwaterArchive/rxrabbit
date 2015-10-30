package com.meltwater.rxrabbit.metrics;

//TODO should NOT be open sourced
public interface RxRabbitMetricsReporter {
    String COUNTER_RECEIVED = "received";
    String COUNTER_COMPLETED = "completed";
    String COUNTER_FAILED = "failed";

    default void reportCount(String metricsName, int count){};

    default void reportCount(String metricsName){};

    default void reportGauge(String metricsName, int gauge){};

    default void reportTime(String metricsName, long timeMs){};

}
