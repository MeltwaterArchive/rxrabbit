package com.meltwater.rxrabbit.metrics;


import com.timgroup.statsd.StatsDClient;

//TODO should not be open sourced
public class RxStatsDMetricsReporter implements RxRabbitMetricsReporter{
    private final StatsDClient client;
    private final String prefix;

    public RxStatsDMetricsReporter(StatsDClient client, String prefix) {
        this.client = client;
        this.prefix = prefix;
    }


    @Override
    public void reportCount(String counterName, int count) {
        this.client.count(this.prefix + counterName, (long)count);
    }

    @Override
    public void reportCount(String counterName) {
        this.client.incrementCounter(this.prefix + counterName);
    }

    @Override
    public void reportGauge(String gaugeName, int value) {
        this.client.recordGaugeValue(this.prefix + gaugeName, value);
    }
    @Override
    public void reportTime(String timerName, long time) {
        this.client.recordExecutionTime(this.prefix + timerName, (int)time);
    }

}
