package com.meltwater.rxrabbit.util;

public class ConstantBackoffAlgorithm implements BackoffAlgorithm {
    private final Integer backoffMs;

    public ConstantBackoffAlgorithm(Integer backoffMs) {
        this.backoffMs = backoffMs;
    }

    @Override
    public int getDelayMs(Integer integer) {
        return backoffMs;
    }
}
