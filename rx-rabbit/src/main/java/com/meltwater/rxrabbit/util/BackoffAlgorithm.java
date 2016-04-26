package com.meltwater.rxrabbit.util;

public interface BackoffAlgorithm {
    public int getDelayMs(Integer attempts);
}
