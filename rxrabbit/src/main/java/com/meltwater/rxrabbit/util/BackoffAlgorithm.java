package com.meltwater.rxrabbit.util;

public interface BackoffAlgorithm {
    int getDelayMs(Integer attempts);
}
