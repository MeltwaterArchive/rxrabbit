package com.meltwater.rxrabbit.util;

public class FibonacciBackoffAlgorithm implements BackoffAlgorithm{

    private static final int[] SEQUENCE = new int[] { 1, 1, 2, 3, 5, 8, 13, 21, 34};

    private int getDelaySec(int attempt) {
        return attempt< SEQUENCE.length? SEQUENCE[attempt]: SEQUENCE[SEQUENCE.length-1];
    }

    @Override
    public int getDelayMs(Integer attempts) {
        return 1_000 * getDelaySec(attempts);
    }
}
