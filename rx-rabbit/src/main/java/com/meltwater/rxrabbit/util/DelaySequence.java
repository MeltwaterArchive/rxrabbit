package com.meltwater.rxrabbit.util;

public class DelaySequence {

    private static final int[] SEQUENCE = new int[] { 1, 1, 2, 3, 5, 7, 10, 20, 30};

    public static int getDelaySec(int attempt) {
        return attempt< SEQUENCE.length? SEQUENCE[attempt]: SEQUENCE[SEQUENCE.length-1];
    }

    public static int getDelayMillis(int attempt) {
        return 1_000*getDelaySec(attempt);
    }
}
