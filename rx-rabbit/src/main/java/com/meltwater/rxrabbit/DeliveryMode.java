package com.meltwater.rxrabbit;

public enum DeliveryMode {
    non_persistent(1),
    persistent(2);

    public final int code;
    DeliveryMode(int code) {
        this.code = code;
    }
}
