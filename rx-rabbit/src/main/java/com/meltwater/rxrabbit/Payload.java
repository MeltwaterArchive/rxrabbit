package com.meltwater.rxrabbit;

public class Payload {

    public final byte[] data;

    public Payload(byte[] data) {
        this.data = data;
    }

    public int size() {
        return data.length;
    }

    @Override
    public String toString() {
        return "Payload{" +
                "size=" + data.length + " bytes"+
                '}';
    }
}
