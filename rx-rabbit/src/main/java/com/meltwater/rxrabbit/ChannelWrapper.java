package com.meltwater.rxrabbit;

public interface ChannelWrapper {

    void close();

    void closeWithError();

    boolean isOpen();

    int getChannelNumber();

}
