package com.meltwater.rxrabbit;

import rx.Observable;

public interface ConsumerFactory {

    public Observable<Message> createConsumer(String queue);
}
