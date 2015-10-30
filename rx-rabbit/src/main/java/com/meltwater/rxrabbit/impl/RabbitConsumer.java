package com.meltwater.rxrabbit.impl;

import com.meltwater.rxrabbit.Message;
import rx.Observable;

interface RabbitConsumer {

    Observable<Message> consume();

}
