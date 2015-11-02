package com.meltwater.rxrabbit;

public interface PublishEventListener {

    default void beforePublish(PublishEvent event){}

    default void afterPublish(PublishEvent event){}

    default void afterConfirm(PublishEvent event){}

    default void afterIntermediateFail(PublishEvent event, Exception error, int secsUntilNextAttempt){}

    default void afterFinalFail(PublishEvent event, Exception error){}
}
