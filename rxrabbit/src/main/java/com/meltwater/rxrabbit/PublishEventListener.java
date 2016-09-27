package com.meltwater.rxrabbit;

/**
 * Listener that get notified about publishing events
 */
public interface PublishEventListener {

    void beforePublish(PublishEvent event);

    void afterPublish(PublishEvent event);

    void afterConfirm(PublishEvent event);

    void afterIntermediateFail(PublishEvent event, Exception error, int msUntilNextAttempt);

    void afterFinalFail(PublishEvent event, Exception error);
}
