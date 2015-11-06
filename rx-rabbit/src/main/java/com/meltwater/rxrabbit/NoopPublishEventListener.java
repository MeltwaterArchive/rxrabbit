package com.meltwater.rxrabbit;

public class NoopPublishEventListener implements PublishEventListener {
    @Override
    public void beforePublish(PublishEvent event) {
    }

    @Override
    public void afterPublish(PublishEvent event) {
    }

    @Override
    public void afterConfirm(PublishEvent event) {
    }

    @Override
    public void afterIntermediateFail(PublishEvent event, Exception error, int secsUntilNextAttempt) {
    }

    @Override
    public void afterFinalFail(PublishEvent event, Exception error) {
    }
}
