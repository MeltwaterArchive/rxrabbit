package com.meltwater.rxrabbit;

import com.meltwater.rxrabbit.docker.DockerContainers;
import com.meltwater.rxrabbit.util.Logger;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * gist http://www.thinkcode.se/blog/2012/07/08/performing-an-action-when-a-test-fails
 */

public class HandleFailuresRule extends TestWatcher {

    private static final Logger log = new Logger(HandleFailuresRule.class);
    private final DockerContainers dockerContainers;

    public HandleFailuresRule(DockerContainers dockerContainers) {
        this.dockerContainers = dockerContainers;
    }

    @Override
    protected void failed(Throwable e, Description description) {
        log.errorWithParams("TEST FAILED", e, "name", description.toString());
        log.infoWithParams("****** Restarting all docker containers ******");
        dockerContainers.rabbit().kill();
        dockerContainers.resetAll(false);
        dockerContainers.rabbit().assertUp();
        log.infoWithParams("****** Rabbit broker is up and running *****");
    }

}