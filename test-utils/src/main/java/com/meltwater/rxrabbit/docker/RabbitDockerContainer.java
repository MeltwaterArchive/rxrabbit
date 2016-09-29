package com.meltwater.rxrabbit.docker;

import com.google.common.base.Stopwatch;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.Duration;
import com.jayway.awaitility.core.ConditionTimeoutException;
import com.meltwater.docker.compose.Docker;
import com.meltwater.docker.compose.DockerCompose;
import com.meltwater.docker.compose.data.InspectData;

import java.util.concurrent.TimeUnit;

public class RabbitDockerContainer {

    private final long POLL_INTERVAL = 500L;

    private final DockerCompose compose;
    private final InspectDataCache inspectDataCache;
    private final String containerName;

    RabbitDockerContainer(DockerCompose compose, InspectDataCache inspectDataCache, String containerName) {
        this.compose = compose;
        this.inspectDataCache = inspectDataCache;
        this.containerName = containerName;
    }

    public String getTypeString() {
        return containerName;
    }

    public RabbitProbe getProbe() {
        return new RabbitProbe(tcpPort());
    }

    public String containerName() {
        String prefix = compose.getPrefix().toLowerCase();
        return "/"+prefix+"_"+getTypeString()+"_"+1;
    }

    public RabbitDockerContainer kill(){
        String instanceId = inspectDataCache.findId(containerName());
        Docker.INSTANCE.kill(instanceId);
        waitForContainerToDie(instanceId);
        return this;
    }

    public RabbitDockerContainer start(){
        String instanceId = inspectDataCache.findId(containerName());
        Docker.INSTANCE.start(instanceId);
        return this;
    }

    public RabbitDockerContainer assertUp() {
        RabbitProbe probe = getProbe();
        try {
            Awaitility
                    .with()
                    .pollDelay(2, TimeUnit.SECONDS)
                    .pollInterval(500, TimeUnit.MILLISECONDS)
                    .await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(probe::isSatisfied);
        } catch(ConditionTimeoutException e) {
            throw new RuntimeException(probe.getLatestError());
        }
        return this;
    }

    private RabbitDockerContainer waitForContainerToDie(String containerId) {
        Stopwatch stopwatch = Stopwatch.createStarted();

        while (stopwatch.elapsed(TimeUnit.SECONDS) < 30) {
            InspectData inspect  = Docker.INSTANCE.inspect(containerId).get(0);
            if (!inspect.getState().getRunning()) {
                return this;
            }else{
                try {
                    Thread.sleep(POLL_INTERVAL);
                } catch (InterruptedException ignored) {}
            }
        }
        throw new RuntimeException("Container "+containerId+" failed to die in under 30 seconds.");
    }

    public String tcpPort() {
        return inspectDataCache.bindingForTcpPort(containerName(), "5672");
    }

    public String adminPort() {
        return inspectDataCache.bindingForTcpPort(containerName(), "15672");
    }

}
