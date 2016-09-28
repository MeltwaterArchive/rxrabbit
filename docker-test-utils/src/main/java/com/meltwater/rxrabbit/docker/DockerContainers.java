package com.meltwater.rxrabbit.docker;


import com.meltwater.docker.compose.DockerCompose;
import com.meltwater.docker.compose.data.InspectData;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

public class DockerContainers {

    private final InspectDataCache inspectDataCache = new InspectDataCache();
    private final RabbitDockerContainer rabbitDockerContainer;
    private final DockerCompose compose;

    public DockerContainers(String composeFile, Class<?> testClass) {
        URL resource = testClass.getClassLoader().getResource(composeFile);
        if (resource == null){
            throw new RuntimeException("Could not find "+composeFile+" in classpath");
        }
        String dockerComposePath = resource.getPath();
        String resourcePath = Paths.get(dockerComposePath).getParent().toString();
        HashMap<String,String> testEnv = buildTestEnv(resourcePath);

        this.compose = new DockerCompose(composeFile, testClass.getSimpleName(), testEnv);
        this.rabbitDockerContainer = new RabbitDockerContainer(compose, inspectDataCache, "rabbit");
    }

    public DockerContainers(Class<?> testClass) {
        this("docker-compose.yml", testClass);
    }

    private HashMap<String, String> buildTestEnv(String resourcePath) {
        HashMap<String, String> env  = new HashMap<>();
        // Adds random ports to be used inside test yml files
        for(int i=1; i<10; i++){
            env.put("TEST_PORT_"+i, randomPort()+"");
        }
        env.put("RESOURCE_PATH", resourcePath);
        return env;
    }

    private int randomPort() {
        //NOTE this is not 100% perfect and can lead to port collisions in some edge cases.
        //But we'll fix it if it becomes a large problem
        try {
            ServerSocket socket = new ServerSocket(0);
            socket.close();
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void  resetAll(Boolean shouldBuild) {
        cleanup();
        if (shouldBuild) {
            compose.build();
        }
        this.up();
    }

    public void  build() {
        compose.build();
    }

    public void  up() {
        List<InspectData> upResult = compose.up();
        // Saving the nameToIdMap for later use
        // We do not save the full InspectDate because the state can/will become stale
        if (inspectDataCache.isEmpty()) {
            inspectDataCache.populate(upResult);
        }
    }

    public void cleanup() {
        compose.kill();
        this.rm();
    }

    public void rm() {
        compose.rm();
        inspectDataCache.clear();
    }

    public RabbitDockerContainer rabbit() {
        return rabbitDockerContainer;
    }

    public RabbitDockerContainer rabbit(String containerName) {
        return new RabbitDockerContainer(compose, inspectDataCache, containerName);
    }
}

