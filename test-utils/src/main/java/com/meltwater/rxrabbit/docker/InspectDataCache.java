package com.meltwater.rxrabbit.docker;

import com.meltwater.docker.compose.data.InspectData;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

class InspectDataCache {

    private final List<InspectData> cache = new ArrayList<>();

    String findId(String typeString) {
        return findData(typeString).getId();
    }

    String bindingForTcpPort(String typeString, String port) {
        return findData(typeString).bindingForTcpPort(port);
    }

    private InspectData findData(String typeString) {
        if (cache.isEmpty()) {
            throw new RuntimeException("One must start containers with before trying to operate on them.");
        }

        Optional<InspectData> data = cache.stream().filter(inspectData -> inspectData.getName().contains(typeString)).findFirst();

        if (data.isPresent()){
            return data.get();
        }else {
            throw new RuntimeException("No instance with the \""+typeString+"\" on it's name was found");
        }
    }

    Boolean isEmpty() {
        return cache.isEmpty();
    }

    void populate(List<InspectData> upResult) {
        cache.addAll(upResult);
    }

    void clear() {
        cache.clear();
    }
}
