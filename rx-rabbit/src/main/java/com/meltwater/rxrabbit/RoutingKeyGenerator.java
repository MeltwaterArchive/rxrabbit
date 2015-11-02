package com.meltwater.rxrabbit;

public interface RoutingKeyGenerator<T> {

    RoutingKey generateRoutingKey(T obj);

}
