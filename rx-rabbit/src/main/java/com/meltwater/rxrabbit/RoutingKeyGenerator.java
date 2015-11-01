package com.meltwater.rxrabbit;

public interface RoutingKeyGenerator<T> {
    String generateRoutingKey(T obj);
}
