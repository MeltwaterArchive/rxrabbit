package com.meltwater.rxrabbit;

/**
 * Can generate a routing key given some input
 *
 * @param <T> the type of the input data
 */
public interface RoutingKeyGenerator<T> {

    RoutingKey generateRoutingKey(T obj);

}
