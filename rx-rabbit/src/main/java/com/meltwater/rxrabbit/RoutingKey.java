package com.meltwater.rxrabbit;

/**
 * A typed {@link String} representing a routing key name to make the {@link RabbitPublisher} type more descriptive
 */
public class RoutingKey {

    public final String value;

    public RoutingKey(String value) {
        this.value = value;
    }
}
