package com.meltwater.rxrabbit;

/**
 * A typed {@link String} representing an exchange name to make the {@link RabbitPublisher} type more descriptive
 */
public class Exchange {

    public final String name;

    public Exchange(String name) {
        this.name = name;
    }
}
