package com.meltwater.rxrabbit;

/**
 * A typed {@link String} representing a routing key name to make the {@link RabbitPublisher} type more descriptive
 */
public class RoutingKey {

    public final String value;

    public RoutingKey(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RoutingKey that = (RoutingKey) o;
        return !(value != null ? !value.equals(that.value) : that.value != null);
    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }

}
