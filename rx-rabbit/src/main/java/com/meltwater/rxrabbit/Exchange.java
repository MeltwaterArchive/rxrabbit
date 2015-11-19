package com.meltwater.rxrabbit;

/**
 * A typed {@link String} representing an exchange name to make the {@link RabbitPublisher} type more descriptive
 */
public class Exchange {

    public final String name;

    public Exchange(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Exchange exchange = (Exchange) o;
        return !(name != null ? !name.equals(exchange.name) : exchange.name != null);

    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }
}
