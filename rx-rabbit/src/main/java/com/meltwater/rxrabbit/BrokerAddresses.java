package com.meltwater.rxrabbit;


import com.google.common.collect.ImmutableList;
import com.rabbitmq.client.ConnectionFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

import static com.rabbitmq.client.ConnectionFactory.DEFAULT_HOST;
import static com.rabbitmq.client.ConnectionFactory.DEFAULT_PASS;
import static com.rabbitmq.client.ConnectionFactory.DEFAULT_USER;
import static com.rabbitmq.client.ConnectionFactory.DEFAULT_VHOST;
import static com.rabbitmq.client.ConnectionFactory.USE_DEFAULT_PORT;

/**
 * This class represents a list AMQP URIs.
 *
 * @see {https://www.rabbitmq.com/uri-spec.html}
 */
public class BrokerAddresses implements Iterable<BrokerAddresses.BrokerAddress>{

    public final ImmutableList<BrokerAddress> addresses;

    public BrokerAddresses(String addressesString) {
        try {
            assert addressesString != null && !addressesString.isEmpty();
            List<BrokerAddress> out = new ArrayList<>();
            for (String amqUriString : addressesString.split(",")) {
                out.add(new BrokerAddressBuilder().withUriString(amqUriString.trim()).build());
            }
            this.addresses = ImmutableList.copyOf(out);
        }catch (Exception e){
            throw new IllegalArgumentException(e);
        }
    }

    public BrokerAddresses(List<BrokerAddress> addresses) {
        this.addresses = ImmutableList.copyOf(addresses);
    }

    public ImmutableList<BrokerAddress> getAddresses() {
        return addresses;
    }

    public BrokerAddress get(int i) {
        return addresses.get(i);
    }

    @Override
    public Iterator<BrokerAddress> iterator() {
        return addresses.iterator();
    }

    @Override
    public void forEach(Consumer<? super BrokerAddress> action) {
        addresses.forEach(action);
    }

    @Override
    public Spliterator<BrokerAddress> spliterator() {
        return addresses.spliterator();
    }

    public static class BrokerAddress {

        public final String username;
        public final String password;
        public final String virtualHost;
        public final String host;
        public final int port;

        private BrokerAddress(String username, String password, String virtualHost, String host, int port) {
            this.username = username;
            this.password = password;
            this.virtualHost = virtualHost;
            this.host = host;
            this.port = port;
        }

        @Override
        public String toString() {
            return "amqp://"+host+":"+(port==-1?5672:port)+"/"+(virtualHost.equals("/")?"":virtualHost);
        }

    }

    public static class BrokerAddressBuilder {

        private String username     = DEFAULT_USER;
        private String password     = DEFAULT_PASS;
        private String virtualHost  = DEFAULT_VHOST;
        private String host         = DEFAULT_HOST;
        private int port            = USE_DEFAULT_PORT;

        public BrokerAddress build() {
            return new BrokerAddress(username, password, virtualHost, host, port);
        }

        public BrokerAddressBuilder withUri(URI amqpUri) throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
            ConnectionFactory tmp = new ConnectionFactory();
            tmp.setUri(amqpUri);
            cloneConnectionSettings(tmp);
            return this;
        }

        public BrokerAddressBuilder withUriString(String amqpUriString) throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
            ConnectionFactory tmp = new ConnectionFactory();
            tmp.setUri(amqpUriString);
            cloneConnectionSettings(tmp);
            return this;
        }

        public BrokerAddressBuilder withUsername(String username) {
            this.username = username;
            return this;
        }

        public BrokerAddressBuilder withPassword(String password) {
            this.password = password;
            return this;
        }

        public BrokerAddressBuilder withVirtualHost(String virtual_host) {
            this.virtualHost = virtual_host;
            return this;
        }

        public BrokerAddressBuilder withHost(String host) {
            this.host = host;
            return this;
        }

        public BrokerAddressBuilder withPort(int port) {
            this.port = port;
            return this;
        }

        private void cloneConnectionSettings(ConnectionFactory tmp) {
            username = tmp.getUsername();
            password = tmp.getPassword();
            virtualHost = tmp.getVirtualHost();
            host = tmp.getHost();
            port = tmp.getPort();
        }
    }

}
