package com.meltwater.rxrabbit;


import com.rabbitmq.client.ConnectionFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import static com.rabbitmq.client.ConnectionFactory.DEFAULT_HOST;
import static com.rabbitmq.client.ConnectionFactory.DEFAULT_PASS;
import static com.rabbitmq.client.ConnectionFactory.DEFAULT_USER;
import static com.rabbitmq.client.ConnectionFactory.DEFAULT_VHOST;
import static com.rabbitmq.client.ConnectionFactory.USE_DEFAULT_PORT;

/**
 * This class represents an AMQP URI.
 *
 * @see {https://www.rabbitmq.com/uri-spec.html}
 */
public class BrokerAddress {

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

    public static class Builder {

        private String username     = DEFAULT_USER;
        private String password     = DEFAULT_PASS;
        private String virtualHost  = DEFAULT_VHOST;
        private String host         = DEFAULT_HOST;
        private int port            = USE_DEFAULT_PORT;

        public BrokerAddress build() {
            return new BrokerAddress(username, password, virtualHost, host, port);
        }

        public Builder withUri(URI amqpUri) throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
            ConnectionFactory tmp = new ConnectionFactory();
            tmp.setUri(amqpUri);
            cloneConnectionSettings(tmp);
            return this;
        }

        public Builder withUriString(String amqpUriString) throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
            ConnectionFactory tmp = new ConnectionFactory();
            tmp.setUri(amqpUriString);
            cloneConnectionSettings(tmp);
            return this;
        }

        public Builder withUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder withVirtualHost(String virtual_host) {
            this.virtualHost = virtual_host;
            return this;
        }

        public Builder withHost(String host) {
            this.host = host;
            return this;
        }

        public Builder withPort(int port) {
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
