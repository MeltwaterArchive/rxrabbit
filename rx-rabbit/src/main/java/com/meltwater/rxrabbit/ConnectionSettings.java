package com.meltwater.rxrabbit;

import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class contains rabbitmq connection with things. Some withtings are part of the official AMQP URI spec v 0-9-1,
 * others are domain specific to this library and is used to configure the {@link com.rabbitmq.client.ConnectionFactory}.
 *
 * @see <a href="https://www.rabbitmq.com/uri-query-parameters.html">AMQP uri-query-parameters</a>
 */
public class ConnectionSettings {

    public static final int DEFAULT_HEARTBEAT = 10;
    public static final int DEFAULT_CONNECTION_TIMEOUT = 30_000;
    public static final int DEFAULT_TIMEOUT_MILLIS = 30_000;
    public static final int DEFAULT_HANDSHAKE_MILLIS = 10_000;
    public static final int DEFAULT_FRAME_MAX = 0;

    private int heartbeat                   = DEFAULT_HEARTBEAT; //in seconds
    private int connection_timeout_millis   = DEFAULT_CONNECTION_TIMEOUT;
    private int shutdown_timeout_millis     = DEFAULT_TIMEOUT_MILLIS;
    private int handshake_timeout_millis    = DEFAULT_HANDSHAKE_MILLIS;
    private int frame_max                   = DEFAULT_FRAME_MAX; //0 = Infinite

    private Map<String,String> client_properties = new HashMap<>();

    /*
    //These params below are here for reference, but they are not yet implemented
    channel_max
    cacertfile
    certfile
    keyfile
    verify
    fail_if_no_peer_cert
     */

    public ConnectionSettings() {}


    public int getHeartbeat() {
        return heartbeat;
    }

    public int getConnection_timeout_millis() {
        return connection_timeout_millis;
    }

    public int getShutdown_timeout_millis() {
        return shutdown_timeout_millis;
    }

    public int getHandshake_timeout_millis() {
        return handshake_timeout_millis;
    }

    public int getFrame_max() {
        return frame_max;
    }

    public Map<String, String> getClient_properties() {
        return client_properties;
    }

    public ConnectionSettings withHeartbeatSecs(int heartbeat) {
        assert heartbeat>0;
        this.heartbeat = heartbeat;
        return this;
    }

    public ConnectionSettings withConnectionTimeoutMillis(int connection_timeout_millis) {
        assert connection_timeout_millis>=0;
        this.connection_timeout_millis = connection_timeout_millis;
        return this;
    }

    public ConnectionSettings withShutdownTimeoutMillis(int shutdown_timeout_millis) {
        assert shutdown_timeout_millis>=0;
        this.shutdown_timeout_millis = shutdown_timeout_millis;
        return this;
    }

    public ConnectionSettings withHandshakeTimeoutMillis(int handshake_timeout_millis) {
        assert handshake_timeout_millis>=0;
        this.handshake_timeout_millis = handshake_timeout_millis;
        return this;
    }

    public ConnectionSettings withFrameMax(int frame_max) {
        this.frame_max = frame_max;
        return this;
    }

    public ConnectionSettings withClientProperties(Map<String, String> client_properties) {
        assert client_properties!=null;
        this.client_properties = client_properties;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConnectionSettings that = (ConnectionSettings) o;

        if (heartbeat != that.heartbeat) return false;
        if (connection_timeout_millis != that.connection_timeout_millis) return false;
        if (shutdown_timeout_millis != that.shutdown_timeout_millis) return false;
        if (handshake_timeout_millis != that.handshake_timeout_millis) return false;
        if (frame_max != that.frame_max) return false;
        return client_properties.toString().equals(that.client_properties.toString());
    }

    @Override
    public int hashCode() {
        int result = heartbeat;
        result = 31 * result + connection_timeout_millis;
        result = 31 * result + shutdown_timeout_millis;
        result = 31 * result + handshake_timeout_millis;
        result = 31 * result + frame_max;
        result = 31 * result + client_properties.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "{" +
                "heartbeat:" + heartbeat +
                ", connection_timeout_millis:" + connection_timeout_millis +
                ", shutdown_timeout_millis:" + shutdown_timeout_millis +
                ", handshake_timeout_millis:" + handshake_timeout_millis +
                ", frame_max:" + frame_max +
                ", client_properties:" + mapToString(client_properties) +
                '}';
    }

    private String mapToString(Map<String, String> map) {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        List<String> parts = new ArrayList<>();
        for (Map.Entry<String,String> e : map.entrySet()){
             parts.add(e.getKey()+":'"+e.getValue()+"'");
        }
        Joiner.on(", ").appendTo(builder, parts);
        return builder.append("}").toString();
    }

}
