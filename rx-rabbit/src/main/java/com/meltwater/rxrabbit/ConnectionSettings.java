package com.meltwater.rxrabbit;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

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

    private final Map<String,Object> defaultClientCapabilities = new HashMap<String, Object>() {{
        //Lets us receive cancellation events, such as the queue being deleted or that the node on which the queue is located failing
        put("consumer_cancel_notify", true);
        put("exchange_exchange_bindings", true);
        put("basic.nack", true);
        put("publisher_confirms",true);
    }};


    private Map<String,Object> client_properties = new HashMap<String,Object>();

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

    public Map<String, Object> getClient_properties() {
        client_properties.put("capabilities", defaultClientCapabilities);
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
        this.client_properties = new HashMap<>(client_properties);
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
        return mapToString(client_properties).equals(mapToString(that.client_properties));
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

    private String mapToString(Map<String, Object> map) {
        SortedMap<String, Object> sortedMap = sortMap(map);
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        List<String> parts = new ArrayList<>();
        for (Map.Entry<String,Object> e : sortedMap.entrySet()){
             parts.add(e.getKey()+":'"+e.getValue()+"'");
        }
        Joiner.on(", ").appendTo(builder, parts);
        return builder.append("}").toString();
    }

    private SortedMap<String, Object> sortMap(Map<String, Object> map) {
        SortedMap<String, Object> res = Maps.newTreeMap(new Comparator<String>()
        {
            public int compare(String o1, String o2)
            {
                return o1.compareTo(o2);
            }
        });
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            res.put(entry.getKey(),entry.getValue());
        }
        return res;
    }

}
