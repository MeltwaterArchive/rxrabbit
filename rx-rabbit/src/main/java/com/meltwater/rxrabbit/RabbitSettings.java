package com.meltwater.rxrabbit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.meltwater.rxrabbit.util.Logger;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Map;

/**
 * This class contains rabbitmq connection settings. Some settings are part of the official AMQP URI spec v 0-9-1,
 * others are domain specific to this library and is used to configure the {@link ConnectionFactory} and {@link PublisherFactory}.
 *
 * Some settings are only relevant for consumers, some are only relevant for producers and some are used by
 * both consumers and publishers.
 *
 * This object can be built pragmatically by using the various {@link RabbitSettings.Builder}
 * withXX methods or by supplying a JSON formatted String to the {@link RabbitSettings.Builder#withSettingsJSON(String)} method.
 *
 * The available JSON parameter names are included as String constants but they also directly corresponds to the names of the fields in this class.
 *
 * The {@link #toString()} method shows the JSON representation of this class and can be used as input to the
 * {@link RabbitSettings.Builder#withSettingsJSON(String)} method. method.
 *
 * @see {@link ConsumerFactory}
 * @see {@link PublisherFactory}
 * @see {https://www.rabbitmq.com/uri-query-parameters.html}
 */
public class RabbitSettings {

    private final static Logger log = new Logger(PublisherFactory.class);
    private final static ObjectMapper mapper = new ObjectMapper();

    //NOTE these params are here for reference, but they are not yet implemented
    public final static String channel_max_param = "channel_max"; //NOTE we use consume/publish_channels instead
    public final static String cacertfile_param = "cacertfile"; //NOTE all ssl settings are implemented as soon as someone needs them
    public final static String certfile_param = "certfile";
    public final static String keyfile_param = "keyfile";
    public final static String verify_param = "verify";
    public final static String fail_if_no_peer_cert_param = "fail_if_no_peer_cert";

    public static final int DEFAULT_NUM_CHANNELS            = 1;
    public static final int DEFAULT_PRE_FETCH               = 0; //no pre-fetch
    public static final int DEFAULT_CLOSE_TIMEOUT_MILLIS    = 30_000;
    public static final int DEFAULT_HEARTBEAT               = 5;
    public static final int DEFAULT_CONNECTION_TIMEOUT      = 10_000;
    public static final int RETRY_COUNT                     = 0; //= forever
    public static final int DEFAULT_PUBLISH_TIMEOUT_SECS    = 20;
    public static final boolean DEFAULT_PUBLISHER_CONFIRMS  = false;

    public final static String heartbeat_param                 = "heartbeat";
    public final static String pre_fetch_count_param           = "pre_fetch_count";
    public final static String connection_timeout_param        = "connection_timeout_millis";
    public final static String shutdown_timeout_param          = "shutdown_timeout_millis";
    public final static String num_channels_param              = "num_channels";
    public final static String retry_count_param               = "retry_count";
    public final static String publish_timeout_secs_param      = "publish_timeout_secs";
    public final static String frame_max_param                 = "frame_max";
    public final static String close_timeout_millis_param      = "close_timeout_millis";
    public final static String handshake_timeout_millis_param  = "handshake_timeout_millis";
    public final static String publisher_confirms_param        = "publisher_confirms";

    //TODO javadoc the settings!!
    //NOTE 0 means forever for all timeout settings
    public final int heartbeat;
    public final int pre_fetch_count;
    public final int connection_timeout_millis;
    public final int shutdown_timeout_millis;
    public final int num_channels;
    public final int retry_count;
    public final int publish_timeout_secs;
    public final int frame_max;
    public final int close_timeout_millis;
    public final int handshake_timeout_millis;

    public final boolean publisher_confirms;

    public final String app_instance_id;

    private RabbitSettings(String appId, boolean publisherConfirms, int preFetchCount, int heartbeat, int connectionTimeout, int shutdownTimeout, int retryCount, int closeTimeout, int numChannels, int publishTimeoutSecs, int frameMax, int handshakeTimeout) {
        this.publisher_confirms = publisherConfirms;
        this.pre_fetch_count = preFetchCount;
        this.heartbeat = heartbeat;
        this.connection_timeout_millis = connectionTimeout;
        this.shutdown_timeout_millis = shutdownTimeout;
        this.retry_count = retryCount;
        this.app_instance_id = appId;
        this.close_timeout_millis = closeTimeout;
        this.num_channels = numChannels;
        publish_timeout_secs = publishTimeoutSecs;
        this.frame_max = frameMax;
        this.handshake_timeout_millis = handshakeTimeout;
    }

    @Override
    public String toString() {
        try {
            return mapper.writeValueAsString(this);
        } catch (IOException e) {
            return e.toString();
        }
    }

    public static class Builder {

        private boolean publisherConfirms   = DEFAULT_PUBLISHER_CONFIRMS;
        private int closeTimeout            = DEFAULT_CLOSE_TIMEOUT_MILLIS;
        private int preFetchCount           = DEFAULT_PRE_FETCH;
        private int heartbeat               = DEFAULT_HEARTBEAT;
        private int connectionTimeout       = DEFAULT_CONNECTION_TIMEOUT;
        private int shutdownTimeout         = ConnectionFactory.DEFAULT_SHUTDOWN_TIMEOUT;
        private int numChannels             = DEFAULT_NUM_CHANNELS;
        private int frameMax                = ConnectionFactory.DEFAULT_FRAME_MAX;
        private int handshakeTimeout        = ConnectionFactory.DEFAULT_HANDSHAKE_TIMEOUT;
        private int retryCount              = RETRY_COUNT;
        private int publishTimeoutSecs      = DEFAULT_PUBLISH_TIMEOUT_SECS;

        private String appId                = "unknown";

        /**
         * Fills in the values supplied in the JSON formatted string. The available parameter values
         *
         * @param settingsJSONString
         * @return this builder with all the values provided in the JSON filled in
         */
        public Builder withSettingsJSON(String settingsJSONString){
            if (!settingsJSONString.startsWith("{")){
                settingsJSONString = "{"+settingsJSONString+"}";
            }
            try {
                Map<String,String> map = (Map<String,String>)mapper.readValue(settingsJSONString, Map.class);
                if (map.containsKey(publisher_confirms_param) && (
                        "true".equals(map.get(publisher_confirms_param).toLowerCase()) ||
                                "1".equals(map.get(publisher_confirms_param)))) {
                    publisherConfirms = true;
                }
                if (map.containsKey(close_timeout_millis_param)) {
                    closeTimeout = Integer.parseInt(map.get(close_timeout_millis_param));
                    assert closeTimeout>=0;
                }
                if (map.containsKey(publish_timeout_secs_param)) {
                    publishTimeoutSecs = Integer.parseInt(map.get(publish_timeout_secs_param));
                    assert publishTimeoutSecs>=0;
                }
                if (map.containsKey(handshake_timeout_millis_param)) {
                    handshakeTimeout = Integer.parseInt(map.get(handshake_timeout_millis_param));
                    assert handshakeTimeout>=0;
                }
                if (map.containsKey(retry_count_param)) {
                    retryCount = Integer.parseInt(map.get(retry_count_param));
                    assert retryCount>=0;
                }
                if (map.containsKey(pre_fetch_count_param)) {
                    preFetchCount = Integer.parseInt(map.get(pre_fetch_count_param));
                    assert preFetchCount>=0;
                }
                if (map.containsKey(heartbeat_param)) {
                    heartbeat = Integer.parseInt(map.get(heartbeat_param));
                    assert heartbeat>=0;
                }
                if (map.containsKey(connection_timeout_param)) {
                    connectionTimeout = Integer.parseInt(map.get(connection_timeout_param));
                    assert connectionTimeout>=0;
                }
                if (map.containsKey(shutdown_timeout_param)) {
                    shutdownTimeout = Integer.parseInt(map.get(shutdown_timeout_param));
                    assert shutdownTimeout>=0;
                }
                if (map.containsKey(num_channels_param)) {
                    numChannels = Integer.parseInt(map.get(num_channels_param));
                    assert numChannels >=0;
                }
                if (map.containsKey(frame_max_param)) {
                    frameMax = Integer.parseInt(map.get(frame_max_param));
                    assert frameMax>=0;
                }
            } catch (Exception e) {
                log.warnWithParams("Could not parse settings string. Will fall back to default values.", "error", e.getMessage());
            }

            return this;
        }

        public RabbitSettings build() {
            return new RabbitSettings(appId, publisherConfirms, preFetchCount, heartbeat, connectionTimeout, shutdownTimeout, retryCount, closeTimeout, numChannels, publishTimeoutSecs, frameMax, handshakeTimeout);
        }

        public Builder withPublisherConfirms(boolean publisherConfirms) {
            this.publisherConfirms = publisherConfirms;
            return this;
        }

        public Builder withPreFetchCount(int preFetchCount) {
            this.preFetchCount = preFetchCount;
            return this;
        }

        public Builder withHeartbeatSecs(int heartbeat) {
            this.heartbeat = heartbeat;
            return this;
        }

        public Builder withConnectionTimeoutMillis(int connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        public Builder withShutdownTimeoutMillis(int shutdownTimeout) {
            this.shutdownTimeout = shutdownTimeout;
            return this;
        }

        public Builder withCloseTimeoutMillis(int closeTimeout) {
            this.closeTimeout = closeTimeout;
            return this;
        }

        public Builder withNumChannels(int consumeChannels) {
            this.numChannels = consumeChannels;
            return this;
        }

        public Builder withAppId(String appId) {
            this.appId = appId;
            return this;
        }

        public Builder withPublishTimeoutSecs(int publishTimeoutSecs) {
            this.publishTimeoutSecs = publishTimeoutSecs;
            return this;
        }

        public Builder withRetryCount(int retryCount) {
            this.retryCount = retryCount;
            return this;
        }
    }
}
