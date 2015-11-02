package com.meltwater.rxrabbit;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.meltwater.rxrabbit.util.Logger;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This class contains rabbitmq connection settings. Some settings are part of the official AMQP URI spec v 0-9-1,
 * others are domain specific to this library and is used to configure the {@link ConnectionFactory} and {@link PublisherFactory}.
 *
 * Some settings are only relevant for consumers, some are only relevant for producers and some are used by
 * both consumers and publishers.
 *
 * This object can be built pragmatically by using the various {@link RabbitSettings.Builder}
 * withXX methods or by supplying a JSON formatted String to the {@link RabbitSettings.Builder#setDefaults(String)} method.
 *
 * The available JSON parameter names are included as String constants but they also directly corresponds to the names of the fields in this class.
 *
 * The {@link #toString()} method shows the JSON representation of this class and can be used as input to the
 * {@link RabbitSettings.Builder#setDefaults(String)} method. method.
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
    public static final int DEFAULT_RETRY_COUNT = 0; //= forever
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

        private boolean publisherConfirms;
        private int closeTimeout;
        private int preFetchCount;
        private int heartbeat;
        private int connectionTimeout;
        private int shutdownTimeout;
        private int numChannels;
        private int frameMax;
        private int handshakeTimeout;
        private int retryCount;
        private int publishTimeoutSecs;

        private String appId;


        public Builder(){
            setDefaults(new HashMap<>());
        }

        public Builder(String settingsJSONString) {
            mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
            mapper.configure(JsonParser.Feature.STRICT_DUPLICATE_DETECTION, true);
            if (!settingsJSONString.startsWith("{")){
                settingsJSONString = "{"+settingsJSONString+"}";
            }
            try {
                setDefaults(mapper.readValue(settingsJSONString, Map.class));
            }catch (Exception e){
                log.errorWithParams("Could not parse settings string.", e);
                throw new IllegalArgumentException(e);
            }
        }

        /**
         * Fills in the values supplied in the JSON formatted string. The available parameter values
         *
         * @return this builder with all the values provided in the JSON filled in
         */
        private void setDefaults(Map<String,Object> map){
            publisherConfirms = (boolean) map.getOrDefault(publisher_confirms_param, DEFAULT_PUBLISHER_CONFIRMS);
            closeTimeout = (int) map.getOrDefault(close_timeout_millis_param, DEFAULT_CLOSE_TIMEOUT_MILLIS);
            publishTimeoutSecs = (int) map.getOrDefault(publish_timeout_secs_param, DEFAULT_PUBLISH_TIMEOUT_SECS);
            handshakeTimeout = (int) map.getOrDefault(handshake_timeout_millis_param, ConnectionFactory.DEFAULT_HANDSHAKE_TIMEOUT);
            retryCount = (int) map.getOrDefault(retry_count_param, DEFAULT_RETRY_COUNT);
            preFetchCount = (int) map.getOrDefault(pre_fetch_count_param, DEFAULT_PRE_FETCH);
            heartbeat = (int) map.getOrDefault(heartbeat_param, DEFAULT_HEARTBEAT);
            connectionTimeout = (int) map.getOrDefault(connection_timeout_param, DEFAULT_CONNECTION_TIMEOUT);
            shutdownTimeout = (int) map.getOrDefault(shutdown_timeout_param, ConnectionFactory.DEFAULT_SHUTDOWN_TIMEOUT);
            numChannels = (int) map.getOrDefault(num_channels_param, DEFAULT_NUM_CHANNELS);
            frameMax = (int) map.getOrDefault(frame_max_param, ConnectionFactory.DEFAULT_FRAME_MAX);
            appId = "unknown";
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
