package com.meltwater.rxrabbit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.meltwater.rxrabbit.util.Logger;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Map;

/**
 * This class contains rabbitmq connection settings. Some settings are part of the official AMQP URI spec v 0-9-1,
 * others are domain specific to this library and is used to configure different parts of the library behaviors.
 *
 * Some settings are only relevant for consumers, some are only relevant for producers and some are used by
 * both consumers and publishers.
 *
 * This object can be built pragmatically by using the various {@link RabbitSettings.Builder}
 * withX methods or by supplying a JSON formatted String to the {@link RabbitSettings.Builder#withSettingsJSON(String)} method.
 *
 * The available JSON parameter names are included as String constants in this class as well as being the name of the fields in this class.
 * The {@link #toString()} method also shows the JSON representation of this class and can be used as input to the builder withSettingsJSON method.
 *
 * @see {@link ConsumerFactory}
 * @see {@link PublisherFactory}
 * @see {https://www.rabbitmq.com/uri-query-parameters.html}
 */
//TODO we probably should have a list of 'connection' urls + A Map<String,Object> for the other settings - this way we do not have to duplicate all the url params
public class RabbitSettings {

    private final static Logger log = new Logger(PublisherFactory.class);
    private final static ObjectMapper mapper = new ObjectMapper();

    public final static String heartbeat_param                 = "heartbeat";
    public final static String pre_fetch_count_param           = "pre_fetch_count";
    public final static String connection_timeout_param        = "connection_timeout";
    public final static String shutdown_timeout_millis_param   = "shutdown_timeout_millis";
    public final static String consume_channels_param          = "consume_channels";
    public final static String publish_channels_param          = "publish_channels";
    public final static String frame_max_param                 = "frame_max";
    public final static String close_timeout_millis_param      = "close_timeout_millis";
    public final static String publisher_confirms_param        = "publisher_confirms";

    //NOTE these params are here for reference, but they are not yet implemented
    public final static String channel_max_param = "channel_max"; //NOTE we use consume/publish_channels instead
    public final static String cacertfile_param = "cacertfile"; //NOTE all ssl settings are implemented as soon as someone needs them
    public final static String certfile_param = "certfile";
    public final static String keyfile_param = "keyfile";
    public final static String verify_param = "verify";
    public final static String fail_if_no_peer_cert_param = "fail_if_no_peer_cert";

    public static final int DEFAULT_CONSUME_CHANNELS        = 1;
    public static final int DEFAULT_PUBLISH_CHANNELS        = 1;
    public static final int DEFAULT_PRE_FETCH               = 0;
    public static final int DEFAULT_CLOSE_TIMEOUT_MILLIS    = 30_000;
    public static final boolean DEFAULT_PUBLISHER_CONFIRMS  = false;

    public final int heartbeat;
    public final int pre_fetch_count;
    public final int connection_timeout;
    public final int shutdown_timeout_millis;
    public final int consume_channels;
    public final int publish_channels;
    public final int frame_max;
    public final int close_timeout_millis; // 0 or negative values means forever..
    public final boolean publisher_confirms;

    public final String appId;

    private RabbitSettings(boolean publisherConfirms, int preFetchCount, int heartbeat, int connectionTimeout, int shutdownTimeout, String appId, int closeTimeout, int consume_channels, int publish_channels, int frame_max) {
        this.publisher_confirms = publisherConfirms;
        this.pre_fetch_count = preFetchCount;
        this.heartbeat = heartbeat;
        this.connection_timeout = connectionTimeout;
        this.shutdown_timeout_millis = shutdownTimeout;
        this.appId = appId;
        this.close_timeout_millis = closeTimeout;
        this.consume_channels = consume_channels;
        this.publish_channels = publish_channels;
        this.frame_max = frame_max;
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
        private int heartbeat               = ConnectionFactory.DEFAULT_HEARTBEAT;
        private int connectionTimeout       = 10_000;
        private int shutdownTimeout         = ConnectionFactory.DEFAULT_SHUTDOWN_TIMEOUT;
        private int consumeChannels         = DEFAULT_CONSUME_CHANNELS;
        private int publishChannels         = DEFAULT_PUBLISH_CHANNELS;
        private int frameMax                = ConnectionFactory.DEFAULT_FRAME_MAX;

        private String appId                = "unknown"; //TODO calculate the appID from the host+component etc!!

        /**
         * Fills in the values supplied in the JSON formatted string. The available parameter values
         *
         * @param settingsJSONString
         * @return
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
                if (map.containsKey(shutdown_timeout_millis_param)) {
                    shutdownTimeout = Integer.parseInt(map.get(shutdown_timeout_millis_param));
                    assert shutdownTimeout>=0;
                }
                if (map.containsKey(consume_channels_param)) {
                    consumeChannels = Integer.parseInt(map.get(consume_channels_param));
                    assert consumeChannels >=0;
                }
                if (map.containsKey(publish_channels_param)) {
                    publishChannels = Integer.parseInt(map.get(publish_channels_param));
                    assert publishChannels >=0;
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

        public Builder withConsumeChannels(int consumeChannels) {
            this.consumeChannels = consumeChannels;
            return this;
        }

        public Builder withPublishChannels(int publishChannels) {
            this.publishChannels = publishChannels;
            return this;
        }

        public Builder withAppId(String appId) {
            this.appId = appId;
            return this;
        }

        public RabbitSettings build() {
            return new RabbitSettings(publisherConfirms, preFetchCount, heartbeat, connectionTimeout, shutdownTimeout, appId, closeTimeout, consumeChannels, publishChannels, frameMax);
        }
    }
}
