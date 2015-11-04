package com.meltwater.rxrabbit;

/**
 * This class contains rabbitmq consumer settings used by the {@link ConsumerFactory}
 */
public class ConsumerSettings {

    public static final int DEFAULT_NUM_CHANNELS = 1;
    public static final int DEFAULT_PREFETCH_COUNT = 256;
    public static final int RETRY_FOREVER=-1;
    public static final int DEFAULT_RETRY_COUNT = RETRY_FOREVER;
    public static final int DEFAULT_CLOSE_TIMEOUT_MILLIS = 10_000;

    private int num_channels            = DEFAULT_NUM_CHANNELS;
    private int pre_fetch_count         = DEFAULT_PREFETCH_COUNT;
    private int retry_count             = DEFAULT_RETRY_COUNT; //0 or lower means forever
    private long close_timeout_millis   = DEFAULT_CLOSE_TIMEOUT_MILLIS; //0 means forever
    private String consumer_tag_prefix  = "";

    public int getNum_channels() {
        return num_channels;
    }

    public int getPre_fetch_count() {
        return pre_fetch_count;
    }

    public String getConsumer_tag_prefix() {
        return consumer_tag_prefix;
    }

    public int getRetry_count() {
        return retry_count;
    }

    public long getClose_timeout_millis() {
        return close_timeout_millis;
    }

    public ConsumerSettings withNumChannels(int num_channels) {
        assert num_channels>0;
        this.num_channels = num_channels;
        return this;
    }

    public ConsumerSettings withPreFetchCount(int pre_fetch_count) {
        assert pre_fetch_count>0;
        this.pre_fetch_count = pre_fetch_count;
        return this;
    }

    public ConsumerSettings withConsumerTagPrefix(String consumer_tag_prefix) {
        this.consumer_tag_prefix = consumer_tag_prefix;
        return this;
    }

    public ConsumerSettings withRetryCount(int retry_count) {
        assert retry_count >= RETRY_FOREVER;
        this.retry_count = retry_count;
        return this;
    }

    public ConsumerSettings withCloseTimeoutMillis(long close_timeout_millis) {
        assert close_timeout_millis>=0;
        this.close_timeout_millis = close_timeout_millis;
        return this;
    }

    @Override
    public String toString() {
        return "{" +
                "num_channels:" + num_channels +
                ", pre_fetch_count:" + pre_fetch_count +
                ", retry_count:" + retry_count +
                ", close_timeout_millis:" + close_timeout_millis +
                ", consumer_tag_prefix:'" + consumer_tag_prefix + "'" +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConsumerSettings that = (ConsumerSettings) o;
        if (num_channels != that.num_channels) return false;
        if (pre_fetch_count != that.pre_fetch_count) return false;
        if (retry_count != that.retry_count) return false;
        if (close_timeout_millis != that.close_timeout_millis) return false;
        return !(consumer_tag_prefix != null ? !consumer_tag_prefix.equals(that.consumer_tag_prefix) : that.consumer_tag_prefix != null);

    }

    @Override
    public int hashCode() {
        int result = num_channels;
        result = 31 * result + pre_fetch_count;
        result = 31 * result + retry_count;
        result = 31 * result + (int) (close_timeout_millis ^ (close_timeout_millis >>> 32));
        result = 31 * result + (consumer_tag_prefix != null ? consumer_tag_prefix.hashCode() : 0);
        return result;
    }
}
