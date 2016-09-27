package com.meltwater.rxrabbit;

import com.meltwater.rxrabbit.util.BackoffAlgorithm;
import com.meltwater.rxrabbit.util.FibonacciBackoffAlgorithm;

public class PublisherSettings {

    public static final int RETRY_FOREVER = -1;
    public static final int DEFAULT_RETRY_COUNT = 3;
    public static final boolean DEFAULT_PUBLISHER_CONFIRM = true;
    public static final int DEFAULT_NUM_CHANNELS = 1;
    public static final int DEFAULT_PUBLISH_TIMEOUT_SECS = 30;
    public static final int DEFAULT_CLOSE_TIMEOUT_MILLIS = 10_000;

    private int num_channels            = DEFAULT_NUM_CHANNELS;
    private boolean publisher_confirms  = DEFAULT_PUBLISHER_CONFIRM;
    private int retry_count             = DEFAULT_RETRY_COUNT;
    private long publish_timeout_secs   = DEFAULT_PUBLISH_TIMEOUT_SECS;
    private long close_timeout_millis   = DEFAULT_CLOSE_TIMEOUT_MILLIS;
    private BackoffAlgorithm backoff_algorithm = new FibonacciBackoffAlgorithm();

    public int getNum_channels() {
        return num_channels;
    }

    public boolean isPublisher_confirms() {
        return publisher_confirms;
    }

    public int getRetry_count() {
        return retry_count;
    }

    public long getPublish_timeout_secs() {
        return publish_timeout_secs;
    }

    public long getClose_timeout_millis() {
        return close_timeout_millis;
    }

    public BackoffAlgorithm getBackoff_algorithm() {
        return backoff_algorithm;
    }

    public PublisherSettings withNumChannels(int num_channels) {
        assert num_channels>0;
        this.num_channels = num_channels;
        return this;
    }

    public PublisherSettings withPublisherConfirms(boolean publisher_confirms) {
        this.publisher_confirms = publisher_confirms;
        return this;
    }

    public PublisherSettings withRetryCount(int retry_count) {
        assert retry_count >= RETRY_FOREVER;
        this.retry_count = retry_count;
        return this;
    }

    public PublisherSettings withPublishTimeoutSecs(long publish_timeout_secs) {
        this.publish_timeout_secs = publish_timeout_secs;
        return this;
    }

    public PublisherSettings withCloseTimeoutMillis(long close_timeout_millis) {
        this.close_timeout_millis = close_timeout_millis;
        return this;
    }

    public PublisherSettings withBackoffAlgorithm(BackoffAlgorithm backoff_algorithm) {
        this.backoff_algorithm = backoff_algorithm;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PublisherSettings that = (PublisherSettings) o;

        if (num_channels != that.num_channels) return false;
        if (publisher_confirms != that.publisher_confirms) return false;
        if (retry_count != that.retry_count) return false;
        if (publish_timeout_secs != that.publish_timeout_secs) return false;
        return close_timeout_millis == that.close_timeout_millis;

    }

    @Override
    public int hashCode() {
        int result = num_channels;
        result = 31 * result + (publisher_confirms ? 1 : 0);
        result = 31 * result + retry_count;
        result = 31 * result + (int) (publish_timeout_secs ^ (publish_timeout_secs >>> 32));
        result = 31 * result + (int) (close_timeout_millis ^ (close_timeout_millis >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "{" +
                "num_channels:" + num_channels +
                ", publisher_confirms:" + publisher_confirms +
                ", retry_count:" + retry_count +
                ", publish_timeout_secs:" + publish_timeout_secs +
                ", close_timeout_millis:" + close_timeout_millis +
                '}';
    }

}
