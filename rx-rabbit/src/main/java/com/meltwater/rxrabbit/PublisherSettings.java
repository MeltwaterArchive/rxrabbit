package com.meltwater.rxrabbit;

public class PublisherSettings {
    private int num_channels            =1;
    private boolean publisher_confirms  =true;
    private int retry_count             =3; //0 or negative means infinite
    private long publish_timeout_secs   =30;
    private long close_timeout_millis   =10_000;

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
