package ai.grakn.redisq.consumer;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TimedWrap<T>  {
    @JsonProperty
    private T element;
    @JsonProperty
    private long timestampMs;

    public TimedWrap() {}

    public TimedWrap(T element, long timestampMs) {
        this.element = element;
        this.timestampMs = timestampMs;
    }

    public T getElement() {
        return element;
    }

    public long getTimestampMs() {
        return timestampMs;
    }
}
