package ai.grakn.redisq;

import com.fasterxml.jackson.annotation.JsonProperty;

public class StateInfo {
    @JsonProperty
    private State state;
    @JsonProperty
    private long lastProcessed;
    @JsonProperty
    private String info = null;

    public StateInfo() {
    }

    public StateInfo(State state, long lastProcessed, String info) {
        this.state = state;
        this.lastProcessed = lastProcessed;
        this.info = info;
    }

    public State getState() {
        return state;
    }

    public long getLastProcessed() {
        return lastProcessed;
    }

    public String getInfo() {
        return info;
    }

    @Override
    public String toString() {
        return "StateInfo{" +
                "state=" + state +
                ", lastProcessed=" + lastProcessed +
                ", info='" + info + '\'' +
                '}';
    }
}
