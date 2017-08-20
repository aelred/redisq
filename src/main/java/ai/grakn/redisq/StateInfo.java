package ai.grakn.redisq;

import com.fasterxml.jackson.annotation.JsonProperty;

public class StateInfo {
    @JsonProperty
    private State state;
    @JsonProperty
    private long lastProcessed;

    public StateInfo() {
    }

    public StateInfo(State state, long lastProcessed) {
        this.state = state;
        this.lastProcessed = lastProcessed;
    }

    public State getState() {
        return state;
    }

    public long getLastProcessed() {
        return lastProcessed;
    }

    @Override
    public String toString() {
        return "StateInfo{" +
                "state=" + state +
                ", lastProcessed=" + lastProcessed +
                '}';
    }
}
