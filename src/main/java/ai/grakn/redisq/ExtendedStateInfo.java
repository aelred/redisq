package ai.grakn.redisq;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ExtendedStateInfo {
    @JsonProperty
    private String id;
    @JsonProperty
    private StateInfo stateInfo;

    // Required by Jackson
    public ExtendedStateInfo() {}

    public ExtendedStateInfo(String id, StateInfo stateInfo) {
        this.id = id;
        this.stateInfo = stateInfo;
    }

    public String getId() {
        return id;
    }

    public StateInfo getStateInfo() {
        return stateInfo;
    }

    @Override
    public String toString() {
        return "ExtendedStateInfo{" +
                "id='" + id + '\'' +
                ", stateInfo=" + stateInfo +
                '}';
    }
}
