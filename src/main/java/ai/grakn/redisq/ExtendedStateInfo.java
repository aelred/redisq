package ai.grakn.redisq;

import com.fasterxml.jackson.annotation.JsonProperty;
import static java.awt.SystemColor.info;

public class ExtendedStateInfo {
    @JsonProperty
    private String id;
    @JsonProperty
    private StateInfo stateInfo;

    public ExtendedStateInfo() {}

    public ExtendedStateInfo(String id, StateInfo state) {
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
