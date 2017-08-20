package ai.grakn.redisq.util;

import ai.grakn.redisq.Idable;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class DummyObject implements Idable {
    @JsonProperty
    private String id;
    @JsonProperty
    private long dumCoefficient;
    @JsonProperty
    private Map<String, Integer> aMap;

    public DummyObject() {
    }

    public DummyObject(String id, long dumCoefficient, Map<String, Integer> aMap) {
        this.id = id;
        this.dumCoefficient = dumCoefficient;
        this.aMap = aMap;
    }

    @Override
    @JsonIgnore
    public String getIdAsString() {
        return id;
    }
}
