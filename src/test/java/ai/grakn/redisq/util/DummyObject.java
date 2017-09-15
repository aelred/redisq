package ai.grakn.redisq.util;

import ai.grakn.redisq.Document;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

public class DummyObject implements Document {
    @JsonProperty
    private String id;
    @JsonProperty
    private long dumCoefficient;
    @JsonProperty
    private Map<String, Integer> aMap;

    // Required by Jackson
    public DummyObject() {}

    public DummyObject(String id) {
        this(id, -1, new HashMap<>());
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
