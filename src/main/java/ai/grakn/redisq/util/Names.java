package ai.grakn.redisq.util;

import java.math.BigInteger;
import java.util.Random;

public class Names {

    private static final Random random = new Random();

    private final static String PREFIX = "redisq:";

    private final static String QUEUE = "queue:";
    private final static String INFLIGHT = "inflight:";

    private final static String CONTENT = "content:";
    private final static String STATE = "state:";
    private final static String STATE_CHANNEL = "state:channel:";
    private final static String LOCK = "lock:";


    public String queueNameFor(String name) {
        return PREFIX + QUEUE + name;
    }

    public String inFlightQueueNameFor(String name) {
        return PREFIX + INFLIGHT + name;
    }

    public String stateKeyFromId(String idAsString) {
        return PREFIX + STATE + encoded(idAsString);
    }

    public String stateChannelKeyFromId(String idAsString) {
        return PREFIX + STATE_CHANNEL + encoded(idAsString);
    }

    public String contentKeyFromId(String idAsString) {
        return PREFIX + CONTENT + encoded(idAsString);
    }

    public String lockKeyFromId(String idAsString) {
        return PREFIX + LOCK + encoded(idAsString);
    }

    public static String getRandomString() {
        return new BigInteger(130, random).toString(32);
    }

    private String encoded(String idAsString) {
        return idAsString.replaceAll(" ", "_");
    }
}
