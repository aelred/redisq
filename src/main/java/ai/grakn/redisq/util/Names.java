package ai.grakn.redisq.util;

import java.math.BigInteger;
import java.util.Random;

public class Names {
    public static final String STOP = "stop";

    private static final Random random = new Random();

    private static final int NUM_BITS = 130;
    private static final int RADIX = 32;

    private static final String PREFIX = "redisq:";

    private static final String QUEUE = "queue:";
    private static final String INFLIGHT = "inflight:";

    private static final String CONTENT = "content:";
    private static final String STATE = "state:";
    private static final String DELAYED = "delayed:";
    private static final String STATE_CHANNEL = "state:channel:";
    private static final String LOCK = "lock:";


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

    public String delayedKeyFromId(String idAsString) {
        return PREFIX + DELAYED + idAsString;
    }

    public static String getRandomString() {
        return new BigInteger(NUM_BITS, random).toString(RADIX);
    }

    private String encoded(String idAsString) {
        return idAsString.replaceAll(" ", "_");
    }
}
