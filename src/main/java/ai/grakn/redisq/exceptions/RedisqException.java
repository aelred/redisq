package ai.grakn.redisq.exceptions;

public class RedisqException extends RuntimeException {

    public RedisqException(String s, Exception e) {
        super(s, e);
    }

    public RedisqException(String s) {
        super(s);
    }
}
