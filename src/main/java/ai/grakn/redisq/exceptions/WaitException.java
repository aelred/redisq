package ai.grakn.redisq.exceptions;

public class WaitException extends Exception {
    public WaitException(String message, Exception e) {
        super(message, e);
    }
}
