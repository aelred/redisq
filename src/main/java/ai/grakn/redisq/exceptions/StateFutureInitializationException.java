package ai.grakn.redisq.exceptions;

public class StateFutureInitializationException extends WaitException {
    public StateFutureInitializationException(String message, InterruptedException e) {
        super(message, e);
    }
}
