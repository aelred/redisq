package ai.grakn.redisq.exceptions;

public class SubscriptionInterruptedException extends RuntimeException {
    public SubscriptionInterruptedException(String message, Exception e) {
        super(message, e);
    }
}
