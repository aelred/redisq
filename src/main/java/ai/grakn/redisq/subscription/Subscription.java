package ai.grakn.redisq.subscription;

public interface Subscription<T> {
    void process(T element);
}
