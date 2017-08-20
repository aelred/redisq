package ai.grakn.redisq;

import ai.grakn.redisq.subscription.Subscription;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

interface Queue<T> {
    void push(T element);
    void pushAndWait(T element, long timeout, TimeUnit unit) throws InterruptedException;
    void startSubscription();
    void close() throws InterruptedException;
    String getName();
    Optional<StateInfo> getState(String id);
    Subscription<T> getSubscription();
}
