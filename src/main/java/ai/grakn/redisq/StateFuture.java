package ai.grakn.redisq;

import ai.grakn.redisq.exceptions.DeserializationException;
import ai.grakn.redisq.exceptions.StateFutureInitializationException;
import ai.grakn.redisq.exceptions.SubscriptionInterruptedException;
import ai.grakn.redisq.util.Names;
import com.codahale.metrics.MetricRegistry;
import static com.codahale.metrics.MetricRegistry.name;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Pool;

import java.util.concurrent.*;

public class StateFuture implements Future<Void> {
    private static final Logger LOG = LoggerFactory.getLogger(StateFuture.class);
    private final JedisPubSub sub;
    private final CompletableFuture<Void> subscription;
    private final Names names;
    private final Timer subscribeWait;

    private Set<State> targetState;
    private String id;
    private final Pool<Jedis> jedisPool;
    private final CountDownLatch latch = new CountDownLatch(1);


    StateFuture(Set<State> targetState, String id, Pool<Jedis> jedisPool,
            long subscriptionWaitTimeout, TimeUnit subscriptionWaitUnit,
            MetricRegistry metricRegistry) throws StateFutureInitializationException {
        this.targetState = targetState;
        this.id = id;
        this.jedisPool = jedisPool;
        this.names = new Names();
        this.sub = new JedisPubSub() {

            @Override
            public void onSubscribe(String channel, int subscribedChannels) {
                latch.countDown();
            }

            @Override
            public void onUnsubscribe(String channel, int subscribedChannels) {
                latch.countDown();
            }

            @Override
            public void onMessage(String channel, String message) {
                try {
                    StateInfo s = Redisq.stateMapper.deserialize(message);
                    if (targetState.contains(s.getState())) {
                        latch.countDown();
                        LOG.debug("Received expected state, completing {}", channel);
                        unsubscribe(channel);
                    }
                } catch (DeserializationException e) {
                    LOG.error("Could not deserialise state {}", id, e);
                }
            }
        };
        subscribeWait = metricRegistry.timer(name(StateFuture.class, "subscribe_wait"));
        Timer initWaitTimer = metricRegistry.timer(name(StateFuture.class, "init_wait"));

        try (Context ignored = initWaitTimer.time()) {
            this.subscription = subscribe(subscriptionWaitTimeout, subscriptionWaitUnit);
        } catch (InterruptedException e) {
            throw new StateFutureInitializationException("Could not initialise StateFuture for id " + id, e);
        }
    }

    private CompletableFuture<Void> subscribe(long timeout, TimeUnit unit) throws InterruptedException {
        CompletableFuture<Void> f = CompletableFuture.runAsync(() -> {
            try{
                try (Jedis jedis = jedisPool.getResource()) {
                    String state = jedis.get(names.stateKeyFromId(id));
                    if (state != null) {
                        try {
                            if (targetState.contains(Redisq.stateMapper.deserialize(state).getState())) {
                                LOG.debug("Unsubscribed because status was already as expected {}", id);
                                return;
                            }
                        } catch (DeserializationException e) {
                            LOG.error("Could not deserialize state for {}", id, e);
                        }
                    }
                    String stateChannel = new Names().stateChannelKeyFromId(id);
                    LOG.debug("Waiting for changes to {}", stateChannel);
                    try (Context ignored = subscribeWait.time()) {
                        jedis.subscribe(sub, stateChannel);
                    }
                } finally{
                    latch.countDown();
                }
            } catch (JedisConnectionException e) {
                if (jedisPool.isClosed()) {
                    throw new SubscriptionInterruptedException("Subscription interrupted because the Jedis connection was closed for id " + id, e);
                } else {
                    LOG.error("Could not connect to Redis while subscribing to {}", id, e);
                    throw e;
                }
            }
        });
        if (!f.isCompletedExceptionally() && !f.isCancelled()) {
            latch.await(timeout, unit);
            LOG.debug("Subscribed successfully to {}", id);
        } else {
            LOG.error("QueueConsumer ended before expected");
        }
        return f;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StateFuture that = (StateFuture) o;
        return targetState == that.targetState && (id != null ? id.equals(that.id) : that.id == null);
    }

    @Override
    public int hashCode() {
        int result = targetState != null ? targetState.hashCode() : 0;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        return result;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        sub.unsubscribe(names.stateChannelKeyFromId(id));
        return subscription.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return subscription.isCancelled();
    }

    @Override
    public boolean isDone() {
        return subscription.isDone();
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        return subscription.get();
    }

    @Override
    public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return subscription.get(timeout, unit);
    }
}
