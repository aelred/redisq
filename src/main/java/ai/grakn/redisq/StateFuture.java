package ai.grakn.redisq;

import ai.grakn.redisq.exceptions.DeserializationException;
import ai.grakn.redisq.exceptions.StateFutureInitializationException;
import ai.grakn.redisq.util.Names;
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

    private State targetState;
    private String id;
    private final Pool<Jedis> jedisPool;
    private final CountDownLatch latch = new CountDownLatch(1);


    public StateFuture(State targetState, String id, Pool<Jedis> jedisPool, long subscriptionWaitTimeout, TimeUnit subscriptionWaitUnit) throws StateFutureInitializationException {
        this.targetState = targetState;
        this.id = id;
        this.jedisPool = jedisPool;
        this.names = new Names();
        this.sub = new JedisPubSub() {

            @Override
            public void onSubscribe(String channel, int subscribedChannels) {
                latch.countDown();
//                String state;
//                try (Jedis jedis = jedisPool.getResource()) {
//                    state = jedis.get(names.stateKeyFromId(id));
//                }
//                if (state != null) {
//                    try {
//                        if (Redisq.stateMapper.deserialize(state).getState().equals(targetState)) {
//                            unsubscribe();
//                            LOG.debug("Unsubscribed because status was already as expected {}", id);
//                        }
//                    } catch (DeserializationException e) {
//                        LOG.error("Could not deserialize state for {}", id, e);
//                    }
//                }
            }

            @Override
            public void onUnsubscribe(String channel, int subscribedChannels) {
                latch.countDown();
            }

            @Override
            public void onMessage(String channel, String message) {
                try {
                    StateInfo s = Redisq.stateMapper.deserialize(message);
                    if (targetState.equals(s.getState())) {
                        latch.countDown();
                        LOG.debug("Received expected state, completing {}", channel);
                        unsubscribe();
                    }
                } catch (DeserializationException e) {
                    LOG.error("Could not deserialise state {}", id, e);
                }
            }
        };
        try {
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
                            if (Redisq.stateMapper.deserialize(state).getState()
                                    .equals(targetState)) {
                                LOG.debug("Unsubscribed because status was already as expected {}", id);
                                return;
                            }
                        } catch (DeserializationException e) {
                            LOG.error("Could not deserialize state for {}", id, e);
                        }
                    }
                    String stateChannel = new Names().stateChannelKeyFromId(id);
                    LOG.debug("Waiting for changes to {}", stateChannel);
                    jedis.subscribe(sub, stateChannel);
                } finally{
                    latch.countDown();
                }
            } catch (JedisConnectionException e) {
                LOG.error("Could not connect to Redis while subscribing to {}. Jedis idle {}, active {}", id, jedisPool.getNumIdle(), jedisPool.getNumActive(), e);
                throw e;
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
