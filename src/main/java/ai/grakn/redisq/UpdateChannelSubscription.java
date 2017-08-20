package ai.grakn.redisq;

import ai.grakn.redisq.exceptions.DeserializationException;
import ai.grakn.redisq.util.Names;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.util.Pool;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class UpdateChannelSubscription<T extends Idable> {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateChannelSubscription.class);
    private final JedisPubSub sub;

    State targetState;
    String id;
    private final Pool<Jedis> jedisPool;
    private final CountDownLatch latch = new CountDownLatch(1);


    public UpdateChannelSubscription(State targetState, String id, Pool<Jedis> jedisPool) {
        this.targetState = targetState;
        this.id = id;
        this.jedisPool = jedisPool;
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
                    if (targetState.equals(s.getState())) {
                        latch.countDown();
                        unsubscribe();
                    }
                } catch (DeserializationException e) {
                    LOG.error("Could not deserialise state {}", id, e);
                }
            }
        };
    }

    public CompletableFuture<Void> subscribe(long timeout, TimeUnit unit) throws InterruptedException {
        CompletableFuture<Void> f = CompletableFuture.runAsync(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.subscribe(sub, new Names().stateChannelKeyFromId(id));
            } catch (Exception e) {
                LOG.error("Error while subscribing {}", id, e);
            } finally{
                latch.countDown();
            }
        });
        if (!f.isCompletedExceptionally() && !f.isCancelled()) {
            latch.await(timeout, unit);
        } else {
            LOG.error("Subscription ended before expected");
        }
        return f;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UpdateChannelSubscription<?> that = (UpdateChannelSubscription<?>) o;
        return targetState == that.targetState && (id != null ? id.equals(that.id) : that.id == null);
    }

    @Override
    public int hashCode() {
        int result = targetState != null ? targetState.hashCode() : 0;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        return result;
    }
}
