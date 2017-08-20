package ai.grakn.redisq.subscription;

import ai.grakn.redisq.Idable;
import ai.grakn.redisq.Redisq;
import ai.grakn.redisq.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Pool;

import java.util.function.Consumer;

import static ai.grakn.redisq.State.DONE;
import static ai.grakn.redisq.State.FAILED;

public class RedisqSubscription<T extends Idable> implements Subscription<T>{
    private static final Logger LOG = LoggerFactory.getLogger(RedisqSubscription.class);

    private Consumer<T> consumer;
    private Pool<Jedis> jedisPool;
    private Redisq<T> tRedisq;

    public RedisqSubscription(Consumer<T> consumer, Pool<Jedis> jedisPool, Redisq<T> tRedisq) {
        this.consumer = consumer;
        this.jedisPool = jedisPool;
        this.tRedisq = tRedisq;
    }

    @Override
    public void process(T element) {
        try {
            consumer
                    .andThen(e -> updateState(e, DONE))
                    .accept(element);
        } catch (Exception e) {
            updateState(element, FAILED);
        }
    }

    private void updateState(T element, State state) {
         try(Jedis jedis = jedisPool.getResource()) {
            String id = element.getIdAsString();
            tRedisq.setState(jedis, System.currentTimeMillis(), id, state);
            jedis.del(tRedisq.getNames().lockKeyFromId(id));
        } catch (JedisConnectionException e) {
            LOG.error("Pool is full  or terminated. Active: {}, idle: {}", jedisPool.getNumActive(), jedisPool.getNumIdle());
            throw e;
        }
        LOG.debug("Status {} set as {}", element.getIdAsString(), state);
    }

}
