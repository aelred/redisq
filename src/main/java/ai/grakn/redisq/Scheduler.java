package ai.grakn.redisq;

import ai.grakn.redisq.consumer.Mapper;
import ai.grakn.redisq.consumer.QueueConsumer;
import ai.grakn.redisq.exceptions.DeserializationException;
import ai.grakn.redisq.exceptions.RedisqException;
import ai.grakn.redisq.exceptions.SerializationException;
import ai.grakn.redisq.util.Names;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Scheduler for the consumption of {@link Document}s by a {@link QueueConsumer}.
 *
 * @param <T> the type of {@link Document} that is being consumed
 */
class Scheduler<T extends Document> {

    private static final Names NAMES = new Names();

    private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);

    private static final Retryer<Integer> CLOSE_RETRIER = RetryerBuilder.<Integer>newBuilder()
            .retryIfResult(r -> !(r != null && r == 0))
            .withWaitStrategy(WaitStrategies.fixedWait(100, TimeUnit.MILLISECONDS))
            .withStopStrategy(StopStrategies.stopAfterDelay(10, TimeUnit.SECONDS))
            .build();

    private final long threadDelay;
    private final ScheduledExecutorService threadPool;
    private final QueueConsumer<T> subscription;
    private final Pool<Jedis> jedisPool;
    private final Mapper<T> mapper;
    private final AtomicInteger runningThreads = new AtomicInteger(0);

    private Scheduler(long threadDelay, ScheduledExecutorService threadPool, QueueConsumer<T> subscription,
                      Pool<Jedis> jedisPool, Mapper<T> mapper) {
        this.threadDelay = threadDelay;
        this.threadPool = threadPool;
        this.subscription = subscription;
        this.jedisPool = jedisPool;
        this.mapper = mapper;
    }

    static <T extends Document> Scheduler<T> of(
            long threadDelay, ScheduledExecutorService threadPool, QueueConsumer<T> subscription,
            Pool<Jedis> jedisPool, Mapper<T> mapper
    ) {
        return new Scheduler<>(threadDelay, threadPool, subscription, jedisPool, mapper);
    }

    void execute(T document) {
        String serialized;
        try {
            serialized = mapper.serialize(document);
        } catch (SerializationException e) {
            throw new RedisqException("Could not serialize element " + document.getIdAsString(), e);
        }

        String key = NAMES.delayedKeyFromId(document.getIdAsString());

        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(key, serialized);
        }

        threadPool.schedule(() -> {
            runningThreads.incrementAndGet();
            try {
                processFromRedis(key);
            } finally {
                runningThreads.decrementAndGet();
            }
        }, threadDelay, TimeUnit.SECONDS);
    }

    private void processFromRedis(String key) {
        String serialized;
        try (Jedis jedis = jedisPool.getResource()) {
            serialized = jedis.get(key);
        }

        T document;
        try {
            document = mapper.deserialize(serialized);
        } catch (DeserializationException e) {
            throw new RedisqException("Could not deserialize element " + serialized, e);
        }

        subscription.process(document);
    }

    void close() throws ExecutionException, InterruptedException {
        try {
            CLOSE_RETRIER.call(runningThreads::get);
        } catch (RetryException e) {
            LOG.warn("Closing while some threads are still running");
        }
        threadPool.shutdown();
        threadPool.awaitTermination(1, TimeUnit.MINUTES);
    }
}