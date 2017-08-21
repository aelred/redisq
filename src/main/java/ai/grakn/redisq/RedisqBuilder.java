package ai.grakn.redisq;

import ai.grakn.redisq.util.Names;
import com.codahale.metrics.MetricRegistry;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static java.time.temporal.ChronoUnit.*;


public class RedisqBuilder<T extends Document> {

    private String name = "redisq_" + Names.getRandomString();
    private Duration timeout = Duration.of(5, SECONDS);
    private Duration ttlStateInfo = Duration.of(1, DAYS);
    private Duration lockTime = Duration.of(5, MINUTES);
    private Duration discardTime = Duration.of(1, HOURS);
    private ExecutorService threadPool = Executors.newFixedThreadPool(4 );
    private Consumer<T> consumer;
    private Pool<Jedis> jedisPool;
    private Class<T> documentClass;
    private MetricRegistry metricRegistry = new MetricRegistry();

    public RedisqBuilder<T> setName(String name) {
        this.name = name;
        return this;
    }

    public RedisqBuilder<T> setTimeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public RedisqBuilder<T> setTtlStateInfo(Duration ttlStateInfo) {
        this.ttlStateInfo = ttlStateInfo;
        return this;
    }

    public RedisqBuilder<T> setLockTime(Duration lockTime) {
        this.lockTime = lockTime;
        return this;
    }

    public RedisqBuilder<T> setDiscardTime(Duration discardTime) {
        this.discardTime = discardTime;
        return this;
    }

    public RedisqBuilder<T> setConsumer(Consumer<T> consumer) {
        this.consumer = consumer;
        return this;
    }

    public RedisqBuilder<T> setJedisPool(Pool<Jedis> jedisPool) {
        this.jedisPool = jedisPool;
        return this;
    }

    public RedisqBuilder<T> setThreadPool(ExecutorService threadPool) {
        this.threadPool = threadPool;
        return this;
    }

    public RedisqBuilder<T> setDocumentClass(Class<T> documentClass) {
        this.documentClass = documentClass;
        return this;
    }

    public RedisqBuilder<T> setMetricRegistry(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        return this;
    }

    public Redisq<T> createRedisq() {
        return new Redisq<>(name, timeout, ttlStateInfo, lockTime, discardTime, consumer, documentClass, jedisPool, threadPool, metricRegistry);
    }
}