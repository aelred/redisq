package ai.grakn.redisq;

import ai.grakn.redisq.util.Names;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.SECONDS;


public class RedisqBuilder<T extends Document> {

    private String name = "redisq_" + Names.getRandomString();
    private Duration timeout = Duration.of(5, SECONDS);
    private Duration ttlStateInfo = Duration.of(1, DAYS);
    private Duration lockTime = Duration.of(5, MINUTES);
    private Duration discardTime = Duration.of(1, HOURS);
    private ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(4, new ThreadFactoryBuilder().setNameFormat("redisq-executor-%d").build());
    private long threadDelay = 0L;
    private Consumer<T> consumer;
    private Pool<Jedis> jedisPool;
    private Class<T> documentClass;
    private MetricRegistry metricRegistry = new MetricRegistry();

    public RedisqBuilder<T> setName(String name) {
        this.name = name;
        return this;
    }

    public RedisqBuilder<T> setDelay(long delay){
        this.threadDelay = delay;
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

    public RedisqBuilder<T> setThreadPoolSize(int size) {
        this.threadPool = Executors.newScheduledThreadPool(size, new ThreadFactoryBuilder().setNameFormat("redisq-executor-%d").build());
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
        return new Redisq<>(name, timeout, ttlStateInfo, lockTime, discardTime, consumer,
                documentClass, jedisPool, threadPool, threadDelay, metricRegistry);
    }
}