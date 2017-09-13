package ai.grakn.redisq;

import ai.grakn.redisq.consumer.QueueConsumer;
import ai.grakn.redisq.exceptions.DeserializationException;
import ai.grakn.redisq.exceptions.SerializationException;
import ai.grakn.redisq.exceptions.StateFutureInitializationException;
import ai.grakn.redisq.exceptions.WaitException;
import ai.grakn.redisq.consumer.Mapper;
import ai.grakn.redisq.consumer.RedisqConsumer;
import ai.grakn.redisq.consumer.TimedWrap;
import ai.grakn.redisq.util.Names;
import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricRegistry.MetricSupplier;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rholder.retry.*;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Pool;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static ai.grakn.redisq.State.*;
import static com.codahale.metrics.MetricRegistry.name;

public class Redisq<T extends Document> implements Queue<T> {

    static final Mapper<StateInfo> stateMapper = new Mapper<>(StateInfo.class);

    private static final Logger LOG = LoggerFactory.getLogger(Redisq.class);
    private static final Retryer<Integer> CLOSE_RETRIER = RetryerBuilder.<Integer>newBuilder()
            .retryIfResult(r -> !(r != null && r == 0))
            .withWaitStrategy(WaitStrategies.fixedWait(100, TimeUnit.MILLISECONDS))
            .withStopStrategy(StopStrategies.stopAfterDelay(10, TimeUnit.SECONDS))
            .build();

    private final String queueName;
    private final String inFlightQueueName;
    private final String name;
    private final Duration timeout;
    private final Mapper<TimedWrap<T>> mapper;
    private final Names names;
    private final int lockTime;
    private final Pool<Jedis> jedisPool;
    private int ttlStateInfo;
    private final ExecutorService threadPool;
    private final AtomicBoolean working = new AtomicBoolean(false);
    private final AtomicInteger runningThreads = new AtomicInteger(0);
    private Duration discardTime;
    private final MetricRegistry metricRegistry;
    private QueueConsumer<T> subscription;
    private Future<?> mainLoop;
    private Future<?> inFlightLoop;

    private final Timer restoreBlockedTimer;
    private final Timer idleTimer;
    private final Timer pushTimer;
    private final Timer executeWaitTimer;
    private final Meter serializationErrors;

    public Redisq(String name, Duration timeout, Duration ttlStateInfo, Duration lockTime,
            Duration discardTime,
            Consumer<T> consumer, Class<T> klass, Pool<Jedis> jedisPool, ExecutorService threadPool,
            MetricRegistry metricRegistry) {
        this.name = name;
        this.timeout = timeout;
        this.ttlStateInfo = (int) ttlStateInfo.getSeconds();
        this.lockTime = (int) lockTime.getSeconds();
        this.discardTime = discardTime;
        this.metricRegistry = metricRegistry;
        this.subscription = new RedisqConsumer<>(consumer, jedisPool, this);
        this.names = new Names();
        this.queueName = names.queueNameFor(name);
        this.inFlightQueueName = names.inFlightQueueNameFor(name);
        this.jedisPool = jedisPool;
        this.threadPool = threadPool;
        this.mapper = new Mapper<>(new ObjectMapper().getTypeFactory()
                .constructParametricType(TimedWrap.class, klass));

        this.pushTimer = metricRegistry.timer(name(this.getClass(), "push"));
        this.idleTimer = metricRegistry.timer(name(this.getClass(), "idle"));
            metricRegistry.register(name(this.getClass(), "queue", "size"),
                new CachedGauge<Long>(15, TimeUnit.SECONDS) {
                    @Override
                    protected Long loadValue() {
                        try (Jedis jedis = jedisPool.getResource()) {
                            return jedis.llen(queueName);
                        }
                    }
                });
        this.restoreBlockedTimer = metricRegistry.timer(name(this.getClass(), "restore_blocked"));
        this.executeWaitTimer = metricRegistry.timer(name(this.getClass(), "execute_wait"));
        this.serializationErrors = metricRegistry
                .meter(name(this.getClass(), "serialization_errors"));
    }

    @Override
    public void push(T document) {
        long timestampMs = System.currentTimeMillis();
        String serialized;
        String stateSerialized;
        try {
            serialized = mapper.serialize(new TimedWrap<>(document, timestampMs));
            stateSerialized = stateMapper.serialize(new StateInfo(NEW, timestampMs, ""));
        } catch (SerializationException e) {
            serializationErrors.mark();
            throw new RuntimeException("Could not serialize element " + document.getIdAsString(),
                    e);
        }
        LOG.debug("Jedis active: {}, idle: {}", jedisPool.getNumActive(), jedisPool.getNumIdle());
        try (Jedis jedis = jedisPool.getResource(); Timer.Context ignored = pushTimer.time();) {
            Transaction transaction = jedis.multi();
            String id = document.getIdAsString();
            String lockId = names.lockKeyFromId(id);
            transaction.setex(lockId, lockTime, "locked");
            transaction.lpush(queueName, id);
            transaction.setex(names.contentKeyFromId(id), ttlStateInfo, serialized);
            transaction.setex(names.stateKeyFromId(id), ttlStateInfo, stateSerialized);
            transaction.publish(names.stateChannelKeyFromId(id), stateSerialized);
            transaction.exec();
            LOG.debug("Pushed {} with lockTime {}s lock id: {}", id, lockTime, lockId);
        }
    }

    @Override
    public void startConsumer() {
        working.set(true);
        mainLoop = Executors.newSingleThreadExecutor().submit(() -> {
            // We keep one resource for the iteration
            while (working.get()) {
                try {
                    iteration();
                } catch(JedisConnectionException exception) {
                    reportBadConnectionAndStop(exception);
                }
            }
        });
        inFlightLoop = Executors.newSingleThreadExecutor().submit(() -> {
            while (working.get()) {
                try {
                    inflightIteration();
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch(JedisConnectionException exception) {
                    reportBadConnectionAndStop(exception);
                }
            }
        });
    }

    private void reportBadConnectionAndStop(JedisConnectionException exception) {
        LOG.error("Could not connect to Redis. Jedis closed: {}", jedisPool.isClosed(), exception);
        working.set(false);
    }

    @Override
    public Future<Void> getFutureForDocumentStateWait(Set<State> state, String id, long timeout,
            TimeUnit unit) throws StateFutureInitializationException {
        return new StateFuture(state, id, jedisPool, timeout, unit, metricRegistry);
    }

    @Override
    public Future<Void> getFutureForDocumentStateWait(Set<State> state, String id, long timeout,
            TimeUnit unit, Pool<Jedis> pool) throws StateFutureInitializationException {
        return new StateFuture(state, id, pool, timeout, unit, metricRegistry);
    }

    private void inflightIteration() {
        List<String> processingElements;
        try (Jedis jedis = jedisPool.getResource()) {
            processingElements = jedis.lrange(inFlightQueueName, 0, -1);
        }
        processingElements
                .forEach(id -> {
                    try (Jedis jedis = jedisPool.getResource()) {
                        String lockId = names.lockKeyFromId(id);
                        // TODO We might get more than one consumer doing this
                        Long ttl = jedis.ttl(lockId);
                        if (ttl == 0  /* TODO check this || ttl == -2 */) {
                            LOG.debug("Found unlocked element {}, lockId({}), ttl={}", id,
                                    lockId, ttl);
                            try (Context ignored = restoreBlockedTimer.time()) {
                                // Restore it in the main queue
                                Transaction multi = jedis.multi();
                                multi.lrem(inFlightQueueName, 1, id);
                                multi.lpush(queueName, id);
                                multi.exec();
                            }
                        }
                    }
                });
    }

    private void iteration() {
        long timestampMs = System.currentTimeMillis();
        String value;
        String key;
        try (Jedis jedis = jedisPool.getResource()) {
            String id;
            try(Context ignored = idleTimer.time()) {
                id = jedis.brpoplpush(queueName, inFlightQueueName, (int) timeout.getSeconds());
            }
            // If something goes wrong after this, the job will be stuck in inflightIteration
            if (id != null) {
                key = names.contentKeyFromId(id);
                value = lockAndGetDocument(timestampMs, key, jedis, id);
            } else {
                LOG.debug("Empty queue");
                return;
            }
        }
        if (value != null && key != null) {
            TimedWrap<T> element;
            try {
                element = mapper.deserialize(value);
            } catch (DeserializationException e) {
                LOG.error("Failed deserialization, skipping element: {}", value, e);
                return;
            }
            try {
                if (Duration.ofMillis(timestampMs - element.getTimestampMs())
                        .compareTo(discardTime) < 0) {
                    try (Context ignored = executeWaitTimer.time()) {
                        execute(element);
                    }
                }
            } catch (RejectedExecutionException e) {
                processRejected(value, key, element, e);
            }
        }
    }

    private String lockAndGetDocument(long timestampMs, String key, Jedis jedis, String id) {
        String value;
        LOG.debug("Found id {}", id);
        jedis.setex(names.lockKeyFromId(id), lockTime, "locked");
        Optional<StateInfo> state = getState(id);
        if (state.isPresent() && !state.get().getState().equals(NEW)) {
            LOG.warn("State already present for {}: {}", id, state.get().getState());
        }
        setState(jedis, timestampMs, id, PROCESSING, "");
        value = jedis.get(key);
        return value;
    }

    private void processRejected(String value, String key, TimedWrap<T> element,
            RejectedExecutionException e) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.lpush(key, value);
            LOG.error("Rejected execution, re-enqueued {}",
                    element.getElement().getIdAsString(), e);
        } catch (Exception pushE) {
            LOG.error("Could not re-enqueue {}", element.getElement().getIdAsString(), e);
        }
    }

    private void execute(TimedWrap<T> element) {
        threadPool.execute(() -> {
            runningThreads.incrementAndGet();
            try {
                subscription.process(element.getElement());
            } finally {
                runningThreads.decrementAndGet();
            }
        });
    }

    @Override
    public void setState(String id, State state, String info) {
        long timestampMs = System.currentTimeMillis();
        try (Jedis jedis = jedisPool.getResource()) {
            setState(jedis, timestampMs, id, state, info);
        }
    }

    public void setState(Jedis jedis, long timestampMs, String id, State state, String info) {
        String stateSerialized;
        StateInfo stateInfo = new StateInfo(state, timestampMs, info);
        try {
            stateSerialized = stateMapper.serialize(stateInfo);
        } catch (SerializationException e) {
            throw new RuntimeException("Could not serialize state " + stateInfo);
        }
        jedis.setex(names.stateKeyFromId(id), ttlStateInfo, stateSerialized);
        jedis.publish(names.stateChannelKeyFromId(id), stateSerialized);
    }

    @Override
    public Optional<StateInfo> getState(String id) {
        String key = names.stateKeyFromId(id);
        return getStateInfoFromRedisKey(key);
    }

    @Override
    public Stream<Optional<ExtendedStateInfo>> getStates() {
        Stream<String> keys;
        try (Jedis jedis = jedisPool.getResource()) {
            keys = jedis.keys(names.stateKeyFromId("*")).stream();
        }
        return keys.map(key -> {
            Optional<StateInfo> stateInfoFromRedisKey = getStateInfoFromRedisKey(key);
            return stateInfoFromRedisKey.map(stateInfo -> new ExtendedStateInfo(key, stateInfo));
        });
    }

    @Override
    public void close() throws InterruptedException {
        LOG.debug("Closing {}", name);
        synchronized (this) {
            working.set(false);
            if (mainLoop != null) {
                try {
                    mainLoop.get();
                    inFlightLoop.get();
                    try {
                        CLOSE_RETRIER.call(this.runningThreads::get);
                    } catch (RetryException e) {
                        LOG.warn("Closing while some threads are still running");
                    }
                } catch (ExecutionException e) {
                    LOG.error("Error during close", e);
                }
            }
        }
        LOG.debug("Shutting down thread {}", name);
        threadPool.shutdown();
        threadPool.awaitTermination(1, TimeUnit.MINUTES);
        LOG.info("Closed {}", name);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public QueueConsumer<T> getConsumer() {
        return subscription;
    }

    @Override
    public void pushAndWait(T dummyObject, long waitTimeout, TimeUnit waitTimeoutUnit)
            throws WaitException {
        Future<Void> f = getFutureForDocumentStateWait(ImmutableSet.of(DONE, FAILED), dummyObject.getIdAsString(),
                waitTimeout, waitTimeoutUnit);
        push(dummyObject);
        try {
            f.get(waitTimeout, waitTimeoutUnit);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new WaitException(
                    "Could not wait for " + dummyObject.getIdAsString() + " to be done", e);
        }
    }

    public Names getNames() {
        return names;
    }

    private Optional<StateInfo> getStateInfoFromRedisKey(String key) {
        try {
            String element;
            try(Jedis jedis = jedisPool.getResource()) {
                element = jedis.get(key);
            }
            if (element == null) {
                return Optional.empty();
            } else {
                return Optional.of(stateMapper.deserialize(element));
            }
        } catch (DeserializationException e) {
        throw new RuntimeException("Could not deserialize state info for " + key, e);
    }
    }
}
