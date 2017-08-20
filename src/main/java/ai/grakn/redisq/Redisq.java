package ai.grakn.redisq;

import ai.grakn.redisq.exceptions.DeserializationException;
import ai.grakn.redisq.exceptions.SerializationException;
import ai.grakn.redisq.subscription.Mapper;
import ai.grakn.redisq.subscription.RedisqSubscription;
import ai.grakn.redisq.subscription.Subscription;
import ai.grakn.redisq.subscription.TimedWrap;
import ai.grakn.redisq.util.Names;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
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

public class Redisq<T extends Idable> implements Queue<T> {
    private static final Logger LOG = LoggerFactory.getLogger(Redisq.class);
    static final Mapper<StateInfo> stateMapper = new Mapper<>(StateInfo.class);

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
    private Subscription<T> subscription;
    private Future<?> mainLoop;
    private Future<?> inFlightLoop;

    private final Timer pushTimer;

    public Redisq(String name, Duration timeout, Duration ttlStateInfo, Duration lockTime, Duration discardTime,
                  Consumer<T> consumer, Class<T> klass, Pool<Jedis> jedisPool, ExecutorService threadPool,
                  MetricRegistry metricRegistry) {
        this.name = name;
        this.timeout = timeout;
        this.ttlStateInfo = (int) ttlStateInfo.getSeconds();
        this.lockTime = (int) lockTime.getSeconds();
        this.discardTime = discardTime;
        this.subscription = new RedisqSubscription<>(consumer, jedisPool, this);
        this.names = new Names();
        this.queueName = names.queueNameFor(name);
        this.inFlightQueueName = names.inFlightQueueNameFor(name);
        this.jedisPool = jedisPool;
        this.threadPool = threadPool;
        this.mapper = new Mapper<>(new ObjectMapper().getTypeFactory().constructParametricType(TimedWrap.class, klass));

        this.pushTimer = metricRegistry.timer(name(this.getClass(), "push"));
    }

    @Override
    public void push(T element) {
        long timestampMs = System.currentTimeMillis();
        String serialized;
        String stateSerialized;
        try {
            serialized = mapper.serialize(new TimedWrap<>(element, timestampMs));
            stateSerialized = stateMapper.serialize(new StateInfo(NEW, timestampMs));
        } catch (SerializationException e) {
            throw new RuntimeException("Could not serialize element " + element.getIdAsString(), e);
        }
        try (Jedis jedis = jedisPool.getResource(); Timer.Context ignored = pushTimer.time();) {
            Transaction transaction = jedis.multi();
            String id = element.getIdAsString();
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
    public void startSubscription() {
        working.set(true);
        mainLoop = Executors.newSingleThreadExecutor().submit(() -> {
            // We keep one resource for the iteration
            while (working.get()) {
                iteration();
            }
        });
        inFlightLoop = Executors.newSingleThreadExecutor().submit(() -> {
            while (working.get()) {
                inflightIteration();
            }
        });
    }


    public CompletableFuture<Void> subscribeToState(State state, String id, long timeout, TimeUnit unit) throws InterruptedException {
        return new UpdateChannelSubscription<>(state, id, jedisPool).subscribe(timeout, unit);
    }

    private void inflightIteration() {
        try (Jedis jedis = jedisPool.getResource()) {
            List<String> processingElements = jedis.lrange(inFlightQueueName, 0, -1);
            processingElements
                .forEach(id -> {
                    String lockId = names.lockKeyFromId(id);
                    // TODO We might get more than one consumer doing this
                    Long ttl = jedis.ttl(lockId);
                    if (ttl == 0  /* TODO check this || ttl == -2 */) {
                        LOG.debug("Found unlocked element {}, lockId({}), ttl={}", id, lockId, ttl);
                        // Restore it in the main queue
                        Transaction multi = jedis.multi();
                        multi.lrem(inFlightQueueName, 1, id);
                        multi.lpush(queueName, id);
                        multi.exec();
                    }
                });
            }
    }

    private void iteration() {
        long timestampMs = System.currentTimeMillis();
        try (Jedis jedis = jedisPool.getResource()) {
            String id = jedis.brpoplpush(queueName, inFlightQueueName, (int) timeout.getSeconds());
            // If something goes wrong after this, the job will be stuck in inflightIteration
            if (id != null) {
                LOG.debug("Found id {}", id);
                jedis.setex(names.lockKeyFromId(id), lockTime, "locked");
                Optional<StateInfo> state = getState(id);
                if (state.isPresent() && !state.get().getState().equals(NEW)) {
                    LOG.warn("State already present for {}: {}", id, state.get().getState());
                }
                setState(jedis, timestampMs, id, PROCESSING);
                String key = names.contentKeyFromId(id);
                String value = jedis.get(key);
                TimedWrap<T> element;
                try {
                    element = mapper.deserialize(value);
                } catch (DeserializationException e) {
                    LOG.error("Failed deserialization, skipping element: {}", value, e);
                    return;
                }
                try {
                    if (Duration.ofMillis(timestampMs - element.getTimestampMs()).compareTo(discardTime) < 0) {
                        threadPool.execute(() -> {
                            runningThreads.incrementAndGet();
                            try {
                                subscription.process(element.getElement());
                            } finally {
                                runningThreads.decrementAndGet();
                            }
                        });
                    }
                } catch (RejectedExecutionException e) {
                    try {
                        jedis.lpush(key, value);
                        LOG.error("Rejected execution, re-enqueued {}", element.getElement().getIdAsString(), e);
                    } catch (Exception pushE) {
                        LOG.error("Could not re-enqueue {}", element.getElement().getIdAsString(), e);
                    }
                }
            } else {
                LOG.debug("Empty queue");
            }
        }
    }

    public void setState(Jedis jedis, long timestampMs, String id, State state) {
        String stateSerialized;
        StateInfo stateInfo = new StateInfo(state, timestampMs);
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
        try (Jedis jedis = jedisPool.getResource()) {
            try {
                String element = jedis.get(names.stateKeyFromId(id));
                if (element == null) {
                    return Optional.empty();
                } else {
                    return Optional.of(stateMapper.deserialize(element));
                }
            } catch (DeserializationException e) {
                throw new RuntimeException("Could not deserialize state info for " + id, e);
            }
        }
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
                    while(runningThreads.get() > 0) {
                        LOG.debug("Still {} threads running", runningThreads.get());
                        Thread.sleep(100);
                    }
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }
        LOG.debug("Shutting down thread {}", name);
        threadPool.shutdown();
        threadPool.awaitTermination(10, TimeUnit.SECONDS);
        LOG.info("Closed {}", name);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Subscription<T> getSubscription() {
        return subscription;
    }

    public Names getNames() {
        return names;
    }

    public void pushAndWait(T dummyObject, long timeout, TimeUnit unit) throws InterruptedException {
        CompletableFuture<Void> f = subscribeToState(DONE, dummyObject.getIdAsString(), timeout, unit);
        push(dummyObject);
        f.join();
    }
}
