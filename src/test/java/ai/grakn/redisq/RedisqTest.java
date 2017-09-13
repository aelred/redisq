package ai.grakn.redisq;

import static ai.grakn.redisq.State.FAILED;
import ai.grakn.redisq.exceptions.StateFutureInitializationException;
import ai.grakn.redisq.exceptions.WaitException;
import ai.grakn.redisq.util.DummyConsumer;
import ai.grakn.redisq.util.DummyObject;
import ai.grakn.redisq.util.Names;
import com.google.common.collect.ImmutableSet;
import static java.util.concurrent.TimeUnit.SECONDS;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.internal.matchers.Null;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisMonitor;
import redis.clients.jedis.JedisPool;
import redis.clients.util.Pool;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import static ai.grakn.redisq.State.DONE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RedisqTest {

    private static final Logger LOG = LoggerFactory.getLogger(RedisqTest.class);

    // Test constants
    private static final long TIMEOUT = 30;
    private static final TimeUnit UNIT = SECONDS;
    private static final int PORT = 6382;
    private static final int QUEUES = 5;
    private static final int DOCUMENTS = 10;
    private static final String LOCALHOST = "localhost";
    private static final int PRODUCERS = 10;
    private static final int CONSUMERS = 3;
    private static final int JEDIS_POOL_MAX_TOTAL = 16;
    private static final int JEDIS_POOL_MAX_WAIT_MILLIS = 5000;

    private static RedisServer server;
    private static Pool<Jedis> jedisPool;

    @BeforeClass
    public static void beforeClass() throws IOException {
        server = new RedisServer(PORT);
        server.start();
    }

    @Before
    public void before() throws IOException {
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxTotal(JEDIS_POOL_MAX_TOTAL);
        genericObjectPoolConfig.setMaxWaitMillis(JEDIS_POOL_MAX_WAIT_MILLIS);
        jedisPool = new JedisPool(genericObjectPoolConfig, LOCALHOST, PORT);
        try(Jedis resource = jedisPool.getResource()) {
            resource.flushAll();
//            Executors.newSingleThreadExecutor().submit( () -> resource.monitor(new JedisMonitor() {
//                public void onCommand(String command) {
//                    System.out.println(command);
//                }
//            }));
        }

    }

    @Test
    public void whenPush_StateIsDone() throws WaitException, InterruptedException {
        Queue<DummyObject> redisq = getStandardRedisq();
        redisq.startConsumer();
        String someId = "some id";
        redisq.pushAndWait(new DummyObject(someId, 23, new HashMap<>()), TIMEOUT, UNIT);
        assertThat(redisq.getState(someId).get().getState(), equalTo(DONE));
        redisq.close();
    }

    @Test
    public void whenPushError_StateIsFailed()
            throws WaitException, InterruptedException, TimeoutException, ExecutionException {
        final String message = "Message";
        Queue<DummyObject> redisq = new RedisqBuilder<DummyObject>()
                .setJedisPool(jedisPool)
                .setName("my_queue")
                .setConsumer((d) -> {
                    throw new RuntimeException(message);})
                .setDocumentClass(DummyObject.class)
                .createRedisq();
        redisq.startConsumer();
        String someId = "some id";
        redisq.push(new DummyObject(someId, 23, new HashMap<>()));
        redisq.getFutureForDocumentStateWait(ImmutableSet.of(FAILED), someId).get();
        assertThat(redisq.getState(someId).get().getState(), equalTo(FAILED));
        assertThat(redisq.getState(someId).get().getInfo(), equalTo(message));
        redisq.close();
    }

    @Test
    public void whenPushReadmeExample_StateIsDone() throws WaitException, InterruptedException {
        Queue<DummyObject> redisq = new RedisqBuilder<DummyObject>()
                .setJedisPool(jedisPool)
                .setName("my_queue")
                .setConsumer((d) -> System.out.println("I'm consuming " + d.getIdAsString()))
                .setDocumentClass(DummyObject.class)
                .createRedisq();
        redisq.startConsumer();
        String someId = "some id";
        redisq.pushAndWait(new DummyObject(someId, 23, new HashMap<>()), TIMEOUT, UNIT);
        assertThat(redisq.getState(someId).get().getState(), equalTo(DONE));
        redisq.close();
    }

    @Test
    public void whenProducersAndConsumers_AllDocumentsAreProcessed() throws WaitException, InterruptedException {
        ExecutorService producersThreadPool = Executors.newFixedThreadPool(PRODUCERS);
        ExecutorService consumersThreadPool = Executors.newFixedThreadPool(CONSUMERS);
        String sharedQueueName = "prod_con_example_queue";
        Set<Future> producers = new HashSet<>();
        for(int i = 0; i < PRODUCERS; i++) {
            final int pid = i;
            Future<Void> future = producersThreadPool.submit(() -> {
                Redisq<DummyObject> redisq =  new RedisqBuilder<DummyObject>()
                        .setName(sharedQueueName)
                        .setJedisPool(jedisPool)
                        .setDocumentClass(DummyObject.class)
                        .createRedisq();
                // Don't start consumer
                List<Future<Void>> subscriptions = new ArrayList<>();
                // We make another JedisPool to hold the subscription connections
                GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
                genericObjectPoolConfig.setMaxTotal(DOCUMENTS);
                jedisPool = new JedisPool(genericObjectPoolConfig, LOCALHOST, PORT);
                for(int j = 0; j < DOCUMENTS; j++) {
                    // Note how we are making the subbscriptions before the push.
                    // Otherwise we might lose the state change
                    Future<Void> futureForDocumentStateWait = redisq
                            .getFutureForDocumentStateWait(ImmutableSet.of(DONE),  makePid(pid, j), 1,
                                    SECONDS, jedisPool);
                    subscriptions.add(futureForDocumentStateWait);
                    redisq.push(new DummyObject(makePid(pid, j)));
                }
                for(int j = 0; j < DOCUMENTS; j++) {
                    String pidid = makePid(pid, j);
                    try {
                        subscriptions.get(j).get();
                        LOG.debug("Wait for {} succeeded", pidid);
                    } catch (Exception e) {
                        LOG.error("Wait for {} failed", pidid, e);
                        assertThat("Errors while waiting for state", false);
                    }
                }
                redisq.close();
                return null;
            });
            producers.add(future);
        }
        Set<Redisq<DummyObject>> consumers = new HashSet<>();
        for(int i = 0; i < CONSUMERS; i++) {
            consumersThreadPool.submit(() -> {
                Redisq<DummyObject> redisq = getRedisq(sharedQueueName);
                redisq.startConsumer();
                consumers.add(redisq);
            });
        }
        producers.forEach(f -> {
            try {
                f.get();
                LOG.debug(f + " completed");
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });
        consumers.forEach(c -> {
            try {
                c.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    private String makePid(int pid, int j) {
        return pid + "_" + j;
    }

    @Test
    public void whenExpired_GoesBackToQueue() throws WaitException, InterruptedException, ExecutionException {
        Redisq<DummyObject> redisq = getStandardRedisq();
        String someId = "some id";
        redisq.push(new DummyObject(someId, 23, new HashMap<>()));
        String lockId = redisq.getNames().lockKeyFromId(someId);
        try(Jedis resource = jedisPool.getResource()) {
            resource.setex(lockId,  1, "locked");
            resource.brpoplpush(redisq.getNames().queueNameFor(redisq.getName()),  redisq.getNames().inFlightQueueNameFor(redisq.getName()), 1);
        }
        Future<Void> f = redisq.getFutureForDocumentStateWait(ImmutableSet.of(DONE), someId);
        redisq.startConsumer();
        f.get();
        assertThat(redisq.getState(someId).get().getState(), equalTo(DONE));
        redisq.close();
    }

    @Test(expected = TimeoutException.class)
    public void whenSubscribeToNonExistingId_Timeout() throws WaitException, InterruptedException, TimeoutException, ExecutionException {
        Queue<DummyObject> redisq = getStandardRedisq();
        redisq.startConsumer();
        String someId = "some id";
        Future<Void> sub = redisq.getFutureForDocumentStateWait(ImmutableSet.of(DONE), someId);
        sub.get(1, SECONDS);
    }

    @Test
    public void whenGetNonExistingState_StateNotPresent() throws InterruptedException, TimeoutException, ExecutionException {
        Queue<DummyObject> redisq = getStandardRedisq();
        redisq.startConsumer();
        Optional<StateInfo> state = redisq.getState("12345");
        assertThat(state.isPresent(), is(false));
    }

    @Ignore
    @Test
    public void whenPushMany_StateIsDone() throws InterruptedException, ExecutionException, StateFutureInitializationException {
        Queue<DummyObject> redisq = getStandardRedisq();
        redisq.startConsumer();
        Map<String, Future<Void>> ss = new HashMap<>();
        for(int i = 0; i < DOCUMENTS; i++) {
            String someId = Names.getRandomString();
            Future<Void> sub = redisq.getFutureForDocumentStateWait(ImmutableSet.of(DONE), someId);
            redisq.push(new DummyObject(someId, 23, new HashMap<>()));
            ss.put(someId, sub);
        }
        for(String id : ss.keySet()) {
            LOG.debug("Consuming {}", id);
            ss.get(id).get();
            assertThat(redisq.getState(id).get().getState(), equalTo(DONE));
        }
        assertThat(redisq.getStates().filter(Optional::isPresent).count(), equalTo((long) DOCUMENTS));
        redisq.getStates().forEach(s -> assertThat(s.get().getStateInfo().getState(), equalTo(DONE)));
        redisq.close();
    }

    @Test
    public void whenPushManyOnDifferentQueuesAndClose_StateIsDone() throws InterruptedException, StateFutureInitializationException {
        Map<String, Map<String, Future<Void>>> idsAndCompletable = new HashMap<>();
        for(int j = 0; j < QUEUES; j++) {
            String someQueueId = Names.getRandomString();
            Queue<DummyObject> redisq = getRedisq(someQueueId);
            HashMap<String, Future<Void>> ids = new HashMap<>();
            redisq.startConsumer();
            for (int i = 0; i < DOCUMENTS; i++) {
                String id = Names.getRandomString();
                Future<Void> sub = redisq.getFutureForDocumentStateWait(ImmutableSet.of(DONE), id);
                redisq.push(new DummyObject(id, 23, new HashMap<>()));
                ids.put(id, sub);
            }
            idsAndCompletable.put(someQueueId, ids);
            redisq.close();
        }
        idsAndCompletable.forEach((queueName, idMap) -> {
            Queue<DummyObject> redisq = getRedisq(queueName);
            redisq.startConsumer();
            for(String id : idMap.keySet()) {
                try {
                    idMap.get(id).get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                assertThat(redisq.getState(id).get().getState(), equalTo(DONE));
            }
            try {
                redisq.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void whenPushManyQueuesAndClose_StateIsDone() throws InterruptedException, ExecutionException, StateFutureInitializationException {
        Map<String, Future<Void>> ss = new HashMap<>();
        String qname = "qname";
        for(int j = 0; j < QUEUES; j++) {
            Queue<DummyObject> redisq = getRedisq(qname);
            redisq.startConsumer();
            for (int i = 0; i < DOCUMENTS; i++) {
                String someId = Names.getRandomString();
                Future<Void> sub = redisq.getFutureForDocumentStateWait(ImmutableSet.of(DONE), someId);
                redisq.push(new DummyObject(someId, 23, new HashMap<>()));
                ss.put(someId, sub);
            }
            for(String id : ss.keySet()) {
                ss.get(id).get();
                assertThat(redisq.getState(id).get().getState(), equalTo(DONE));
            }
            redisq.close();
        }
    }

    @Test
    public void whenPushAndClose_StateIsDone() throws InterruptedException, ExecutionException, StateFutureInitializationException {
        Map<String, Future<Void>> ss = new HashMap<>();
        String qname = "qname";
        for(int j = 0; j < QUEUES; j++) {
            Queue<DummyObject> redisq = getRedisq(qname);
            redisq.startConsumer();
            for (int i = 0; i < DOCUMENTS; i++) {
                String someId = Names.getRandomString();
                LOG.debug("Processing {}", someId);
                Future<Void> sub = redisq.getFutureForDocumentStateWait(ImmutableSet.of(DONE), someId);
                LOG.debug("Subscribed {}", someId);
                redisq.push(new DummyObject(someId, 23, new HashMap<>()));
                LOG.debug("Pushed {}", someId);
                ss.put(someId, sub);
            }
            // The problem here is that many consumer pulls can happen after the close
            // So there are consumer still pending
            // Since everything is closed, we cannot even re-enqueue these
            redisq.close();
        }
        Queue<DummyObject> redisq = getRedisq(qname);
        redisq.startConsumer();
        for(String id : ss.keySet()) {
            ss.get(id).get();
            assertThat(redisq.getState(id).get().getState(), equalTo(DONE));
        }
        redisq.close();
    }

    private Redisq<DummyObject> getStandardRedisq() {
        return new RedisqBuilder<DummyObject>()
                .setJedisPool(jedisPool)
                .setConsumer(new DummyConsumer<>())
                .setDocumentClass(DummyObject.class)
                .createRedisq();
    }

    private Redisq<DummyObject> getRedisq(String name) {
        return new RedisqBuilder<DummyObject>()
                .setJedisPool(jedisPool)
                .setName(name)
                .setConsumer(new DummyConsumer<>())
                .setDocumentClass(DummyObject.class)
                .createRedisq();
    }


    @After
    public void after() {
        jedisPool.close();
    }

    @AfterClass
    public static void afterClass() {
        server.stop();
        server = null;
    }

}