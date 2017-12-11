package ai.grakn.redisq;

import ai.grakn.redisq.exceptions.RedisqException;
import ai.grakn.redisq.exceptions.StateFutureInitializationException;
import ai.grakn.redisq.exceptions.WaitException;
import ai.grakn.redisq.util.DummyConsumer;
import ai.grakn.redisq.util.DummyObject;
import ai.grakn.redisq.util.Names;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.util.Pool;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static ai.grakn.redisq.State.DONE;
import static ai.grakn.redisq.State.FAILED;
import static ai.grakn.redisq.State.PROCESSING;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RedisqTest {

    private static final Logger LOG = LoggerFactory.getLogger(RedisqTest.class);

    // Test constants
    private static final long TIMEOUT = 30;
    private static final TimeUnit UNIT = SECONDS;
    private static final int PORT = 6382;
    private static final int QUEUES = 3;
    private static final int DOCUMENTS = 10;
    private static final String LOCALHOST = "localhost";
    private static final int PRODUCERS = 10;
    private static final int CONSUMERS = 3;
    private static final int JEDIS_POOL_MAX_TOTAL = 16;
    private static final int JEDIS_POOL_MAX_WAIT_MILLIS = 5000;

    private static final String SOME_ID = "some id";
    
    private static RedisServer server;
    private static Pool<Jedis> jedisPool;

    @BeforeClass
    public static void beforeClass() throws IOException {
        server = new RedisServer(PORT);
        //server = RedisServer.newRedisServer();
        server.start();
    }

    @Before
    public void before() throws IOException {
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxTotal(JEDIS_POOL_MAX_TOTAL);
        genericObjectPoolConfig.setMaxWaitMillis(JEDIS_POOL_MAX_WAIT_MILLIS);
        jedisPool = new JedisPool(genericObjectPoolConfig, LOCALHOST, PORT);
//        jedisPool = new JedisPool(genericObjectPoolConfig, server.getHost(), server.getBindPort());
//
        try(Jedis resource = jedisPool.getResource()) {
            resource.flushAll();
        }
//        Executors.newSingleThreadExecutor().submit( () -> {
//            try(Jedis resource = jedisPool.getResource()) {
//                resource.monitor(new JedisMonitor() {
//                public void onCommand(String command) {
//                    System.out.println(command);
//                }
//            });
//            }
//        });
    }

    @Test
    public void whenPush_StateIsDone() throws WaitException, InterruptedException {
        Queue<DummyObject> redisq = getStandardRedisq();
        redisq.startConsumer();
        redisq.pushAndWait(new DummyObject(SOME_ID, 23, new HashMap<>()), TIMEOUT, UNIT);
        assertThat(redisq.getState(SOME_ID).get().getState(), equalTo(DONE));
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
                    throw new RedisqException(message);})
                .setDocumentClass(DummyObject.class)
                .createRedisq();
        redisq.startConsumer();
        String SOME_ID = "some id";
        redisq.push(new DummyObject(SOME_ID, 23, new HashMap<>()));
        redisq.getFutureForDocumentStateWait(ImmutableSet.of(FAILED), SOME_ID).get();
        assertThat(redisq.getState(SOME_ID).get().getState(), equalTo(FAILED));
        assertThat(redisq.getState(SOME_ID).get().getInfo(), equalTo(message));
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
        String SOME_ID = "some id";
        redisq.pushAndWait(new DummyObject(SOME_ID, 23, new HashMap<>()), TIMEOUT, UNIT);
        assertThat(redisq.getState(SOME_ID).get().getState(), equalTo(DONE));
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
                    String pidid = makePid(pid, j);
                    LOG.debug("Subscribing to {}", pidid);
                    Future<Void> futureForDocumentStateWait = redisq
                            .getFutureForDocumentStateWait(ImmutableSet.of(DONE), pidid, 1,
                                    SECONDS, jedisPool);
                    subscriptions.add(futureForDocumentStateWait);
                    LOG.debug("Pushing {}", pidid);
                    redisq.push(new DummyObject(pidid));
                }
                for(int j = 0; j < DOCUMENTS; j++) {
                    String pidid = makePid(pid, j);
                    try {
                        LOG.debug("Waiting for {}", pidid);
                        subscriptions.get(j).get(10, SECONDS);
                        LOG.debug("Wait for {} succeeded", pidid);
                    } catch (Throwable e) {
                        LOG.error("Wait for {} failed", pidid, e);
                        try(Jedis resource = jedisPool.getResource()) {
                            diagnostics(sharedQueueName, pidid, resource);
                        } catch (Throwable eDiag) {
                            LOG.error("Failed get diagnostics", eDiag);
                        } finally {
                            assertThat("Errors while waiting for state", false,  is(true));
                        }
                    }
                }
                redisq.close();
                return null;
            });
            producers.add(future);
        }
        LOG.info("Starting consumers");
        Set<Redisq<DummyObject>> consumers = new HashSet<>();
        for(int i = 0; i < CONSUMERS; i++) {
            consumersThreadPool.submit(() -> {
                Redisq<DummyObject> redisq = getRedisq(sharedQueueName);
                redisq.startConsumer();
                LOG.info("Started consumer for {}", sharedQueueName);
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

    private void diagnostics(String queueName, String pidid, Jedis resource) {
        Names names = new Names();
        String lockId = names.lockKeyFromId(pidid);
        String lock = resource.get(lockId);
        List<String> queue = resource.lrange(queueName, 0, -1);
        String content = resource.get(names.contentKeyFromId(pidid));
        String state = resource.get(names.stateKeyFromId(pidid));
        Boolean channelExists = resource
                .exists(names.stateChannelKeyFromId(pidid));
        LOG.error("Diagnostic info. \nLock: {}\nQueue: {}\nContent: {}\nState: {}\nChannel Exists:{}", lock, queue, content, state, channelExists);
    }

    private String makePid(int pid, int j) {
        return pid + "_" + j;
    }

    @Test
    public void whenExpired_GoesBackToQueue() throws WaitException, InterruptedException, ExecutionException {
        Redisq<DummyObject> redisq = getStandardRedisq();
        String SOME_ID = "some id";
        redisq.push(new DummyObject(SOME_ID, 23, new HashMap<>()));
        String lockId = redisq.getNames().lockKeyFromId(SOME_ID);
        try(Jedis resource = jedisPool.getResource()) {
            resource.setex(lockId,  1, "locked");
            redisq.setState(SOME_ID, PROCESSING);
            resource.brpoplpush(redisq.getNames().queueNameFor(redisq.getName()),  redisq.getNames().inFlightQueueNameFor(redisq.getName()), 1);
        }
        Future<Void> f = redisq.getFutureForDocumentStateWait(ImmutableSet.of(DONE), SOME_ID);
        redisq.startConsumer();
        f.get();
        assertThat(redisq.getState(SOME_ID).get().getState(), equalTo(DONE));
        redisq.close();
    }

    @Test(expected = TimeoutException.class)
    public void whenSubscribeToNonExistingId_Timeout() throws WaitException, InterruptedException, TimeoutException, ExecutionException {
        Queue<DummyObject> redisq = getStandardRedisq();
        redisq.startConsumer();
        String SOME_ID = "some id";
        Future<Void> sub = redisq.getFutureForDocumentStateWait(ImmutableSet.of(DONE), SOME_ID);
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
            String SOME_ID = Names.getRandomString();
            Future<Void> sub = redisq.getFutureForDocumentStateWait(ImmutableSet.of(DONE), SOME_ID);
            redisq.push(new DummyObject(SOME_ID, 23, new HashMap<>()));
            ss.put(SOME_ID, sub);
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
                LOG.info("Pushing {}", id);
                redisq.push(new DummyObject(id, 23, new HashMap<>()));
                ids.put(id, sub);
            }
            idsAndCompletable.put(someQueueId, ids);
            LOG.info("Closing queue {}", someQueueId);
            redisq.close();
        }
        LOG.info("Reading");
        idsAndCompletable.forEach((queueName, idMap) -> {
            Queue<DummyObject> redisq = getRedisq(queueName);
            redisq.startConsumer();
            for(String id : idMap.keySet()) {
                try {
                    LOG.info("Waiting on {}", id);
                    idMap.get(id).get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                State state = redisq.getState(id).get().getState();
                if (!state.equals(DONE)) {
                    try(Jedis resource = jedisPool.getResource()) {
                        diagnostics(queueName, id, resource);
                    }
                }
                assertThat(id + " not done", state, equalTo(DONE));
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
                String SOME_ID = Names.getRandomString();
                Future<Void> sub = redisq.getFutureForDocumentStateWait(ImmutableSet.of(DONE), SOME_ID);
                redisq.push(new DummyObject(SOME_ID, 23, new HashMap<>()));
                ss.put(SOME_ID, sub);
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
                String SOME_ID = Names.getRandomString();
                LOG.debug("Processing {}", SOME_ID);
                Future<Void> sub = redisq.getFutureForDocumentStateWait(ImmutableSet.of(DONE), SOME_ID);
                LOG.debug("Subscribed {}", SOME_ID);
                redisq.push(new DummyObject(SOME_ID, 23, new HashMap<>()));
                LOG.debug("Pushed {}", SOME_ID);
                ss.put(SOME_ID, sub);
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