package ai.grakn.redisq;

import ai.grakn.redisq.util.DummyConsumer;
import ai.grakn.redisq.util.DummyObject;
import ai.grakn.redisq.util.Names;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.util.Pool;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static ai.grakn.redisq.State.DONE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RedisqTest {
    private static final Logger LOG = LoggerFactory.getLogger(RedisqTest.class);
    private static final long TIMEOUT = 100;
    private static final TimeUnit UNIT = TimeUnit.SECONDS;
    private static final int PORT = 6382;
    private RedisServer server;
    private Pool<Jedis> jedisPool;

    @Before
    public void before() throws IOException {
        server = new RedisServer(PORT);
        server.start();
        jedisPool = new JedisPool("localhost", PORT);
        try(Jedis resource = jedisPool.getResource()) {
            resource.flushAll();
        }
    }


    @Test
    public void whenPush_StateIsDone() throws InterruptedException {
        Redisq<DummyObject> redisq = getStandardRedisq();
        redisq.startSubscription();
        String someId = "some id";
        redisq.pushAndWait(new DummyObject(someId, 23, new HashMap<>()), TIMEOUT, UNIT);
        assertThat(redisq.getState(someId).get().getState(), equalTo(DONE));
        redisq.close();
    }

    @Test
    public void whenExpired_GoesBackToQueue() throws InterruptedException {
        Redisq<DummyObject> redisq = getStandardRedisq();
        String someId = "some id";
        redisq.push(new DummyObject(someId, 23, new HashMap<>()));
        String lockId = redisq.getNames().lockKeyFromId(someId);
        try(Jedis resource = jedisPool.getResource()) {
            resource.setex(lockId,  15, "locked");
            resource.brpoplpush(redisq.getNames().queueNameFor(redisq.getName()),  redisq.getNames().inFlightQueueNameFor(redisq.getName()), 1);
        }
        CompletableFuture<Void> f = redisq.subscribeToState(DONE, someId, 30, TimeUnit.SECONDS);
        redisq.startSubscription();
        f.join();
        assertThat(redisq.getState(someId).get().getState(), equalTo(DONE));
        redisq.close();
    }

    @Test(expected = TimeoutException.class)
    public void whenSubscribeToNonExistingId_Timeout() throws InterruptedException, TimeoutException, ExecutionException {
        Redisq<DummyObject> redisq = getStandardRedisq();
        redisq.startSubscription();
        String someId = "some id";
        CompletableFuture<Void> sub = redisq.subscribeToState(DONE, someId, TIMEOUT, UNIT);
        sub.get(1, TimeUnit.SECONDS);
    }

    @Test
    public void whenGetNonExistingState_StateNotPresent() throws InterruptedException, TimeoutException, ExecutionException {
        Redisq<DummyObject> redisq = getStandardRedisq();
        redisq.startSubscription();
        Optional<StateInfo> state = redisq.getState("12345");
        assertThat(state.isPresent(), is(false));
    }

    @Test
    public void whenPushMany_StateIsDone() throws InterruptedException {
        Redisq<DummyObject> redisq = getStandardRedisq();
        redisq.startSubscription();
        Map<String, CompletableFuture<Void>> ss = new HashMap<>();
        for(int i = 0; i < 100; i++) {
            String someId = Names.getRandomString();
            CompletableFuture<Void> sub = redisq.subscribeToState(DONE, someId, TIMEOUT, UNIT);
            redisq.push(new DummyObject(someId, 23, new HashMap<>()));
            ss.put(someId, sub);
        }
        for(String id : ss.keySet()) {
            ss.get(id).join();
            assertThat(redisq.getState(id).get().getState(), equalTo(DONE));
        }
        redisq.close();
    }

    @Test
    public void whenPushManyOnDifferentQueuesAndClose_StateIsDone() throws InterruptedException {
        Map<String, Map<String, CompletableFuture<Void>>> idsAndCompletable = new HashMap<>();
        for(int j = 0; j < 10; j++) {
            String someQueueId = Names.getRandomString();
            Redisq<DummyObject> redisq = getRedisq(someQueueId);
            HashMap<String, CompletableFuture<Void>> ids = new HashMap<>();
            redisq.startSubscription();
            for (int i = 0; i < 100; i++) {
                String id = Names.getRandomString();
                CompletableFuture<Void> sub = redisq.subscribeToState(DONE, id, TIMEOUT, UNIT);
                redisq.push(new DummyObject(id, 23, new HashMap<>()));
                ids.put(id, sub);
            }
            idsAndCompletable.put(someQueueId, ids);
            redisq.close();
        }
        idsAndCompletable.forEach((queueName, idMap) -> {
            Redisq<DummyObject> redisq = getRedisq(queueName);
            redisq.startSubscription();
            for(String id : idMap.keySet()) {
                System.out.println(id);
                idMap.get(id).join();
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
    public void whenPushManyQueuesAndClose_StateIsDone() throws InterruptedException {
        Map<String, CompletableFuture<Void>> ss = new HashMap<>();
        String qname = "qname";
        for(int j = 0; j < 5; j++) {
            Redisq<DummyObject> redisq = getRedisq(qname);
            redisq.startSubscription();
            for (int i = 0; i < 10; i++) {
                String someId = Names.getRandomString();
                CompletableFuture<Void> sub = redisq.subscribeToState(DONE, someId, TIMEOUT, UNIT);
                redisq.push(new DummyObject(someId, 23, new HashMap<>()));
                ss.put(someId, sub);
            }
            for(String id : ss.keySet()) {
                ss.get(id).join();
                assertThat(redisq.getState(id).get().getState(), equalTo(DONE));
            }
            redisq.close();
        }
    }

    @Test
    public void testPushManyWithClosuresAfter() throws InterruptedException {
        Map<String, CompletableFuture<Void>> ss = new HashMap<>();
        String qname = "qname";
        for(int j = 0; j < 5; j++) {
            Redisq<DummyObject> redisq = getRedisq(qname);
            redisq.startSubscription();
            for (int i = 0; i < 10; i++) {
                String someId = Names.getRandomString();
                LOG.debug("Processing {}", someId);
                CompletableFuture<Void> sub = redisq.subscribeToState(DONE, someId, TIMEOUT, UNIT);
                LOG.debug("Subscribed {}", someId);
                redisq.push(new DummyObject(someId, 23, new HashMap<>()));
                LOG.debug("Pushed {}", someId);
                ss.put(someId, sub);
            }
            // The problem here is that many subscription pulls can happen after the close
            // So there are subscription still pending
            // Since everything is closed, we cannot even re-enqueue these
            redisq.close();
        }
        Redisq<DummyObject> redisq = getRedisq(qname);
        redisq.startSubscription();
        for(String id : ss.keySet()) {
            ss.get(id).join();
            assertThat(redisq.getState(id).get().getState(), equalTo(DONE));
        }
        redisq.close();
    }

    private Redisq<DummyObject> getStandardRedisq() {
        return new RedisqBuilder<DummyObject>()
                .setJedisPool(jedisPool)
                .setConsumer(new DummyConsumer<>())
                .setKlass(DummyObject.class)
                .createRedisq();
    }

    private Redisq<DummyObject> getRedisq(String name) {
        return new RedisqBuilder<DummyObject>()
                .setJedisPool(jedisPool)
                .setName(name)
                .setConsumer(new DummyConsumer<>())
                .setKlass(DummyObject.class)
                .createRedisq();
    }


    @After
    public void after() {
        jedisPool.close();
        server.stop();
        server = null;
    }

}