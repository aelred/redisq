package ai.grakn.redisq;

import ai.grakn.redisq.consumer.Mapper;
import ai.grakn.redisq.consumer.QueueConsumer;
import ai.grakn.redisq.exceptions.DeserializationException;
import ai.grakn.redisq.exceptions.SerializationException;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Felix Chapman
 */
public class SchedulerTest {

    private final static long THREAD_DELAY = 564;
    private final static String SERIALIZED_DOCUMENT = "I AM SERIALIZED";
    private final static String DOCUMENT_ID = "DOCUMENT ID HERE";

    private final ScheduledExecutorService threadPool = mock(ScheduledExecutorService.class);
    private final QueueConsumer<Document> consumer = mock(QueueConsumer.class);
    private final Document document = mock(Document.class);
    private final Document deserializedDocument = mock(Document.class);
    private final JedisPool jedisPool = mock(JedisPool.class);
    private final Jedis jedis = mock(Jedis.class);
    private final Mapper<Document> mapper = mock(Mapper.class);

    private final Scheduler<Document> scheduler = Scheduler.of(THREAD_DELAY, threadPool, consumer, jedisPool, mapper);

    @Before
    public void setUp() {
        when(jedisPool.getResource()).thenReturn(jedis);
        when(document.getIdAsString()).thenReturn(DOCUMENT_ID);
    }

    @Test
    public void whenExecutingAnElement_ARunnableIsScheduledForExecutionAfterTheDelayInSeconds() {
        scheduler.execute(document);

        verify(threadPool).schedule(any(Runnable.class), eq(THREAD_DELAY), eq(TimeUnit.SECONDS));
    }

    @Test
    public void whenExecutingAnElement_ARunnableIsScheduledThatDeserializesAndConsumesTheDocumentStoredInRedis()
            throws DeserializationException {

        doAnswer(q -> {
            q.getArgumentAt(0, Runnable.class).run();
            return null;
        }).when(threadPool).schedule(any(Runnable.class), anyLong(), any());

        when(jedis.get("redisq:delayed:" + DOCUMENT_ID)).thenReturn(SERIALIZED_DOCUMENT);
        when(mapper.deserialize(SERIALIZED_DOCUMENT)).thenReturn(deserializedDocument);

        scheduler.execute(document);

        verify(consumer).process(deserializedDocument);
    }

    @Test
    public void whenExecutingAnElement_TheElementIsInitiallyStoredInRedis() throws SerializationException {
        when(mapper.serialize(document)).thenReturn(SERIALIZED_DOCUMENT);

        scheduler.execute(document);

        verify(jedis).set("redisq:delayed:" + DOCUMENT_ID, SERIALIZED_DOCUMENT);
    }
}