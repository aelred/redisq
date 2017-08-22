package ai.grakn.redisq;

import ai.grakn.redisq.exceptions.StateFutureInitializationException;
import ai.grakn.redisq.exceptions.WaitException;
import ai.grakn.redisq.consumer.QueueConsumer;

import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

public interface Queue<T> {
    /**
     * Put a document in the queue
     * @param document  Document to be pushed to the queue. It must be serialisable.
     */
    void push(T document);

    /**
     * Same as push but it waits for the state of the document to be DONE i.e. the consumer successfully completed
     * working on it.
     *
     * @param document          Document to be pushed to the queue. It must be serialisable.
     * @param waitTimeout       Timeout for the wait. A WaitException is thrown when expired
     * @param waitTimeoutUnit   Unit for the timeout
     * @throws WaitException    Thrown if a timeout occurs while waiting for the consumer to be acknowledged in Redis or if the waitTimeout expires
     */
    void pushAndWait(T document, long waitTimeout, TimeUnit waitTimeoutUnit) throws WaitException;


    /**
     * It returns a future that waits for a document to reac a certain state
     * Note that this works for DONE and FAILED since they are terminal states.
     * @param state     Desired state
     * @param id        Id of the document we are watching
     * @param timeout   How long to wait until failing
     * @param unit      Unit of the timeout
     * @return          A future that blocks on the state being equal to the given state
     * @throws StateFutureInitializationException   Thrown if it fails to subscribe to the state
     */
    Future<Void> getFutureForDocumentStateWait(State state, String id, long timeout, TimeUnit unit) throws StateFutureInitializationException;


    /**
     * @see ai.grakn.redisq.Queue#getFutureForDocumentStateWait(State, String, long, TimeUnit)
     * Also takes a jedis pool
     */
    Future<Void> getFutureForDocumentStateWait(State state, String id, long timeout, TimeUnit unit, Pool<Jedis> pool) throws StateFutureInitializationException;

    /**
     * Starts the comsumer for this queue. The consumer takes care of the whole lifecycle, so e.g. in the Redisq
     * implementation this includes a thread that consumes the elements in the queue and a thread
     * that makes sure there are no dead jobs in the inflight queue.
     */
    void startConsumer();


    /**
     * @throws InterruptedException
     */
    void close() throws InterruptedException;

    /**
     * @return
     */
    String getName();


    /**
     * @param id
     * @return
     */
    Optional<StateInfo> getState(String id);

    /**
     * @return
     */
    QueueConsumer<T> getConsumer();
}
