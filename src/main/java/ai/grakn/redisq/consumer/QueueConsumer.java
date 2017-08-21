package ai.grakn.redisq.consumer;

/**
 * Interface for queue consumers.
 *
 * @param <T>   Class of the object that is consumed.
 */
public interface QueueConsumer<T> {

    /**
     * This method implements the logic for consuming documents from the queue.
     * The result is not returned but some effect should be produced.
     *
     * @param document Document read from the queue
     */
    void process(T document);
}
