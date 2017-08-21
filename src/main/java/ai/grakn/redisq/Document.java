package ai.grakn.redisq;

/**
 * Document stored in the queue. It's required to have an Id.
 */
public interface Document {

    /**
     * It returns the id of the document
     * @return  The id of the document as a string
     */
    String getIdAsString();
}
