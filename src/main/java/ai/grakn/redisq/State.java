package ai.grakn.redisq;

public enum State {
    NEW, PROCESSING, FAILED, DONE;

    public boolean isFinal() {
        return this.equals(DONE) || this.equals(FAILED);
    }
}
