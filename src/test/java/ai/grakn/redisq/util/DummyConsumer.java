package ai.grakn.redisq.util;

import ai.grakn.redisq.Document;

import java.util.function.Consumer;

public class DummyConsumer<T extends Document> implements Consumer<T> {
    @Override
    public void accept(T t) {

    }
}
