package ai.grakn.redisq.util;

import ai.grakn.redisq.Idable;

import java.util.function.Consumer;

public class DummyConsumer<T extends Idable> implements Consumer<T> {
    @Override
    public void accept(T t) {

    }
}
