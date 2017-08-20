package ai.grakn.redisq.exceptions;

import java.io.IOException;

public class DeserializationException extends IOException {
    private final String element;

    public DeserializationException(String message, String element, Throwable e) {
        super(message, e);
        this.element = element;
    }

    public String getElement() {
        return element;
    }
}
