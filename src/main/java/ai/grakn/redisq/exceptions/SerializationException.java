package ai.grakn.redisq.exceptions;

import java.io.IOException;

public class SerializationException extends IOException {

    private final Object element;

    public <T> SerializationException(String message, Object element) {
        super(message);
        this.element = element;
    }

    public <T> SerializationException(String message, Object element, Throwable throwable) {
        super(message, throwable);
        this.element = element;
    }

    public Object getElement() {
        return element;
    }
}
