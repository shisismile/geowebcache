package org.geowebcache.mongo;

/**
 * @author shimingen
 * @date 2021/9/2
 */
public class NoSuchCollectionException extends RuntimeException {
    public NoSuchCollectionException() {
        super("no such collection");
    }

    public NoSuchCollectionException(String message) {
        super(message);
    }

    public NoSuchCollectionException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoSuchCollectionException(Throwable cause) {
        super(cause);
    }

    protected NoSuchCollectionException(
            String message,
            Throwable cause,
            boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
