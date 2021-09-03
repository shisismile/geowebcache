package org.geowebcache.mongo;

/**
 * @author shimingen
 * @date 2021/9/2
 */
public class InvalidCollectionException extends RuntimeException {
    public InvalidCollectionException() {
    }

    public InvalidCollectionException(String message) {
        super(message);
    }

    public InvalidCollectionException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidCollectionException(Throwable cause) {
        super(cause);
    }

    public InvalidCollectionException(
            String message,
            Throwable cause,
            boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
