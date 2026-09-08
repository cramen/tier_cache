package io.tiercache.internal;

/**
 * Marker for "L2 is unavailable" (breaker open or failed call). Never escapes
 * the library — the cache catches it and takes the degraded path.
 */
public final class L2UnavailableException extends RuntimeException {

    public static final L2UnavailableException OPEN = new L2UnavailableException("breaker open");

    public L2UnavailableException(String message) {
        super(message);
    }

    public L2UnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
