package io.tiercache.internal;

/**
 * Marker for "L2 is unavailable" (breaker open or failed call). Never escapes
 * the library — the cache catches it and takes the degraded path.
 *
 * <p><b>Internal — not part of the supported API.</b>
 *
 * @since 0.1.0
 */
public final class L2UnavailableException extends RuntimeException {

    /**
     * Shared, preallocated instance thrown when the breaker is open: no
     * allocation on the degraded path.
     *
     * @since 0.1.0
     */
    public static final L2UnavailableException OPEN = new L2UnavailableException("breaker open");

    /**
     * Creates the exception with a message.
     *
     * @param message the failure description
     * @since 0.1.0
     */
    public L2UnavailableException(String message) {
        super(message);
    }

    /**
     * Creates the exception with a message and cause.
     *
     * @param message the failure description
     * @param cause   the underlying infrastructure exception
     * @since 0.1.0
     */
    public L2UnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
