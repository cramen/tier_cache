package io.tiercache;

/**
 * Thrown when cache configuration violates a startup invariant.
 * The message always names the offending cache, the violated invariant,
 * and the concrete values involved.
 *
 * @since 0.1.0
 */
public class CacheConfigurationException extends RuntimeException {

    /**
     * Creates the exception with a message describing the violated
     * invariant.
     *
     * @param message the violation description
     * @since 0.1.0
     */
    public CacheConfigurationException(String message) {
        super(message);
    }
}
