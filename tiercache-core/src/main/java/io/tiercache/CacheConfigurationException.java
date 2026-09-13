package io.tiercache;

/**
 * Thrown when cache configuration violates a startup invariant.
 * The message always names the offending cache, the violated invariant,
 * and the concrete values involved.
 */
public class CacheConfigurationException extends RuntimeException {

    public CacheConfigurationException(String message) {
        super(message);
    }
}
