package io.tiercache.spi;

/**
 * Implemented by {@link RemoteCache} implementations that can also provide
 * distributed rebuild locks. Lets the factory derive the lock
 * provider automatically from the configured L2 transport.
 *
 * <p><b>Internal — not part of the supported API.</b> Extension point for
 * L2 transport implementations.
 *
 * @since 0.1.0
 */
public interface LockProviderSource {

    /**
     * The lock provider colocated with this L2 implementation.
     *
     * @return the lock provider; never {@code null}
     * @since 0.1.0
     */
    DistributedLockProvider lockProvider();
}
