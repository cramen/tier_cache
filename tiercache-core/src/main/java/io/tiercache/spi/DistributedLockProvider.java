package io.tiercache.spi;

import java.time.Duration;

/**
 * SPI for acquiring distributed rebuild locks. Separate from
 * {@link RemoteCache}: coordination is not storage.
 *
 * <p><b>Internal — not part of the supported API.</b> Extension point for
 * lock-provider implementations.
 *
 * @since 0.1.0
 */
public interface DistributedLockProvider {

    /**
     * Attempts to acquire the named lock with the given lease.
     *
     * @param name  the lock name
     * @param lease the lock lease; expired locks are released implicitly
     * @return the lock if acquired, {@code null} if it is currently held by
     *         another caller
     * @since 0.1.0
     */
    DistributedLock tryLock(String name, Duration lease);
}
