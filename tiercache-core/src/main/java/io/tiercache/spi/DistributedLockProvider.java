package io.tiercache.spi;

import java.time.Duration;

/**
 * SPI for acquiring distributed rebuild locks. Separate from
 * {@link RemoteCache}: coordination is not storage.
 */
public interface DistributedLockProvider {

    /**
     * Attempts to acquire the named lock with the given lease.
     *
     * @return the lock if acquired, {@code null} if it is currently held by
     *         another caller
     */
    DistributedLock tryLock(String name, Duration lease);
}
