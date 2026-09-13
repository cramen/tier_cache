package io.tiercache.spi;

import java.time.Duration;

/**
 * SPI for distributed rebuild locks. Acquired via
 * {@link DistributedLockProvider#tryLock}.
 */
public interface DistributedLock extends AutoCloseable {

    /**
     * Extends this lock's lease. Callers use this from a watchdog while their
     * work is in progress.
     *
     * @return {@code true} if the lease was extended, {@code false} if the
     *         lock is no longer held (lost or expired)
     */
    boolean extend(Duration lease);

    /**
     * Releases the lock. Safe to call when the lock is already lost or
     * expired: never releases another holder's lock.
     */
    void release();

    @Override
    default void close() {
        release();
    }
}
