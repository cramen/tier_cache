package io.tiercache.spi;

import java.time.Duration;

/**
 * SPI for distributed rebuild locks. Acquired via
 * {@link DistributedLockProvider#tryLock}.
 *
 * <p><b>Internal — not part of the supported API.</b> Extension point for
 * lock-provider implementations.
 *
 * @since 0.1.0
 */
public interface DistributedLock extends AutoCloseable {

    /**
     * Extends this lock's lease. Callers use this from a watchdog while their
     * work is in progress.
     *
     * @param lease the new lease duration from now
     * @return {@code true} if the lease was extended, {@code false} if the
     *         lock is no longer held (lost or expired)
     * @since 0.1.0
     */
    boolean extend(Duration lease);

    /**
     * Releases the lock. Safe to call when the lock is already lost or
     * expired: never releases another holder's lock.
     *
     * @since 0.1.0
     */
    void release();

    /**
     * Releases the lock; equivalent to {@link #release()}.
     *
     * @since 0.1.0
     */
    @Override
    default void close() {
        release();
    }
}
