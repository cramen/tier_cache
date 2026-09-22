package io.tiercache.internal;

import io.tiercache.spi.DistributedLock;
import io.tiercache.spi.DistributedLockProvider;

import java.time.Duration;

/**
 * {@link DistributedLockProvider} guarded by the L2 circuit breaker: fails
 * fast when open (returns no lock — coordination degrades to per-instance
 * coalescing), accounts outcomes otherwise.
 *
 * <p><b>Internal — not part of the supported API.</b>
 *
 * @since 0.1.0
 */
public final class BreakerLockProvider implements DistributedLockProvider, AutoCloseable {

    private final DistributedLockProvider delegate;
    private final CircuitBreaker breaker;

    /**
     * Creates a guarded provider.
     *
     * @param delegate the provider to guard
     * @param breaker  the breaker that gates lock acquisition
     * @since 0.1.0
     */
    public BreakerLockProvider(DistributedLockProvider delegate, CircuitBreaker breaker) {
        this.delegate = delegate;
        this.breaker = breaker;
    }

    @Override
    public DistributedLock tryLock(String name, Duration lease) {
        CircuitBreaker.Permit permit = breaker.tryAcquirePermit();
        if (permit == null) {
            return null;
        }
        try {
            DistributedLock lock = delegate.tryLock(name, lease);
            permit.success();
            return lock;
        } catch (RuntimeException e) {
            permit.failure();
            return null;
        }
    }

    /**
     * Forwards the close to the delegate when it is closeable (lifecycle
     * transparency for ownership wiring).
     *
     * @since 1.4.0
     */
    @Override
    public void close() throws Exception {
        if (delegate instanceof AutoCloseable closeable) {
            closeable.close();
        }
    }
}
