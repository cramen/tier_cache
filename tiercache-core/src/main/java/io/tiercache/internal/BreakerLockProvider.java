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
public final class BreakerLockProvider implements DistributedLockProvider {

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
        if (!breaker.tryAcquire()) {
            return null;
        }
        try {
            DistributedLock lock = delegate.tryLock(name, lease);
            breaker.onSuccess();
            return lock;
        } catch (RuntimeException e) {
            breaker.onFailure();
            return null;
        }
    }
}
