package io.tiercache.internal;

import io.tiercache.spi.DistributedLock;
import io.tiercache.spi.DistributedLockProvider;

import java.time.Duration;

/**
 * {@link DistributedLockProvider} guarded by the L2 circuit breaker: fails
 * fast when open (returns no lock — coordination degrades to per-instance
 * coalescing), accounts outcomes otherwise.
 */
public final class BreakerLockProvider implements DistributedLockProvider {

    private final DistributedLockProvider delegate;
    private final CircuitBreaker breaker;

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
