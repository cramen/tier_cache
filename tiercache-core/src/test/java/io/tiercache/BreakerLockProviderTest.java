package io.tiercache.internal;

import io.tiercache.spi.DistributedLock;
import io.tiercache.spi.DistributedLockProvider;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Breaker-guarded lock provider: fail-fast when open, outcome accounting otherwise. */
class BreakerLockProviderTest {

    /** Provider that fails a configurable number of times, then hands out no-op locks. */
    private static final class FlakyLockProvider implements DistributedLockProvider {
        final AtomicInteger calls = new AtomicInteger();
        int failuresLeft;

        @Override
        public DistributedLock tryLock(String name, Duration lease) {
            calls.incrementAndGet();
            if (failuresLeft > 0) {
                failuresLeft--;
                throw new L2UnavailableException("simulated lock failure");
            }
            return new DistributedLock() {
                @Override
                public boolean extend(Duration lease) {
                    return true;
                }

                @Override
                public void release() {
                }
            };
        }
    }

    @Test
    void openBreakerFailsFastWithoutTouchingTheDelegate() {
        CircuitBreaker breaker = new CircuitBreaker(
                new CircuitBreaker.Config(10, 0.5, 2, Duration.ofMinutes(1), 1),
                new CircuitBreaker.Listener() {
                    public void onOpen() { }
                    public void onClose() { }
                });
        breaker.onFailure();
        breaker.onFailure();
        assertTrue(breaker.isOpen());
        FlakyLockProvider delegate = new FlakyLockProvider();
        BreakerLockProvider provider = new BreakerLockProvider(delegate, breaker);

        assertNull(provider.tryLock("n", Duration.ofSeconds(1)), "no lock while open");
        org.junit.jupiter.api.Assertions.assertEquals(0, delegate.calls.get(),
                "the delegate must not be called while the breaker is open");
    }

    @Test
    void successfulProbeIsAccountedAndClosesTheBreaker() throws InterruptedException {
        CircuitBreaker breaker = new CircuitBreaker(
                new CircuitBreaker.Config(10, 0.5, 2, Duration.ofMillis(50), 1),
                new CircuitBreaker.Listener() {
                    public void onOpen() { }
                    public void onClose() { }
                });
        FlakyLockProvider delegate = new FlakyLockProvider();
        delegate.failuresLeft = 2;
        BreakerLockProvider provider = new BreakerLockProvider(delegate, breaker);

        assertNull(provider.tryLock("n", Duration.ofSeconds(1)));
        assertNull(provider.tryLock("n", Duration.ofSeconds(1)));
        assertTrue(breaker.isOpen());

        Thread.sleep(80); // past halfOpenAfter
        assertNotNull(provider.tryLock("n", Duration.ofSeconds(1)), "probe acquires the lock");
        assertNotNull(provider.tryLock("n", Duration.ofSeconds(1)),
                "the successful probe must close the breaker; a second acquisition succeeds");
    }
}
