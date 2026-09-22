package io.tiercache;

import io.tiercache.internal.CircuitBreaker;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;

class BreakerCallbackMonitorTest {
    @Test void transitionCallbacksRunOutsideBreakerMonitor() {
        var owner = new AtomicReference<CircuitBreaker>();
        var held = new java.util.ArrayList<Boolean>();
        var breaker = new CircuitBreaker(new CircuitBreaker.Config(10, 0.5, 1, Duration.ZERO, 1),
                new CircuitBreaker.Listener() {
                    public void onOpen() { held.add(Thread.holdsLock(owner.get())); }
                    public void onClose() { held.add(Thread.holdsLock(owner.get())); }
                });
        owner.set(breaker); breaker.onFailure();
        assertTrue(breaker.tryAcquire()); breaker.onSuccess();
        assertEquals(java.util.List.of(false, false), held);
    }
}
