package io.tiercache;

import io.tiercache.internal.CircuitBreaker;
import io.tiercache.internal.CircuitBreakerRemoteCache;
import io.tiercache.internal.L2UnavailableException;
import io.tiercache.testkit.FailingRemoteCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CircuitBreakerRemoteCacheTest {

    @Test
    void openThenProbeCloses() throws Exception {
        CircuitBreaker.Config cfg = new CircuitBreaker.Config(10, 0.5, 2, Duration.ofMillis(50), 1);
        List<String> events = new ArrayList<>();
        CircuitBreaker breaker = new CircuitBreaker(cfg, new CircuitBreaker.Listener() {
            public void onOpen() { events.add("open"); }
            public void onClose() { events.add("close"); }
        });
        FailingRemoteCache<String, String> failing = new FailingRemoteCache<>();
        CircuitBreakerRemoteCache<String, String> l2 = new CircuitBreakerRemoteCache<>(failing, breaker);

        failing.fail();
        assertThrows(L2UnavailableException.class, () -> l2.get("a"));
        assertThrows(L2UnavailableException.class, () -> l2.get("b"));
        assertTrue(breaker.isOpen());
        failing.heal();
        Thread.sleep(80);
        assertNull(l2.get("a")); // probe succeeds
        assertFalse(breaker.isOpen());
        assertEquals(List.of("open", "close"), events);
    }
}
