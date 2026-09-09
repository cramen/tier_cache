package io.tiercache;

import io.tiercache.internal.CircuitBreaker;
import io.tiercache.internal.CircuitBreakerRemoteCache;
import io.tiercache.internal.L2UnavailableException;
import io.tiercache.spi.StoredEntry;
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

    @Test
    void putIfNewerPropagatesDelegateResult() {
        CircuitBreaker breaker = newBreaker();
        boolean[] delegateResult = {false};
        io.tiercache.spi.RemoteCache<String, String> stub = stubRemoteCache(delegateResult);
        CircuitBreakerRemoteCache<String, String> l2 = new CircuitBreakerRemoteCache<>(stub, breaker);

        assertFalse(l2.putIfNewer("k", StoredEntry.ofValue("v"), Duration.ofMinutes(1)),
                "a lost version race must surface as false");
        delegateResult[0] = true;
        assertTrue(l2.putIfNewer("k", StoredEntry.ofValue("v"), Duration.ofMinutes(1)));
    }

    @Test
    void clearReachesDelegate() {
        CircuitBreaker breaker = newBreaker();
        java.util.concurrent.atomic.AtomicInteger clears = new java.util.concurrent.atomic.AtomicInteger();
        CircuitBreakerRemoteCache<String, String> l2 = new CircuitBreakerRemoteCache<>(
                new ClearCountingRemoteCache(clears), breaker);
        l2.clear();
        assertEquals(1, clears.get(), "clear must delegate");
    }

    private static CircuitBreaker newBreaker() {
        return new CircuitBreaker(new CircuitBreaker.Config(10, 0.5, 2, Duration.ofMillis(50), 1),
                new CircuitBreaker.Listener() {
                    public void onOpen() { }
                    public void onClose() { }
                });
    }

    private static io.tiercache.spi.RemoteCache<String, String> stubRemoteCache(boolean[] putIfNewerResult) {
        return new io.tiercache.spi.RemoteCache<>() {
            @Override
            public StoredEntry<String> get(String key) {
                return null;
            }

            @Override
            public void put(String key, StoredEntry<String> entry, Duration ttl) {
            }

            @Override
            public boolean putIfNewer(String key, StoredEntry<String> entry, Duration ttl) {
                return putIfNewerResult[0];
            }

            @Override
            public void evict(String key) {
            }

            @Override
            public void clear() {
            }

            @Override
            public boolean setIfAbsent(String key, StoredEntry<String> entry, Duration ttl) {
                return true;
            }
        };
    }

    private static final class ClearCountingRemoteCache implements io.tiercache.spi.RemoteCache<String, String> {
        private final java.util.concurrent.atomic.AtomicInteger clears;

        ClearCountingRemoteCache(java.util.concurrent.atomic.AtomicInteger clears) {
            this.clears = clears;
        }

        @Override
        public StoredEntry<String> get(String key) {
            return null;
        }

        @Override
        public void put(String key, StoredEntry<String> entry, Duration ttl) {
        }

        @Override
        public void evict(String key) {
        }

        @Override
        public void clear() {
            clears.incrementAndGet();
        }

        @Override
        public boolean setIfAbsent(String key, StoredEntry<String> entry, Duration ttl) {
            return true;
        }
    }
}
