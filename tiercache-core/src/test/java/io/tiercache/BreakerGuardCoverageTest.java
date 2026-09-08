package io.tiercache;

import io.tiercache.internal.CircuitBreaker;
import io.tiercache.internal.CircuitBreakerRemoteCache;
import io.tiercache.internal.BreakerLockProvider;
import io.tiercache.internal.DefaultTierCache;
import io.tiercache.internal.L2UnavailableException;
import io.tiercache.spi.StoredEntry;
import io.tiercache.testkit.CountingLocalCache;
import io.tiercache.testkit.CountingRemoteCache;
import io.tiercache.testkit.InMemoryLockProvider;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Branch coverage for breaker guards and versioned paths. */
class BreakerGuardCoverageTest {

    private static CircuitBreaker openBreaker() {
        CircuitBreaker breaker = new CircuitBreaker(
                new CircuitBreaker.Config(10, 0.5, 2, Duration.ofMinutes(1), 1),
                new CircuitBreaker.Listener() {
                    @Override
                    public void onOpen() {
                    }

                    @Override
                    public void onClose() {
                    }
                });
        breaker.onFailure();
        breaker.onFailure();
        return breaker;
    }

    @Test
    void decoratorFailsFastWhenOpen() {
        CircuitBreakerRemoteCache<String, String> l2 = new CircuitBreakerRemoteCache<>(
                new InMemoryRemoteCache<>(), openBreaker());
        assertThrows(L2UnavailableException.class, () -> l2.get("k"));
    }

    @Test
    void lockProviderReturnsNullWhenOpen() {
        assertNull(new BreakerLockProvider(new InMemoryLockProvider(), openBreaker())
                .tryLock("l", Duration.ofMinutes(1)));
    }

    @Test
    void breakerEdgeBranches() throws InterruptedException {
        // Invalid config combos.
        assertThrows(IllegalArgumentException.class,
                () -> new CircuitBreaker.Config(10, 0.5, 2, Duration.ofSeconds(1), 0));
        assertThrows(IllegalArgumentException.class,
                () -> new CircuitBreaker.Config(10, 1.5, 2, Duration.ofSeconds(1), 1));

        // ratio 0.75 over a 3-call window: threshold = 3 failures.
        CircuitBreaker breaker = new CircuitBreaker(
                new CircuitBreaker.Config(3, 0.75, 2, Duration.ofMillis(50), 1),
                new CircuitBreaker.Listener() {
                    @Override
                    public void onOpen() {
                    }

                    @Override
                    public void onClose() {
                    }
                });
        // Window full [S,S,F], one failure -> below threshold.
        breaker.onSuccess();
        breaker.onSuccess();
        breaker.onFailure();
        // Overwrite a success with a failure: 2 failures -> still closed.
        breaker.onFailure();
        assertFalse(breaker.isOpen());
        // Third concurrent failure -> open.
        breaker.onFailure();
        assertTrue(breaker.isOpen());
        // Fail while open: re-open resets the wait, stays open.
        breaker.onFailure();
        assertTrue(breaker.isOpen());

        // Half-open probe budget: only probesToClose probes at a time.
        Thread.sleep(80);
        assertTrue(breaker.tryAcquire());
        assertFalse(breaker.tryAcquire(), "probe budget exhausted");
    }

    @Test
    void markerInL1ReadsAsNull() {
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                l1, new CountingRemoteCache<>(), CacheSettings.defaults(), true,
                null, null, new VersionGenerator(), null);
        l1.put("k", StoredEntry.nullMarker(new Version(1, UUID.randomUUID())), Duration.ofMinutes(1));
        assertNull(cache.get("k"));
        assertNull(cache.getOrCompute("k", key -> {
            throw new AssertionError();
        }));
    }

    @Test
    void updateModePublishesPayloadThroughHandler() {
        List<Object> updates = new ArrayList<>();
        io.tiercache.spi.InvalidationHandler handler = new io.tiercache.spi.InvalidationHandler() {
            @Override
            public void onLocalWrite(String cache, Object key, Version version,
                    InvalidationMessage.Type type) {
            }

            @Override
            public void onLocalUpdate(String cache, Object key, Object value, Version version) {
                updates.add(value);
            }

            @Override
            public void registerTarget(String cache, io.tiercache.spi.InvalidationTarget target) {
            }

            @Override
            public void close() {
            }
        };
        TierCacheFactory factory = TierCacheFactory.builder()
                .cache("upd", new CacheOverride().invalidationMode(InvalidationMode.UPDATE))
                .remoteCache(new InMemoryRemoteCache<>())
                .invalidation(versions -> handler)
                .build();
        factory.getCache("upd").put("k", "v");
        assertEquals(List.of("v"), updates);

        // And the receiving side applies/drops by version.
        TierCache<String, String> receiver = factory.getCache("upd");
        ((DefaultTierCache<String, String>) receiver).applyUpdateL1("k", "newer",
                new Version(100, UUID.randomUUID()));
        assertEquals("newer", receiver.get("k"));
        ((DefaultTierCache<String, String>) receiver).applyUpdateL1("k", "stale",
                new Version(1, UUID.randomUUID()));
        assertEquals("newer", receiver.get("k"), "stale update dropped");
        factory.close();
    }
}
