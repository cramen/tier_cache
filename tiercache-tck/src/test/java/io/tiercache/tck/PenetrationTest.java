package io.tiercache.tck;

import io.tiercache.CacheSettings;
import io.tiercache.NullPolicy;
import io.tiercache.TierCache;
import io.tiercache.TierCacheFactory;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cache penetration. Repeated requests for nonexistent keys must be
 * absorbed by null-markers (allow policy): the loader sees at most a tiny
 * fraction of the traffic.
 */
class PenetrationTest {

    private static final int DISTINCT_KEYS = 100;
    private static final int REQUESTS = 100_000;

    @Test
    void nullMarkersAbsorbPenetrationTraffic() {
        AtomicInteger loaderCalls = new AtomicInteger();
        TierCache<String, String> cache = TierCacheFactory.builder()
                .defaults(new CacheSettings(DISTINCT_KEYS * 2, Duration.ofMinutes(5), null,
                        Duration.ofHours(1), 0.10, NullPolicy.allow(Duration.ofSeconds(60))))
                .remoteCache(new InMemoryRemoteCache<>())
                .build()
                .getCache("penetration");

        for (int i = 0; i < REQUESTS; i++) {
            String value = cache.getOrCompute("nonexistent-" + (i % DISTINCT_KEYS), key -> {
                loaderCalls.incrementAndGet();
                return null; // key genuinely does not exist
            });
            assertNull(value);
        }

        // Penetration acceptance: loader load <= 1/600 of traffic. We expect ~one
        // call per distinct key per marker window.
        assertTrue(loaderCalls.get() <= REQUESTS / 600,
                () -> "loader calls " + loaderCalls.get() + " exceed 1/600 of " + REQUESTS);
    }
}
