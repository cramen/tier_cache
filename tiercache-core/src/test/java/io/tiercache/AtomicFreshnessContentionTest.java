package io.tiercache;

import io.tiercache.spi.CacheMetricsListener;
import io.tiercache.testkit.FreshHitRemoteCache;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class AtomicFreshnessContentionTest {
    @ParameterizedTest @ValueSource(booleans = {false, true})
    void freshReadersNeverCreateMissesOrCascade(boolean marker) throws Exception {
        var remote = new FreshHitRemoteCache<String, String>();
        var unexpected = new AtomicInteger();
        var d = CacheSettings.defaults();
        var settings = new CacheSettings(100, Duration.ofHours(1), Duration.ofMinutes(30), Duration.ofHours(3),
                0, NullPolicy.allow(Duration.ofMinutes(30)), d.invalidationMode(), d.payloadCapBytes(),
                Duration.ZERO, false, d.xfetchBeta(), Duration.ofMinutes(5));
        var metrics = new CacheMetricsListener() {
            public void onRequest(String cache, Outcome result) {
                if (result != Outcome.L1_HIT) unexpected.incrementAndGet();
            }
        };
        try (var factory = TierCacheFactory.builder().defaults(settings).remoteCache(remote).metricsListener(metrics).build()) {
            TierCache<String, String> cache = factory.getCache("contention");
            if (marker) cache.putNull("hot"); else cache.put("hot", "value");
            remote.resetAttribution();
            var pool = Executors.newFixedThreadPool(16);
            var ready = new CountDownLatch(16); var start = new CountDownLatch(1);
            try {
                var results = new ArrayList<Future<?>>();
                for (int t = 0; t < 16; t++) results.add(pool.submit(() -> {
                    ready.countDown();
                    try { assertTrue(start.await(5, TimeUnit.SECONDS)); }
                    catch (InterruptedException e) { throw new AssertionError(e); }
                    for (int i = 0; i < 5000; i++) {
                        assertEquals(marker ? null : "value", cache.get("hot"));
                        assertEquals(marker ? LookupResult.cachedNull() : LookupResult.hit("value"), cache.lookup("hot"));
                        assertEquals(marker ? null : "value", cache.getOrCompute("hot", k -> {
                            remote.recordLoad(); throw new AssertionError("Unexpected loader");
                        }));
                    }
                }));
                assertTrue(ready.await(5, TimeUnit.SECONDS)); start.countDown();
                for (var result : results) result.get(15, TimeUnit.SECONDS);
                remote.assertNoCalls(); assertEquals(0, unexpected.get());
            } finally { start.countDown(); pool.shutdownNow(); assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS)); }
        }
    }
}
