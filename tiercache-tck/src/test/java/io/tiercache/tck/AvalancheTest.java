package io.tiercache.tck;

import io.tiercache.InvalidationMode;

import io.tiercache.CacheSettings;
import io.tiercache.TierCache;
import io.tiercache.TierCacheFactory;
import io.tiercache.NullPolicy;
import io.tiercache.testkit.InMemoryRemoteCache;
import io.tiercache.testkit.RecordingLocalCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cache avalanche. Mass write with an identical base TTL must produce
 * effective L1 TTLs uniformly spread over the jitter band, so entries do
 * not expire simultaneously.
 *
 * <p>Checked on assigned TTLs (not wall-clock expiry): uniformity of the
 * TTL-jitter assignment is the actual defense.
 */
class AvalancheTest {

    private static final int WRITES = 10_000;
    private static final int BINS = 10;
    /** Chi-square critical value, df = BINS-1, p = 0.001. */
    private static final double CHI_SQUARE_CRITICAL = 27.88;

    @Test
    void defaultJitterSpreadsExpiryUniformly() {
        double amplitude = 0.10;
        RecordingLocalCache<String, String> recorder = new RecordingLocalCache<>();
        TierCache<String, String> cache = TierCacheFactory.builder()
                .defaults(new CacheSettings(WRITES * 2, Duration.ofHours(1), null,
                        Duration.ofHours(2), amplitude, NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024))
                .remoteCache(new InMemoryRemoteCache<>())
                .localCacheFactory((name, settings) -> recorder)
                .build()
                .getCache("avalanche");

        for (int i = 0; i < WRITES; i++) {
            cache.put("k" + i, "v");
        }

        double chiSquare = chiSquareUniformity(recorder.recordedTtls(), Duration.ofHours(1), amplitude);
        assertTrue(chiSquare < CHI_SQUARE_CRITICAL,
                () -> "expiry distribution not uniform enough: chi2=" + chiSquare
                        + " (critical " + CHI_SQUARE_CRITICAL + ")");
    }

    @Test
    void harnessDetectsMissingJitter() {
        // Sensitivity check: with amplitude 0 the same measurement must FAIL
        // the uniformity criterion (all TTLs identical -> degenerate).
        RecordingLocalCache<String, String> recorder = new RecordingLocalCache<>();
        TierCache<String, String> cache = TierCacheFactory.builder()
                .defaults(new CacheSettings(WRITES * 2, Duration.ofHours(1), null,
                        Duration.ofHours(2), 0.0, NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024))
                .remoteCache(new InMemoryRemoteCache<>())
                .localCacheFactory((name, settings) -> recorder)
                .build()
                .getCache("avalanche-flat");

        for (int i = 0; i < WRITES; i++) {
            cache.put("k" + i, "v");
        }

        double chiSquare = chiSquareUniformity(recorder.recordedTtls(), Duration.ofHours(1), 0.10);
        assertFalse(chiSquare < CHI_SQUARE_CRITICAL,
                () -> "harness must reject a degenerate distribution, chi2=" + chiSquare);
    }

    /**
     * Chi-square statistic of the TTL sample against the uniform
     * distribution over {@code [base * (1 - amplitude), base]}.
     */
    private static double chiSquareUniformity(List<Duration> ttls, Duration base, double amplitude) {
        long baseNanos = base.toNanos();
        long lowNanos = (long) (baseNanos * (1.0 - amplitude));
        double expected = (double) ttls.size() / BINS;
        long[] observed = new long[BINS];
        for (Duration ttl : ttls) {
            long nanos = ttl.toNanos();
            int bin = (int) Math.min(BINS - 1,
                    Math.max(0, (nanos - lowNanos) * BINS / (baseNanos - lowNanos)));
            observed[bin]++;
        }
        double chi2 = 0.0;
        for (long o : observed) {
            chi2 += (o - expected) * (o - expected) / expected;
        }
        return chi2;
    }
}
