package io.tiercache;

import io.tiercache.internal.TtlJitter;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spec: cache-configuration — TTL ordering guarantee at runtime (F-05/F-24).
 */
class TtlJitterTest {

    @Test
    void zeroAmplitudeLeavesTtlUntouched() {
        Duration base = Duration.ofMinutes(10);
        assertEquals(base, TtlJitter.apply(base, 0.0));
    }

    @Test
    void jitteredTtlNeverExceedsBaseAcrossLargeSample() {
        // F-05: with jitter only shortening TTLs, l1 base <= l2Ttl implies the
        // effective L1 TTL never exceeds the L2 TTL.
        Duration base = Duration.ofMinutes(10);
        Duration l2Ttl = Duration.ofMinutes(10);
        double amplitude = 0.10;
        long lowerBoundNanos = (long) (base.toNanos() * (1.0 - amplitude));
        for (int i = 0; i < 100_000; i++) {
            Duration jittered = TtlJitter.apply(base, amplitude);
            assertTrue(jittered.compareTo(l2Ttl) <= 0,
                    () -> "jittered TTL " + jittered + " exceeded L2 TTL " + l2Ttl);
            assertTrue(jittered.toNanos() >= lowerBoundNanos,
                    () -> "jittered TTL " + jittered + " below jitter amplitude bound");
        }
    }

    @Test
    void jitterActuallyVaries() {
        Duration base = Duration.ofMinutes(10);
        Set<Duration> observed = new HashSet<>();
        for (int i = 0; i < 1_000; i++) {
            observed.add(TtlJitter.apply(base, 0.10));
        }
        assertFalse(observed.size() < 10, "jitter produced suspiciously few distinct TTLs");
    }
}
