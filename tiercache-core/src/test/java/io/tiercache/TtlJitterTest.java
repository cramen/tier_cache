package io.tiercache;

import io.tiercache.internal.TtlJitter;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spec: cache-configuration — TTL ordering guarantee at runtime.
 */
class TtlJitterTest {

    @Test
    void zeroAmplitudeLeavesTtlUntouched() {
        Duration base = Duration.ofMinutes(10);
        assertEquals(base, new TtlJitter().apply(base, 0.0));
        assertEquals(base, new TtlJitter(new SplittableRandom(1L)).apply(base, 0.0));
    }

    @Test
    void jitteredTtlNeverExceedsBaseAcrossLargeSample() {
        // With jitter only shortening TTLs, l1 base <= l2Ttl implies the
        // effective L1 TTL never exceeds the L2 TTL.
        Duration base = Duration.ofMinutes(10);
        Duration l2Ttl = Duration.ofMinutes(10);
        double amplitude = 0.10;
        long lowerBoundNanos = (long) (base.toNanos() * (1.0 - amplitude));
        TtlJitter jitter = new TtlJitter();
        for (int i = 0; i < 100_000; i++) {
            Duration jittered = jitter.apply(base, amplitude);
            assertTrue(jittered.compareTo(l2Ttl) <= 0,
                    () -> "jittered TTL " + jittered + " exceeded L2 TTL " + l2Ttl);
            assertTrue(jittered.toNanos() >= lowerBoundNanos,
                    () -> "jittered TTL " + jittered + " below jitter amplitude bound");
        }
    }

    @Test
    void jitterActuallyVaries() {
        Duration base = Duration.ofMinutes(10);
        TtlJitter jitter = new TtlJitter();
        Set<Duration> observed = new HashSet<>();
        for (int i = 0; i < 1_000; i++) {
            observed.add(jitter.apply(base, 0.10));
        }
        assertFalse(observed.size() < 10, "jitter produced suspiciously few distinct TTLs");
    }

    @Test
    void seededSourceProducesDeterministicSequence() {
        Duration base = Duration.ofMinutes(10);
        TtlJitter first = new TtlJitter(new SplittableRandom(42L));
        TtlJitter second = new TtlJitter(new SplittableRandom(42L));
        for (int i = 0; i < 1_000; i++) {
            assertEquals(first.apply(base, 0.10), second.apply(base, 0.10));
        }
    }

    @Test
    void seededSourceRespectsJitterBand() {
        Duration base = Duration.ofMinutes(10);
        double amplitude = 0.10;
        long lowerBoundNanos = (long) (base.toNanos() * (1.0 - amplitude));
        TtlJitter jitter = new TtlJitter(new SplittableRandom(42L));
        for (int i = 0; i < 1_000; i++) {
            Duration jittered = jitter.apply(base, amplitude);
            assertTrue(jittered.toNanos() >= lowerBoundNanos
                    && jittered.compareTo(base) <= 0,
                    () -> "jittered TTL " + jittered + " outside the jitter band");
        }
    }

    @Test
    void seededSourceRejectsNull() {
        assertThrows(NullPointerException.class, () -> new TtlJitter(null));
    }
}
