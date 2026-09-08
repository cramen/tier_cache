package io.tiercache.internal;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * TTL jitter. Jitter only shortens TTLs — the result lies in
 * {@code [base * (1 - amplitude), base]} — which keeps the runtime TTL
 * ordering guarantee (L1 never outlives L2) intact by construction.
 */
public final class TtlJitter {

    private TtlJitter() {
    }

    /**
     * Returns {@code base} shortened by a random fraction in
     * {@code [0, amplitude]} of {@code base}.
     */
    public static Duration apply(Duration base, double amplitude) {
        if (amplitude <= 0.0) {
            return base;
        }
        double factor = 1.0 - ThreadLocalRandom.current().nextDouble(amplitude);
        return Duration.ofNanos((long) (base.toNanos() * factor));
    }
}
