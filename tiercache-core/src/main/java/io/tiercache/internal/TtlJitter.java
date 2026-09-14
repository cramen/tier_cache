package io.tiercache.internal;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.random.RandomGenerator;

/**
 * TTL jitter. Jitter only shortens TTLs — the result lies in
 * {@code [base * (1 - amplitude), base]} — which keeps the runtime TTL
 * ordering guarantee (L1 never outlives L2) intact by construction.
 *
 * <p><b>Internal — not part of the supported API.</b>
 *
 * @since 0.1.0
 */
public final class TtlJitter {

    private final Supplier<RandomGenerator> random;

    /**
     * Production wiring: draws from {@link ThreadLocalRandom}.
     *
     * @since 0.1.0
     */
    public TtlJitter() {
        this.random = ThreadLocalRandom::current;
    }

    /**
     * Test seam: deterministic draws from the given source. Not for
     * production wiring.
     *
     * @param random the deterministic random source; must not be {@code null}
     * @since 0.1.0
     */
    public TtlJitter(RandomGenerator random) {
        Objects.requireNonNull(random, "random");
        this.random = () -> random;
    }

    /**
     * Returns {@code base} shortened by a random fraction in
     * {@code [0, amplitude]} of {@code base}.
     *
     * @param base      the TTL before jitter
     * @param amplitude jitter amplitude as a fraction in [0, 1)
     * @return the jittered TTL; never longer than {@code base}
     * @since 0.1.0
     */
    public Duration apply(Duration base, double amplitude) {
        if (amplitude <= 0.0) {
            return base;
        }
        double factor = 1.0 - random.get().nextDouble(amplitude);
        return Duration.ofNanos((long) (base.toNanos() * factor));
    }
}
