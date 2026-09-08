package io.tiercache.micrometer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.tiercache.TierCacheFactory;
import io.tiercache.spi.CacheMetricsListener;
import io.tiercache.spi.DegradationListener;
import io.tiercache.spi.InvalidationListener;
import io.tiercache.spi.InvalidationJournal;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Binds core metrics events to a Micrometer {@link MeterRegistry}.
 *
 * <p>Metric set: {@code tiercache.requests{cache,result}},
 * {@code tiercache.latency{cache,level}},
 * {@code tiercache.invalidation{cache,direction}}, {@code tiercache.degraded},
 * {@code tiercache.breaker.state}, {@code tiercache.journal.size},
 * {@code tiercache.entry.age.max}, {@code tiercache.null.entries}.
 *
 * <p>Entry age is approximate: tracked from store events, not per-entry
 * metadata.
 */
public final class MicrometerCacheMetrics
        implements CacheMetricsListener, DegradationListener, InvalidationListener {

    private final MeterRegistry registry;
    private final Map<String, Counter> requestCounters = new ConcurrentHashMap<>();
    private final Map<String, Timer> latencyTimers = new ConcurrentHashMap<>();
    private final Map<String, Counter> invalidationCounters = new ConcurrentHashMap<>();
    private final Map<String, Counter> nullEntries = new ConcurrentHashMap<>();
    private final Map<String, Long> lastStoreNanos = new ConcurrentHashMap<>();

    public MicrometerCacheMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    // --- CacheMetricsListener ---

    @Override
    public void onRequest(String cache, Outcome outcome) {
        counter(requestCounters, cache, "requests", "result",
                outcome.name().toLowerCase()).increment();
        if (outcome == Outcome.LOAD) {
            lastStoreNanos.put(cache, System.nanoTime());
        }
    }

    @Override
    public void onLatency(String cache, Level level, long nanos) {
        latencyTimers.computeIfAbsent(cache + ":" + level, k -> Timer.builder("tiercache.latency")
                        .tags("cache", cache, "level", level.name().toLowerCase())
                        .register(registry))
                .record(nanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public void onInvalidation(String cache, Direction direction) {
        counter(invalidationCounters, cache, "invalidation", "direction",
                direction.name().toLowerCase()).increment();
    }

    @Override
    public void onNullEntry(String cache) {
        nullEntries.computeIfAbsent(cache, c -> Counter.builder("tiercache.null.entries")
                        .tags("cache", c)
                        .register(registry))
                .increment();
    }

    private Counter counter(Map<String, Counter> map, String cache, String name,
            String tagKey, String tagValue) {
        return map.computeIfAbsent(cache + ":" + tagValue, k -> Counter.builder("tiercache." + name)
                .tags("cache", cache, tagKey, tagValue)
                .register(registry));
    }

    // --- DegradationListener ---

    @Override
    public void onDegraded() {
        // Gauges read the factory state; nothing to count here.
    }

    @Override
    public void onRecovered() {
    }

    // --- InvalidationListener ---

    @Override
    public void onJournalOverflow(String cache) {
        onInvalidation(cache, Direction.DROPPED);
    }

    /**
     * Registers factory-level gauges: degraded, breaker state, journal size,
     * max entry age (approximate). Call once per factory.
     */
    public void registerGauges(TierCacheFactory factory, InvalidationJournal journal,
            List<String> cacheNames) {
        Gauge.builder("tiercache.degraded", factory, f -> f.isDegraded() ? 1 : 0)
                .register(registry);
        Gauge.builder("tiercache.breaker.state", factory, f -> f.isDegraded() ? 1 : 0)
                .register(registry);
        for (String cache : cacheNames) {
            Gauge.builder("tiercache.journal.size", journal,
                            j -> j != null ? j.size(cache) : 0)
                    .tags("cache", cache)
                    .register(registry);
            Gauge.builder("tiercache.entry.age.max", cache,
                            c -> ageMillis(c))
                    .tags("cache", cache)
                    .register(registry);
        }
    }

    private double ageMillis(String cache) {
        Long last = lastStoreNanos.get(cache);
        if (last == null) {
            return 0;
        }
        return (System.nanoTime() - last) / 1_000_000.0;
    }
}
