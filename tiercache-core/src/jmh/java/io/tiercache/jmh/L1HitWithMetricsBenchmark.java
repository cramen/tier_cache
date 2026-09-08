package io.tiercache.jmh;

import io.tiercache.TierCache;
import io.tiercache.TierCacheFactory;
import io.tiercache.spi.CacheMetricsListener;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

/**
 * L1-hit hot path with metrics emission enabled — the emission overhead
 * check (must stay allocation-free and near the no-listener baseline).
 */
@State(Scope.Thread)
public class L1HitWithMetricsBenchmark {

    /** A cheap recording listener (what a real binder does per event). */
    private static final class CountingListener implements CacheMetricsListener {
        long count;

        @Override
        public void onRequest(String cache, Outcome outcome) {
            count++;
        }
    }

    private TierCache<String, String> cache;

    @Setup
    public void setup() {
        cache = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<String, String>())
                .metricsListener(new CountingListener())
                .build()
                .getCache("bench");
        cache.put("hot", "value");
    }

    @Benchmark
    public void l1Hit(Blackhole bh) {
        bh.consume(cache.get("hot"));
    }
}
