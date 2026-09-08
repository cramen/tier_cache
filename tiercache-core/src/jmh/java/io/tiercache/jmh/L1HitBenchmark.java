package io.tiercache.jmh;

import io.tiercache.CacheSettings;
import io.tiercache.TierCache;
import io.tiercache.TierCacheFactory;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Baseline for the L1-hit hot path (N-01 overhead, N-03 allocations).
 * Run short mode: ./gradlew :tiercache-core:jmh -Pjmh.iterations=1 ...
 * or via the {@code jmh} task defaults configured for CI.
 */
@State(Scope.Thread)
public class L1HitBenchmark {

    private TierCache<String, String> cache;

    @Setup
    public void setup() {
        cache = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<String, String>())
                .defaults(CacheSettings.defaults())
                .build()
                .getCache("bench");
        cache.put("hot", "value");
    }

    @Benchmark
    public void l1Hit(Blackhole bh) {
        bh.consume(cache.get("hot"));
    }
}
