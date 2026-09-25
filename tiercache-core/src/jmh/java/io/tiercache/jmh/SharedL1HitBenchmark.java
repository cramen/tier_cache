package io.tiercache.jmh;

import io.tiercache.*;
import io.tiercache.spi.InvalidationTarget;
import io.tiercache.spi.LocalCache;
import io.tiercache.spi.StoredEntry;
import io.tiercache.internal.CaffeineLocalCache;
import io.tiercache.testkit.FreshHitRemoteCache;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.ThreadParams;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/** Shared-instance contention diagnostic; run separately from the thread-local baseline. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(3)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
public class SharedL1HitBenchmark {
    @State(Scope.Benchmark)
    public static class CacheState {
        @Param({"plain", "plainAccess", "stale", "staleAccess"}) public String feature;
        @Param({"hot", "distributed", "skewed", "stripe"}) public String keys;
        @Param({"read"}) public String workload;
        @Param({"value"}) public String valueKind;
        @Param({"get"}) public String api;
        @Param({"builtin"}) public String provider;
        public TierCacheFactory factory;
        public TierCache<Integer, String> cache;
        public final Integer[] keySet = new Integer[64];
        public final FreshHitRemoteCache<Integer, String> remote = new FreshHitRemoteCache<>();
        public final java.util.function.Function<Integer, String> loader = key -> {
            remote.recordLoad();
            return "loaded";
        };

        @Setup(Level.Trial)
        public void setup() {
            CacheSettings d = CacheSettings.defaults();
            CacheSettings settings = new CacheSettings(10000, Duration.ofHours(1),
                    feature.endsWith("Access") ? Duration.ofMinutes(30) : null,
                    Duration.ofHours(3), 0, NullPolicy.allow(Duration.ofMinutes(30)),
                    d.invalidationMode(), d.payloadCapBytes(), Duration.ZERO, false,
                    d.xfetchBeta(), feature.startsWith("stale") ? Duration.ofMinutes(5) : Duration.ZERO);
            var builder = TierCacheFactory.builder().defaults(settings).remoteCache(remote);
            if (provider.equals("fallback")) {
                builder.localCacheFactory((name, configured) -> new LegacyProvider<>(configured));
            }
            factory = builder.build();
            cache = factory.getCache("shared-l1");
            for (int i = 0; i < keySet.length; i++) {
                // Identical low six bits in engine stripes, distinct CHM spread hashes.
                keySet[i] = keys.equals("stripe") ? (i << 16) + (i << 6) : i;
                if (valueKind.equals("null")) cache.putNull(keySet[i]);
                else cache.put(keySet[i], "value");
            }
            remote.resetAttribution();
            if (Boolean.getBoolean("tiercache.benchmark.injectL2Call")) remote.get(keySet[0]);
        }

        @TearDown(Level.Iteration)
        public void verifyAttribution() {
            if (workload.equals("read")) remote.assertNoCalls();
        }
        @TearDown(Level.Trial)
        public void close() { factory.close(); }
    }

    /** Same fixture but private caches: a control, not evidence of shared scaling. */
    @State(Scope.Thread)
    public static class PrivateCacheState extends CacheState { }

    @State(Scope.Thread)
    public static class Caller {
        int random;
        long operations;
        @Setup public void setup(ThreadParams params) { random = params.getThreadIndex() + 1; }
        int next() {
            int x = random;
            x ^= x << 13;
            x ^= x >>> 17;
            x ^= x << 5;
            random = x;
            return x;
        }
    }

    @Benchmark public Object shared(CacheState state, Caller caller) { return read(state, caller); }
    @Benchmark public Object threadLocal(PrivateCacheState state, Caller caller) { return read(state, caller); }

    private static Object read(CacheState state, Caller caller) {
        int sample = caller.next();
        int index = switch (state.keys) {
            case "hot" -> 0;
            case "skewed" -> (sample & 1023) < 922 ? 0 : (sample >>> 10) & 63;
            default -> sample & 63;
        };
        Integer key = state.keySet[index];
        long operation = ++caller.operations;
        // Mixed controls explicitly permit cascade work and are not pure-hit numbers.
        if (state.workload.equals("write") && (operation & 127) == 0) cacheWrite(state, key);
        if ((operation & 8191) == 0) {
            InvalidationTarget target = (InvalidationTarget) state.cache;
            if (state.workload.equals("clear")) target.evictAllL1();
            if (state.workload.equals("recovery")) target.resetRecovery(target.recoveryGeneration());
        }
        return switch (state.api) {
            case "lookup" -> state.cache.lookup(key);
            case "compute" -> state.cache.getOrCompute(key, state.loader);
            default -> state.cache.get(key);
        };
    }

    /** Deliberately implements only the pre-atomic-freshness SPI, preserving fallback selection. */
    private static final class LegacyProvider<K, V> implements LocalCache<K, V> {
        private final CaffeineLocalCache<K, V> delegate;
        LegacyProvider(CacheSettings settings) { delegate = new CaffeineLocalCache<>(settings); }
        public StoredEntry<V> get(K key) { return delegate.get(key); }
        public void put(K key, StoredEntry<V> value, Duration ttl) { delegate.put(key, value, ttl); }
        public void evict(K key) { delegate.evict(key); }
        public void clear() { delegate.clear(); }
        public boolean setIfAbsent(K key, StoredEntry<V> value, Duration ttl) {
            return delegate.setIfAbsent(key, value, ttl);
        }
        public boolean supportsAtomicReplace() { return true; }
        public boolean replaceIfSame(K key, StoredEntry<V> expected, StoredEntry<V> value, Duration ttl) {
            return delegate.replaceIfSame(key, expected, value, ttl);
        }
    }

    private static void cacheWrite(CacheState state, Integer key) {
        if (state.valueKind.equals("null")) state.cache.putNull(key);
        else state.cache.put(key, "value");
    }
}
