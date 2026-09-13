package io.tiercache.tck;

import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.tiercache.CacheSettings;
import io.tiercache.TierCache;
import io.tiercache.TierCacheFactory;
import io.tiercache.invalidation.InvalidationService;
import io.tiercache.redis.JdkCacheSerializer;
import io.tiercache.redis.LettucePubSubInvalidationTransport;
import io.tiercache.redis.LettuceRemoteCache;
import io.tiercache.redis.RedisStreamJournal;
import io.tiercache.spi.CacheMetricsListener;
import io.tiercache.spi.InvalidationListener;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Mixed-workload throughput against a real Redis (the
 * {@code redis:6.2-alpine} container, started in trial setup): this benchmark
 * carries the externally promised budget of &ge; 1M ops/s per instance.
 *
 * <p>A hot key set of 1,000 keys (one tenth of the default L1 capacity of
 * 10,000, so L1 holds it entirely) receives ~95% of draws; the 40,000-key
 * cold set from {@link CascadeThroughputBenchmark} receives ~5%. The split is
 * a fixed 95/5 coin flip per invocation. This is a deliberate simplification:
 * the workload is a reference profile for the budget — representative of
 * real read mixes dominated by a small hot set — not a Zipf simulation. Both
 * regions are pre-populated in trial setup, so hot reads resolve in L1 and
 * cold reads exercise the L1-miss &rarr; L2-hit &rarr; L1-warm cascade. Trial
 * teardown prints the effective L1 hit share measured via
 * {@link io.tiercache.spi.CacheMetricsListener} (target ~95%, acceptable band
 * 93–97%), so drift between the draw ratio and the real hit rate is visible
 * in every run.
 *
 * <p>The cache under test is built exactly as the TCK chaos tests build it —
 * {@link TierCacheFactory} with {@link CacheSettings#defaults()}, a
 * {@link LettuceRemoteCache} with a stream journal, and the Pub/Sub
 * invalidation profile — with no benchmark-specific tuning: the budget
 * applies to default behavior.
 *
 * <p>Run: {@code ./gradlew :tiercache-tck:jmhBenchmark} (defaults: 1 fork,
 * 3 warmup + 5 measurement iterations of 1s; override with
 * {@code -Pjmh.fork/-Pjmh.warmupIterations/-Pjmh.iterations}). Results land
 * in {@code tiercache-tck/build/results/jmh-benchmark/results.txt}.
 */
@State(Scope.Benchmark)
@Threads(Threads.MAX)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class MixedWorkloadBenchmark {

    private static final String CACHE = "bench-mixed";

    /** One tenth of the default L1 capacity: fully L1-resident. */
    private static final int HOT_KEYS = 1_000;

    /** 4x the default L1 capacity: uniform-random reads cannot fit L1. */
    private static final int COLD_KEYS = 40_000;

    /** Percentage of draws routed to the hot set. */
    private static final int HOT_PERCENT = 95;

    private GenericContainer<?> server;
    private RedisClient client;
    private RedisStreamJournal journal;
    private LettuceRemoteCache<String, String> l2;
    private TierCacheFactory factory;
    private TierCache<String, String> cache;

    private String[] hotKeys;
    private String[] coldKeys;

    // Outcome counters: the trial report below prints the measured effective
    // L1 hit share, so drift between the draw ratio and the real hit rate is
    // visible in every run.
    private final LongAdder l1Hits = new LongAdder();
    private final LongAdder otherOutcomes = new LongAdder();

    @Setup(Level.Trial)
    public void setup() {
        server = new GenericContainer<>(DockerImageName.parse("redis:6.2-alpine"))
                .withExposedPorts(6379);
        server.start();
        String uri = "redis://" + server.getHost() + ":" + server.getMappedPort(6379);

        client = RedisClient.create(uri);
        journal = new RedisStreamJournal(
                client.connect(ByteArrayCodec.INSTANCE), 10_000, new JdkCacheSerializer<>());
        l2 = LettuceRemoteCache.<String, String>builder(uri)
                .client(client)
                .cacheName(CACHE)
                .journal(journal)
                .build();
        LettucePubSubInvalidationTransport transport =
                new LettucePubSubInvalidationTransport(client, new JdkCacheSerializer<>());
        factory = TierCacheFactory.builder()
                .defaults(CacheSettings.defaults())
                .remoteCache(l2)
                .invalidation(versions -> new InvalidationService(transport, journal,
                        versions.instanceId(), InvalidationListener.NOOP))
                .metricsListener(new CacheMetricsListener() {
                    @Override
                    public void onRequest(String cacheName, Outcome outcome) {
                        if (outcome == Outcome.L1_HIT) {
                            l1Hits.increment();
                        } else {
                            otherOutcomes.increment();
                        }
                    }
                })
                .build();
        cache = factory.getCache(CACHE);

        hotKeys = new String[HOT_KEYS];
        for (int i = 0; i < HOT_KEYS; i++) {
            hotKeys[i] = "h-" + i;
            cache.put(hotKeys[i], "v-" + i);
        }
        coldKeys = new String[COLD_KEYS];
        for (int i = 0; i < COLD_KEYS; i++) {
            coldKeys[i] = "c-" + i;
            cache.put(coldKeys[i], "v-" + i);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        long l1 = l1Hits.sum();
        long total = l1 + otherOutcomes.sum();
        System.out.printf(Locale.ROOT,
                "mixedWorkload effective L1 hit rate over the trial: %.2f%% (%d of %d reads)%n",
                total == 0 ? 0.0 : 100.0 * l1 / total, l1, total);
        factory.close();
        l2.close();
        client.shutdown();
        server.stop();
    }

    /** Budget scenario: ~95% hot L1 hits, ~5% cold cascade reads. */
    @Benchmark
    public void mixedWorkload(Blackhole bh) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        if (random.nextInt(100) < HOT_PERCENT) {
            bh.consume(cache.get(hotKeys[random.nextInt(HOT_KEYS)]));
        } else {
            bh.consume(cache.get(coldKeys[random.nextInt(COLD_KEYS)]));
        }
    }
}
