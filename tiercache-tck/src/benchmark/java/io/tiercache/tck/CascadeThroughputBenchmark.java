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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Two-level cascade-read throughput against a real Redis (the
 * {@code redis:6.2-alpine} container, started in trial setup): the externally
 * promised budget is &ge; 1M ops/s per instance for sustained
 * L1-miss &rarr; L2-hit &rarr; L1-warm reads with production defaults.
 *
 * <p>The cache under test is built exactly as the TCK chaos tests build it —
 * {@link TierCacheFactory} with {@link CacheSettings#defaults()}, a
 * {@link LettuceRemoteCache} with a stream journal, and the Pub/Sub
 * invalidation profile — with no benchmark-specific tuning: the budget
 * applies to default behavior.
 *
 * <p>Key-space sizing: the default L1 capacity is
 * {@code CacheSettings.defaults().l1MaxSize()} (10,000 entries). The cascade
 * key space is 4x that (40,000 keys), so a uniform-random read stream
 * provably cannot fit L1; at steady state most reads miss L1, hit L2, and
 * warm L1 — the budget scenario. The {@link #l1Hit} control uses 1,000 keys
 * (one tenth of L1 capacity), which L1 holds entirely, so the L1-hit vs
 * cascade gap keeps attribution honest.
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
public class CascadeThroughputBenchmark {

    private static final String CACHE = "bench-cascade";
    private static final String CONTROL_CACHE = "bench-control";

    /** 4x the default L1 capacity: uniform-random reads cannot fit L1. */
    private static final int CASCADE_KEYS = 40_000;

    /** One tenth of the default L1 capacity: fully L1-resident. */
    private static final int CONTROL_KEYS = 1_000;

    private GenericContainer<?> server;
    private RedisClient client;
    private RedisStreamJournal journal;
    private LettuceRemoteCache<String, String> l2;
    private TierCacheFactory factory;
    private TierCache<String, String> cascadeCache;
    private TierCache<String, String> controlCache;

    private String[] cascadeKeys;
    private String[] controlKeys;

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
                .build();
        cascadeCache = factory.getCache(CACHE);
        controlCache = factory.getCache(CONTROL_CACHE);

        cascadeKeys = new String[CASCADE_KEYS];
        for (int i = 0; i < CASCADE_KEYS; i++) {
            cascadeKeys[i] = "k-" + i;
            cascadeCache.put(cascadeKeys[i], "v-" + i);
        }
        controlKeys = new String[CONTROL_KEYS];
        for (int i = 0; i < CONTROL_KEYS; i++) {
            controlKeys[i] = "c-" + i;
            controlCache.put(controlKeys[i], "v-" + i);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        factory.close();
        l2.close();
        client.shutdown();
        server.stop();
    }

    /** Budget scenario: steady-state L1 miss &rarr; L2 hit &rarr; L1 warm. */
    @Benchmark
    public void cascadeRead(Blackhole bh) {
        bh.consume(cascadeCache.get(cascadeKeys[ThreadLocalRandom.current().nextInt(CASCADE_KEYS)]));
    }

    /** Control: L1-hit reads, for attributing the cascade cost. */
    @Benchmark
    public void l1Hit(Blackhole bh) {
        bh.consume(controlCache.get(controlKeys[ThreadLocalRandom.current().nextInt(CONTROL_KEYS)]));
    }
}
