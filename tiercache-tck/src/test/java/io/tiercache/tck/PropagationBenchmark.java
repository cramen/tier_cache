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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * Invalidation propagation latency harness (Pub/Sub profile), the diagnostic
 * for the cross-instance propagation budget (p99 &le; 5 ms). NOT a JUnit
 * test: run via {@code ./gradlew :tiercache-tck:propagationBenchmark}
 * (override the event count with
 * {@code -Dtiercache.propagation.events=N}, default 10,000).
 *
 * <p>Method: one Redis container, two {@link TierCacheFactory} instances
 * wired exactly like the TCK invalidation chaos tests (stream journal +
 * Pub/Sub transport + {@link InvalidationService}, production-default
 * settings). The writer puts N fresh keys; the publish timestamp for each
 * event is {@code System.nanoTime()} taken immediately before the
 * {@code put} call. The peer registers an {@code InvalidationEventListener}
 * that records the arrival {@code nanoTime} per event key; events are paired
 * by key and per-event latency = arrival &minus; publish.
 *
 * <p>Single-clock assumption: publish and arrival timestamps come from the
 * same machine's monotonic clock, so cross-host network jitter is not
 * represented. This matches the budget definition, which is single-AZ; the
 * harness is a regression net, not a production emulation.
 *
 * <p>Report: p50/p95/p99 in milliseconds, printed to stdout and written to
 * {@code build/results/propagation/results.txt} in a stable key=value format.
 */
public final class PropagationBenchmark {

    private static final String CACHE = "propagation";
    private static final String KEY_PREFIX = "prop-";
    private static final int WARMUP_EVENTS = 64;
    private static final int JOURNAL_CAPACITY = 10_000;

    private PropagationBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        int events = Integer.parseInt(System.getProperty("tiercache.propagation.events", "10000"));
        Path report = Path.of(System.getProperty("tiercache.propagation.report",
                "build/results/propagation/results.txt"));

        try (GenericContainer<?> server = new GenericContainer<>(
                DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379)) {
            server.start();
            String uri = "redis://" + server.getHost() + ":" + server.getMappedPort(6379);

            // Peer state: arrival nanoTime per event key, and a count of
            // measurement events applied (warm-up events do not count).
            Map<String, Long> arrivals = new ConcurrentHashMap<>();
            AtomicInteger received = new AtomicInteger();

            RedisClient writerClient = RedisClient.create(uri);
            RedisClient readerClient = RedisClient.create(uri);
            Side writer = new Side(writerClient, uri, (cache, message) -> {
            });
            Side reader = new Side(readerClient, uri, (cache, message) -> {
                String key = (String) message.key();
                arrivals.put(key, System.nanoTime());
                if (key.startsWith(KEY_PREFIX)) {
                    received.incrementAndGet();
                }
            });
            try {
                TierCache<String, String> writerCache = writer.factory.getCache(CACHE);
                reader.factory.getCache(CACHE); // registers the subscription

                // Warm-up: lets the Pub/Sub subscription establish and the
                // receive path JIT before any measured event.
                for (int i = 0; i < WARMUP_EVENTS; i++) {
                    writerCache.put("warmup-" + i, "v");
                }
                await(() -> arrivals.size() >= WARMUP_EVENTS, Duration.ofSeconds(30),
                        "warm-up events did not propagate");

                long[] published = new long[events];
                String[] keys = new String[events];
                for (int i = 0; i < events; i++) {
                    keys[i] = KEY_PREFIX + i;
                    published[i] = System.nanoTime();
                    writerCache.put(keys[i], "v");
                }
                int expected = events;
                await(() -> received.get() >= expected, Duration.ofSeconds(120),
                        "only " + received.get() + " of " + expected + " events arrived");

                double[] latenciesMs = new double[events];
                for (int i = 0; i < events; i++) {
                    latenciesMs[i] = (arrivals.get(keys[i]) - published[i]) / 1_000_000.0;
                }
                Arrays.sort(latenciesMs);

                String text = "tiercache.propagation.events=" + events + "\n"
                        + "tiercache.propagation.received=" + received.get() + "\n"
                        + "tiercache.propagation.p50_ms=" + format(percentile(latenciesMs, 50)) + "\n"
                        + "tiercache.propagation.p95_ms=" + format(percentile(latenciesMs, 95)) + "\n"
                        + "tiercache.propagation.p99_ms=" + format(percentile(latenciesMs, 99)) + "\n";
                System.out.print(text);
                Files.createDirectories(report.getParent());
                Files.writeString(report, text);
            } finally {
                writer.close();
                reader.close();
            }
        }
    }

    private static double percentile(double[] sorted, int p) {
        // Nearest-rank: smallest value with at least p% of samples at or below.
        int rank = Math.max(1, (int) Math.ceil(p / 100.0 * sorted.length));
        return sorted[rank - 1];
    }

    private static String format(double ms) {
        return String.format(Locale.ROOT, "%.3f", ms);
    }

    private static void await(Check check, Duration timeout, String failure) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!check.ok()) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException(failure);
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
    }

    private interface Check {
        boolean ok();
    }

    /** One cache instance over the shared server; mirrors the TCK chaos-test wiring. */
    private static final class Side implements AutoCloseable {
        private final RedisClient client;
        private final LettuceRemoteCache<String, String> l2;
        private final TierCacheFactory factory;

        Side(RedisClient client, String uri, io.tiercache.spi.InvalidationEventListener eventListener) {
            this.client = client;
            RedisStreamJournal journal = new RedisStreamJournal(
                    client.connect(ByteArrayCodec.INSTANCE), JOURNAL_CAPACITY, new JdkCacheSerializer<>());
            this.l2 = LettuceRemoteCache.<String, String>builder(uri)
                    .client(client)
                    .cacheName(CACHE)
                    .journal(journal)
                    .build();
            LettucePubSubInvalidationTransport transport =
                    new LettucePubSubInvalidationTransport(client, new JdkCacheSerializer<>());
            this.factory = TierCacheFactory.builder()
                    .defaults(CacheSettings.defaults())
                    .remoteCache(l2)
                    .invalidation(versions -> new InvalidationService(transport, journal,
                            versions.instanceId(), InvalidationListener.NOOP))
                    .invalidationEventListener(eventListener)
                    .build();
        }

        @Override
        public void close() {
            factory.close();
            l2.close();
            client.shutdown();
        }
    }
}
