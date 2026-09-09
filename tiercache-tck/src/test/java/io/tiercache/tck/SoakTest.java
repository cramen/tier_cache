package io.tiercache.tck;

import com.sun.management.OperatingSystemMXBean;
import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.tiercache.CacheSettings;
import io.tiercache.InvalidationMode;
import io.tiercache.NullPolicy;
import io.tiercache.TierCache;
import io.tiercache.TierCacheFactory;
import io.tiercache.invalidation.InvalidationService;
import io.tiercache.redis.JdkCacheSerializer;
import io.tiercache.redis.LettucePubSubInvalidationTransport;
import io.tiercache.redis.LettuceRemoteCache;
import io.tiercache.redis.RedisStreamJournal;
import io.tiercache.spi.InvalidationListener;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Soak: sustained churn against a real L2 container. A fixed worker pool runs
 * a weighted workload (~80% reads, ~15% puts, ~5% evictions and tag
 * invalidations) over a rotating key space with jittered TTLs, exercising the
 * invalidation journal for real. Duration comes from the
 * {@code tiercache.soak.duration} system property (ISO-8601, default PT10M).
 *
 * <p>Every 30s the harness samples memory (heap used after an explicit GC,
 * plus committed VM size via {@link OperatingSystemMXBean}) and the journal
 * size. The first 20% of samples are discarded as warm-up; the gates on the
 * remainder are: memory growth at most 5%, journal size bounded with no
 * monotonic trend, and zero workload errors.
 *
 * <p>Memory metric rationale (design D2): a true per-process RSS read is
 * platform-specific, so the portable proxy is committed-VM size plus post-GC
 * heap used. Post-GC heap bounds live-set growth (leaks), committed VM bounds
 * total address-space growth the JVM is responsible for. The proxy can miss
 * pure native leaks outside the JVM's books; on a Linux CI runner
 * {@code /proc/self/status} can replace it without changing the gate shape.
 *
 * <p>Runs only via the dedicated {@code soakTest} task (tag-filtered); the
 * default {@code test} task excludes the {@code soak} tag.
 */
@Tag("soak")
class SoakTest {

    private static final String CACHE = "soak";
    private static final Duration DEFAULT_DURATION = Duration.parse("PT10M");
    private static final Duration SAMPLE_INTERVAL = Duration.ofSeconds(30);
    private static final int WORKERS = 8;
    private static final int KEY_SPACE = 5_000;
    private static final int TAGS = 32;
    private static final int JOURNAL_CAPACITY = 2_000;
    private static final double MAX_MEMORY_GROWTH = 0.05;
    private static final int WARMUP_DISCARD_PERCENT = 20;

    /** One memory/journal observation. */
    record Sample(long memoryBytes, long journalSize) {
    }

    @Test
    void churnStaysWithinMemoryAndJournalBudgets() throws Exception {
        Duration duration = duration();
        try (var server = new GenericContainer<>(DockerImageName.parse("redis:6.2-alpine"))
                .withExposedPorts(6379)) {
            server.start();
            String uri = "redis://" + server.getHost() + ":" + server.getMappedPort(6379);
            RedisClient client = RedisClient.create(uri);
            RedisStreamJournal journal = new RedisStreamJournal(
                    client.connect(ByteArrayCodec.INSTANCE), JOURNAL_CAPACITY, new JdkCacheSerializer<>());
            LettuceRemoteCache<String, String> l2 = LettuceRemoteCache.<String, String>builder(uri)
                    .client(client)
                    .cacheName(CACHE)
                    .journal(journal)
                    .build();
            LettucePubSubInvalidationTransport transport =
                    new LettucePubSubInvalidationTransport(client, new JdkCacheSerializer<>());
            TierCacheFactory factory = TierCacheFactory.builder()
                    .defaults(new CacheSettings(KEY_SPACE, Duration.ofMinutes(2), null,
                            Duration.ofMinutes(5), 0.10, NullPolicy.deny(),
                            InvalidationMode.INVALIDATE, 64 * 1024))
                    .remoteCache(l2)
                    .invalidation(versions -> new InvalidationService(transport, journal,
                            versions.instanceId(), InvalidationListener.NOOP))
                    .build();
            TierCache<String, String> cache = factory.getCache(CACHE);
            try {
                runChurn(cache, journal, duration);
            } finally {
                factory.close();
                l2.close();
                client.shutdown();
            }
        }
    }

    private static Duration duration() {
        String property = System.getProperty("tiercache.soak.duration");
        return property == null || property.isBlank() ? DEFAULT_DURATION : Duration.parse(property);
    }

    private static void runChurn(TierCache<String, String> cache, RedisStreamJournal journal,
            Duration duration) throws Exception {
        AtomicLong errors = new AtomicLong();
        long deadline = System.nanoTime() + duration.toNanos();
        ExecutorService pool = Executors.newFixedThreadPool(WORKERS);
        for (int i = 0; i < WORKERS; i++) {
            pool.submit(() -> {
                while (System.nanoTime() < deadline) {
                    try {
                        churnOnce(cache);
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    }
                }
            });
        }
        pool.shutdown();

        List<Sample> samples = new ArrayList<>();
        samples.add(sample(journal));
        while (System.nanoTime() < deadline) {
            long remainingMillis = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            Thread.sleep(Math.max(1, Math.min(SAMPLE_INTERVAL.toMillis(), remainingMillis)));
            samples.add(sample(journal));
        }
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "workers did not stop in time");
        assertEquals(0, errors.get(), "workload errors during the soak run");
        assertGates(samples);
    }

    private static void churnOnce(TierCache<String, String> cache) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        String key = "k" + random.nextInt(KEY_SPACE);
        int roll = random.nextInt(100);
        if (roll < 80) {
            cache.getOrCompute(key, k -> "value-" + k + "-" + random.nextInt());
        } else if (roll < 95) {
            if (random.nextInt(4) == 0) {
                cache.put(key, "value-" + random.nextInt(), "tag-" + random.nextInt(TAGS));
            } else {
                cache.put(key, "value-" + random.nextInt());
            }
        } else if (random.nextInt(2) == 0) {
            cache.evict(key);
        } else {
            cache.evictByTag("tag-" + random.nextInt(TAGS));
        }
    }

    private static Sample sample(RedisStreamJournal journal) throws InterruptedException {
        System.gc();
        System.gc();
        Thread.sleep(200); // let the explicit GCs finish before reading the heap
        long heapUsed = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
        long committedVm = ((OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean())
                .getCommittedVirtualMemorySize();
        return new Sample(heapUsed + committedVm, journal.size(CACHE));
    }

    private static void assertGates(List<Sample> samples) {
        assertTrue(samples.size() >= 3, () -> "too few samples to judge a trend: " + samples.size());
        int discard = (int) Math.ceil(samples.size() * WARMUP_DISCARD_PERCENT / 100.0);
        List<Sample> steady = samples.subList(discard, samples.size());

        long baseline = steady.get(0).memoryBytes();
        long peak = steady.stream().mapToLong(Sample::memoryBytes).max().orElseThrow();
        double growth = (peak - baseline) / (double) baseline;
        assertTrue(growth <= MAX_MEMORY_GROWTH,
                () -> "post-warm-up memory growth %.2f%% exceeds the %.0f%% gate (samples=%s)"
                        .formatted(growth * 100, MAX_MEMORY_GROWTH * 100, samples));

        // Bounded: approximate MAXLEN trimming keeps the stream near capacity;
        // whole-node trimming may overshoot, so the bound carries headroom.
        long journalPeak = steady.stream().mapToLong(Sample::journalSize).max().orElseThrow();
        assertTrue(journalPeak <= JOURNAL_CAPACITY * 2L,
                () -> "journal size " + journalPeak + " exceeds the bounded gate (samples=" + samples + ")");

        // No monotonic trend: the mean of the second half of the run must not
        // exceed the first half beyond a small allowance.
        int half = steady.size() / 2;
        double firstHalfMean = steady.subList(0, half).stream()
                .mapToLong(Sample::journalSize).average().orElse(0);
        double secondHalfMean = steady.subList(half, steady.size()).stream()
                .mapToLong(Sample::journalSize).average().orElse(0);
        assertTrue(secondHalfMean <= firstHalfMean + JOURNAL_CAPACITY * 0.25,
                () -> "journal size trends upward: first-half mean %.0f, second-half mean %.0f (samples=%s)"
                        .formatted(firstHalfMean, secondHalfMean, samples));
    }
}
