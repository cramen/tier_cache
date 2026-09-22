package io.tiercache.tck;

import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.tiercache.*;
import io.tiercache.invalidation.InvalidationService;
import io.tiercache.redis.*;
import io.tiercache.spi.InvalidationListener;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;

/** Strict, opt-in churn gate: separate post-GC heap/RSS, observed workers and retained evidence. */
@Tag("soak")
class SoakTest {
    private static final String CACHE = "soak";
    private static final Duration DEFAULT_DURATION = Duration.ofMinutes(10);
    private static final Duration SAMPLE_INTERVAL = Duration.ofSeconds(30);
    private static final Duration GC_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration WORKER_TIMEOUT = Duration.ofSeconds(60);
    private static final int WORKERS = 8, KEY_SPACE = 5_000, TAGS = 32, JOURNAL_CAPACITY = 2_000;

    @Test void churnStaysWithinMemoryAndJournalBudgets() throws Exception {
        Path path = Path.of(System.getProperty("tiercache.soak.report", "build/reports/soak/report.json"));
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("status", "RUNNING"); report.put("startedAt", Instant.now().toString());
        report.put("durationRequested", System.getProperty("tiercache.soak.duration", DEFAULT_DURATION.toString()));
        report.put("jdk", System.getProperty("java.runtime.version")); report.put("vm", System.getProperty("java.vm.name"));
        report.put("os", System.getProperty("os.name")); report.put("pid", ProcessHandle.current().pid());
        report.put("collectors", ManagementFactory.getGarbageCollectorMXBeans().stream().map(bean -> bean.getName()).toList());
        var heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        report.put("heapInitialBytes", heap.getInit()); report.put("heapMaxBytes", heap.getMax());
        report.put("relevantVmFlags", ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
                .filter(arg -> arg.startsWith("-Xms") || arg.startsWith("-Xmx") || arg.contains("DisableExplicitGC")
                        || arg.contains("ExplicitGCInvokesConcurrent") || arg.matches("-XX:[+-]Use.*GC")
                        || arg.startsWith("-XX:MaxRAMPercentage") || arg.startsWith("-XX:InitialRAMPercentage")).toList());
        report.put("sampleIntervalNanos", SAMPLE_INTERVAL.toNanos()); report.put("units", Map.of("memory", "bytes", "elapsed", "nanoseconds", "journal", "rows"));
        report.put("workload", Map.of("workers", WORKERS, "keySpace", KEY_SPACE, "tags", TAGS,
                "readPercent", 80, "putPercent", 15, "evictPercent", 5, "journalCapacity", JOURNAL_CAPACITY));
        report.put("limits", Map.of("heapGrowthFraction", SoakGate.MAX_GROWTH, "rssGrowthFraction", SoakGate.MAX_GROWTH,
                "warmupDiscardPercent", SoakGate.WARMUP_PERCENT, "minimumSteadySamples", 3,
                "journalPeakRows", JOURNAL_CAPACITY * 2, "journalMeanIncreaseRows", JOURNAL_CAPACITY * 0.25,
                "gcConfirmationTimeoutNanos", GC_TIMEOUT.toNanos(), "rssCommandTimeoutNanos", Duration.ofSeconds(2).toNanos(),
                "workerTerminationTimeoutNanos", WORKER_TIMEOUT.toNanos(), "workerCleanupTimeoutNanos", Duration.ofSeconds(1).toNanos()));
        report.put("samples", List.of()); report.put("workers", List.of());
        Throwable failed = null;
        try {
            Duration duration = duration(); SoakGate.validateDuration(duration, SAMPLE_INTERVAL);
            report.put("durationNanos", duration.toNanos());
            var rss = SoakMemory.systemRss();
            report.put("rssSource", System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("linux")
                    ? "/proc/self/status VmRSS (KiB to bytes)" : "/bin/ps -o rss= -p <JVM pid> (KiB to bytes)");
            report.put("heapSource", "Heap pools in completed System.gc() JMX notifications");
            rss.bytes(); // Reject unsupported/unavailable RSS before starting the container/workload.
            try (var gc = new SoakMemory.ExplicitGc()) {
                var preflight = memory(gc, rss);
                report.put("preflight", Map.of("heapBytes", preflight.heapBytes(), "rssBytes", preflight.rssBytes(),
                        "explicitGcCompletions", preflight.explicitGcCompletions()));
                SoakReport.write(path, report);
                try (var server = new GenericContainer<>(DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379)) {
                    server.start(); String uri = "redis://" + server.getHost() + ":" + server.getMappedPort(6379);
                    try (var client = RedisClient.create(uri); var connection = client.connect(ByteArrayCodec.INSTANCE)) {
                        var journal = new RedisStreamJournal(connection, JOURNAL_CAPACITY, new JdkCacheSerializer<>());
                        try (var l2 = LettuceRemoteCache.<String, String>builder(uri).client(client).cacheName(CACHE).journal(journal).build()) {
                            var transport = new LettucePubSubInvalidationTransport(client, new JdkCacheSerializer<>());
                            TierCacheFactory factory = null;
                            try {
                                factory = TierCacheFactory.builder()
                                        .defaults(new CacheSettings(KEY_SPACE, Duration.ofMinutes(2), null, Duration.ofMinutes(5),
                                                0.10, NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024))
                                        .remoteCache(l2).invalidation(versions -> new InvalidationService(transport, journal,
                                                versions.instanceId(), InvalidationListener.NOOP)).build();
                                runChurn(factory.getCache(CACHE), journal, duration, gc, rss, report, path);
                            } finally { if (factory != null) factory.close(); else transport.close(); }
                        }
                    }
                }
            }
            report.put("status", "PASS");
        } catch (Throwable failure) {
            failed = failure; report.put("status", "FAIL");
            report.put("failureClass", failure.getClass().getName()); report.put("failureMessage", failure.getMessage());
            var stack = new java.io.StringWriter(); failure.printStackTrace(new java.io.PrintWriter(stack)); report.put("failureStackTrace", stack.toString());
        } finally {
            report.put("finishedAt", Instant.now().toString());
            try { SoakReport.write(path, report); }
            catch (Exception writeFailure) { if (failed != null) failed.addSuppressed(writeFailure); else throw writeFailure; }
            System.out.println("SOAK " + report.get("status") + " report=" + path.toAbsolutePath());
        }
        if (failed instanceof Error error) throw error;
        if (failed instanceof Exception exception) throw exception;
        if (failed != null) throw new AssertionError(failed);
    }
    private static Duration duration() {
        String value = System.getProperty("tiercache.soak.duration");
        return value == null || value.isBlank() ? DEFAULT_DURATION : Duration.parse(value);
    }
    private static SoakMemory.Reading memory(SoakMemory.GcAccess gc, SoakMemory.RssReader rss) throws Exception {
        return SoakMemory.sample(gc, rss, System::nanoTime, duration -> TimeUnit.NANOSECONDS.sleep(duration.toNanos()), GC_TIMEOUT);
    }
    private static void runChurn(TierCache<String, String> cache, RedisStreamJournal journal, Duration duration,
            SoakMemory.GcAccess gc, SoakMemory.RssReader rss, Map<String, Object> report, Path path) throws Exception {
        long start = System.nanoTime(), deadline = start + duration.toNanos();
        var samples = new ArrayList<SoakGate.Sample>();
        var workers = new SoakWorkers(WORKERS, deadline, System::nanoTime, worker -> {
            while (worker.keepRunning()) { churnOnce(cache); worker.succeeded(); }
        });
        try {
            long next = start;
            while (true) {
                workers.checkProgress();
                long delay = next - System.nanoTime();
                if (delay > 0) TimeUnit.NANOSECONDS.sleep(delay);
                workers.checkProgress();
                var measured = memory(gc, rss);
                var sample = new SoakGate.Sample(System.nanoTime() - start, measured.heapBytes(), measured.rssBytes(),
                        journal.size(CACHE), workers.operations(), measured.explicitGcCompletions(), workers.snapshots());
                samples.add(sample); report.put("samples", samples.stream().map(SoakReport::sample).toList());
                report.put("workers", workers.snapshots()); SoakReport.write(path, report);
                System.out.printf(Locale.ROOT, "SOAK sample t=%.1fs heap=%d rss=%d journal=%d operations=%d%n",
                        sample.elapsedNanos() / 1e9, sample.heapBytes(), sample.rssBytes(), sample.journalRows(), sample.operations());
                if (System.nanoTime() - deadline >= 0) break;
                next = Math.min(next + SAMPLE_INTERVAL.toNanos(), deadline);
            }
            workers.awaitHealthy(WORKER_TIMEOUT);
            var assessment = SoakGate.assess(samples, JOURNAL_CAPACITY);
            report.put("assessment", SoakReport.assessment(assessment)); assessment.requirePass();
        } finally {
            workers.close(); report.put("workers", workers.snapshots()); report.put("workersTerminated", workers.terminated());
            report.put("completedOperations", workers.operations());
        }
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

}
