package io.tiercache;

import io.tiercache.internal.DefaultTierCache;
import io.tiercache.testkit.InMemoryRemoteCache;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Standalone before/after harness; see docs/benchmarks/async-followers.md. */
public final class AsyncFollowerBenchmark {
    private static final int WORKERS = 4;
    private final TierCacheFactory factory = TierCacheFactory.builder()
            .remoteCache(new InMemoryRemoteCache<>()).asyncExecutorThreads(WORKERS).build();
    private final TierCache<String, String> sync = factory.getCache("bench");
    private final AsyncTierCache<String, String> async = factory.asyncCache("bench");
    private final ThreadPoolExecutor executor;
    private final AtomicLong credits;
    private final Map<?, ?> claims;
    private long round;
    private final String profile;
    private final List<Long> latency = new ArrayList<>(), unrelatedLatency = new ArrayList<>();
    private final AtomicInteger rejected = new AtomicInteger(), failures = new AtomicInteger();
    private final AtomicLong attachmentsPeak = new AtomicLong(), creditsPeak = new AtomicLong(), queuePeak = new AtomicLong();
    private volatile boolean sample;

    private AsyncFollowerBenchmark(String profile) throws Exception {
        this.profile = profile;
        executor = (ThreadPoolExecutor) field(factory, "asyncExecutor");
        AtomicLong found;
        try { found = (AtomicLong) field(field(factory, "asyncAdmission"), "occupied"); }
        catch (NoSuchFieldException baseline) { found = null; }
        credits = found;
        claims = (Map<?, ?>) field(sync, "inflight");
        executor.prestartAllCoreThreads();
        sync.put("hot", "v");
    }
    private static Object field(Object object, String name) throws Exception {
        Field field = object.getClass().getDeclaredField(name); field.setAccessible(true); return field.get(object);
    }
    private void batch(boolean measure) throws Exception {
        int count = profile.equals("l1") ? 256 : 64;
        String slow = "slow" + round++;
        if (!profile.equals("l1")) {
            sync.evictAll();
            if (profile.equals("l2")) for (int i = 32; i < count; i++) sync.put("l2-" + i, "v");
            ((DefaultTierCache<?, ?>) sync).evictAllL1();
            sync.put("hot", "v");
        }
        List<CompletableFuture<?>> calls = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            boolean shared = !profile.equals("l1") && i < 32;
            String key = shared ? slow : switch (profile) {
                case "miss" -> "miss-" + i;
                case "l2" -> "l2-" + i;
                default -> "hot";
            };
            long start = System.nanoTime();
            var call = async.getOrComputeAsync(key, k -> {
                if (shared) {
                    try { Thread.sleep(25); } catch (InterruptedException e) { throw new RuntimeException(e); }
                    if (profile.equals("failure")) throw new IllegalStateException("expected source failure");
                } else if (profile.equals("miss")) {
                    try { Thread.sleep(2); } catch (InterruptedException e) { throw new RuntimeException(e); }
                } else throw new AssertionError("unexpected loader " + k);
                return "v";
            }).handle((value, error) -> {
                if (measure) {
                    long elapsed = System.nanoTime() - start;
                    synchronized (latency) { latency.add(elapsed); if (!shared) unrelatedLatency.add(elapsed); }
                    if (error != null) {
                        Throwable cause = error instanceof CompletionException ? error.getCause() : error;
                        if (cause instanceof RejectedExecutionException) rejected.incrementAndGet();
                        else failures.incrementAndGet();
                    }
                }
                return null;
            }).toCompletableFuture();
            calls.add(call);
        }
        CompletableFuture.allOf(calls.toArray(CompletableFuture[]::new)).get(10, TimeUnit.SECONDS);
    }
    private long allocated(com.sun.management.ThreadMXBean bean, long[] ids) {
        long sum = 0;
        for (long value : bean.getThreadAllocatedBytes(ids)) if (value > 0) sum += value;
        return sum;
    }
    private void run() throws Exception {
        try {
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (System.nanoTime() < end) batch(false);
            sample = true;
            Thread sampler = new Thread(() -> {
                while (sample) {
                    queuePeak.accumulateAndGet(executor.getQueue().size(), Math::max);
                    creditsPeak.accumulateAndGet(credits == null
                            ? executor.getActiveCount() + executor.getQueue().size() : credits.get(), Math::max);
                    long attached = 0;
                    for (Object claim : claims.values()) {
                        try { attached += ((CompletableFuture<?>) field(claim, "result")).getNumberOfDependents(); }
                        catch (Exception error) { throw new RuntimeException(error); }
                    }
                    attachmentsPeak.accumulateAndGet(attached, Math::max);
                    try { Thread.sleep(1); } catch (InterruptedException ignored) { return; }
                }
            }, "benchmark-sampler");
            sampler.start();
            var bean = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
            var os = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            long[] ids = bean.getAllThreadIds();
            long allocationStart = allocated(bean, ids), cpuStart = os.getProcessCpuTime(), start = System.nanoTime();
            end = start + TimeUnit.SECONDS.toNanos(3);
            while (System.nanoTime() < end) batch(true);
            long elapsed = System.nanoTime() - start, cpu = os.getProcessCpuTime() - cpuStart;
            long allocation = allocated(bean, ids) - allocationStart;
            sample = false; sampler.join();
            Collections.sort(latency); Collections.sort(unrelatedLatency);
            System.out.printf(Locale.ROOT,
                    "{\"profile\":\"%s\",\"operations\":%d,\"seconds\":%.6f,\"opsPerSecond\":%.3f,\"p50Ms\":%.6f,\"p95Ms\":%.6f,\"p99Ms\":%.6f,\"unrelatedP99Ms\":%.6f,\"cpuSeconds\":%.6f,\"allocatedBytesPerOp\":%.3f,\"rejections\":%d,\"failures\":%d,\"queuePeak\":%d,\"creditsPeak\":%d,\"attachmentsPeak\":%d}%n",
                    profile, latency.size(), elapsed / 1e9, latency.size() * 1e9 / elapsed,
                    percentile(latency, .5), percentile(latency, .95), percentile(latency, .99),
                    percentile(unrelatedLatency, .99), cpu / 1e9, (double) allocation / latency.size(),
                    rejected.get(), failures.get(), queuePeak.get(), creditsPeak.get(), attachmentsPeak.get());
        } finally { sample = false; factory.close(); }
    }
    private static double percentile(List<Long> data, double quantile) {
        return data.get(Math.min(data.size() - 1, (int) (data.size() * quantile))) / 1e6;
    }
    public static void main(String[] args) throws Exception { new AsyncFollowerBenchmark(args[0]).run(); }
}
