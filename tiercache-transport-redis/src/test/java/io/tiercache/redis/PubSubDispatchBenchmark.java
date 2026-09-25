package io.tiercache.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.tiercache.*;
import io.tiercache.invalidation.InvalidationService;
import io.tiercache.spi.*;
import java.lang.management.ManagementFactory;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

/** Standalone real-Redis comparison; uses the legacy public constructor on both jars. */
public final class PubSubDispatchBenchmark {
    static final UUID ORIGIN = UUID.randomUUID();
    static final JdkCacheSerializer<Object> CODEC = new JdkCacheSerializer<>();
    static final class Target implements InvalidationTarget {
        final Map<Object, Long> versions = new ConcurrentHashMap<>();
        public Version versionOfL1Entry(Object key) { return null; }
        public void evictL1IfNewer(Object key, Version version) { versions.remove(key); }
        public void evictAllL1() { versions.clear(); }
        public void applyUpdateL1(Object key, Object value, Version version) { versions.merge(key, version.sequence(), Math::max); }
    }
    static InvalidationMessage event(String cache, long sequence) {
        return new InvalidationMessage(cache, "key", new Version(sequence, ORIGIN), ORIGIN, InvalidationMessage.Type.UPDATE, "value-" + sequence);
    }
    static Object field(Object object, String name) throws Exception {
        Class<?> type = object.getClass();
        while (type != null) {
            try { Field f = type.getDeclaredField(name); f.setAccessible(true); return f.get(object); }
            catch (NoSuchFieldException absent) { type = type.getSuperclass(); }
        }
        throw new NoSuchFieldException(name);
    }
    static long number(Object object, String method) throws Exception {
        Method m = object.getClass().getDeclaredMethod(method); m.setAccessible(true); return ((Number) m.invoke(object)).longValue();
    }
    static boolean pending(Object object, String cache) throws Exception {
        Method m = object.getClass().getDeclaredMethod("pending", String.class); m.setAccessible(true); return (boolean) m.invoke(object, cache);
    }
    static void until(BooleanSupplier condition) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!condition.getAsBoolean()) { if (System.nanoTime() > end) throw new AssertionError("timeout"); Thread.sleep(1); }
    }
    static double percentile(List<Long> values, double fraction) {
        values.sort(Long::compare); return values.get(Math.min(values.size() - 1, (int) (values.size() * fraction))) / 1e6;
    }
    public static void main(String[] args) throws Exception {
        String mode = args[1];
        var client = RedisClient.create(args[0]);
        var connection = client.connect(ByteArrayCodec.INSTANCE);
        var journal = new RedisStreamJournal(connection, 10000, CODEC);
        var publisher = new LettucePubSubInvalidationTransport(client, CODEC);
        var receiver = new LettucePubSubInvalidationTransport(client, CODEC);
        var service = new InvalidationService(receiver, journal, UUID.randomUUID(), InvalidationListener.NOOP);
        String a = "bench-a-" + UUID.randomUUID(), b = "bench-b-" + UUID.randomUUID();
        var targetA = new Target(); var targetB = new Target();
        service.registerTarget(a, targetA); service.registerTarget(b, targetB);
        Object dispatcher = field(receiver, "dispatcher");
        boolean candidate = !(dispatcher instanceof ExecutorService);
        ThreadPoolExecutor baseline = candidate ? null : (ThreadPoolExecutor) field(dispatcher, "e");
        var repairs = new AtomicInteger();
        if (candidate) {
            Object delegate = field(dispatcher, "recovery");
            var replacement = Proxy.newProxyInstance(InvalidationGapHandler.class.getClassLoader(), new Class<?>[]{InvalidationGapHandler.class}, (proxy, method, parameters) -> {
                if (method.getName().equals("recoverLocalGap")) repairs.incrementAndGet();
                return method.invoke(delegate, parameters);
            });
            Field f = dispatcher.getClass().getDeclaredField("recovery"); f.setAccessible(true); f.set(dispatcher, replacement);
        }
        var sent = new ConcurrentHashMap<String, Long>();
        var latency = new CopyOnWriteArrayList<Long>();
        var independentLatency = new AtomicLong(-1);
        var pause = new CountDownLatch(mode.equals("burst") ? 1 : 0);
        var entered = new CountDownLatch(1);
        var peakMessages = new AtomicLong(); var peakBytes = new AtomicLong(); var peakHeap = new AtomicLong(); var peakControl = new AtomicLong();
        var sampling = new AtomicBoolean(true);
        service.setEventListener((cache, message) -> {
            if (mode.equals("burst") && cache.equals(a) && message.version().sequence() == 1) {
                entered.countDown();
                try { pause.await(); } catch (InterruptedException error) { Thread.currentThread().interrupt(); }
            }
            Long start = sent.remove(cache + ":" + message.version().sequence());
            if (start != null) {
                long elapsed = System.nanoTime() - start; latency.add(elapsed);
                if (cache.equals(b)) independentLatency.set(elapsed);
            }
        });
        Thread sampler = new Thread(() -> {
            while (sampling.get()) {
                try {
                    long retained = candidate ? number(dispatcher, "retainedMessages") : baseline.getQueue().size() + baseline.getActiveCount();
                    peakMessages.accumulateAndGet(retained, Math::max);
                    if (candidate) {
                        peakBytes.accumulateAndGet(number(dispatcher, "retainedBytes"), Math::max);
                        peakControl.accumulateAndGet(number(dispatcher, "controlGroups"), Math::max);
                    }
                    peakHeap.accumulateAndGet(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed(), Math::max);
                    Thread.sleep(1);
                } catch (Exception error) { throw new RuntimeException(error); }
            }
        }, "benchmark-observer");
        var os = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        try {
            // Warm JIT/connection paths; all cache state is shared across publishers/readers.
            for (int i = 1; i <= 500; i++) {
                var message = event(b, i); journal.append(b, message);
                publisher.publishAsync(message).toCompletableFuture().get(5, TimeUnit.SECONDS);
            }
            until(() -> targetB.versions.getOrDefault("key", 0L) == 500);
            sampler.start(); long cpuStart = os.getProcessCpuTime(), start = System.nanoTime();
            int total = mode.equals("burst") ? 2001 : 5000;
            for (int i = 1; i <= total; i++) {
                if (mode.equals("healthy")) {
                    long remaining = start + (i - 1) * 1_000_000L - System.nanoTime();
                    if (remaining > 0) LockSupport.parkNanos(remaining);
                }
                var message = event(a, i); journal.append(a, message);
                sent.put(a + ":" + i, System.nanoTime());
                publisher.publishAsync(message).toCompletableFuture().get(5, TimeUnit.SECONDS);
                if (mode.equals("burst") && i == 1) {
                    if (!entered.await(5, TimeUnit.SECONDS)) throw new AssertionError("handler did not pause");
                    var independent = event(b, 501); journal.append(b, independent);
                    sent.put(b + ":501", System.nanoTime()); publisher.publishAsync(independent).toCompletableFuture().get(5, TimeUnit.SECONDS);
                }
            }
            boolean independentProgress = independentLatency.get() >= 0;
            long recoveryStart = System.nanoTime(); pause.countDown();
            until(() -> targetA.versions.getOrDefault("key", 0L) == total);
            if (candidate) until(() -> { try { return !pending(dispatcher, a); } catch (Exception e) { throw new RuntimeException(e); } });
            if (mode.equals("burst")) until(() -> independentLatency.get() >= 0);
            until(() -> latency.size() == total + (mode.equals("burst") ? 1 : 0));
            double recoveryMillis = (System.nanoTime() - recoveryStart) / 1e6;
            long elapsed = System.nanoTime() - start, cpu = os.getProcessCpuTime() - cpuStart;
            sampling.set(false); sampler.join();
            System.out.printf(Locale.ROOT,
                    "{\"mode\":\"%s\",\"operations\":%d,\"observed\":%d,\"throughput\":%.3f,\"p50Ms\":%.6f,\"p95Ms\":%.6f,\"p99Ms\":%.6f,\"independentBeforeRelease\":%s,\"independentMs\":%.6f,\"repairMillis\":%.6f,\"repairAttempts\":%d,\"peakMessages\":%d,\"peakEncodedBytes\":%d,\"peakControlGroups\":%d,\"peakHeapBytes\":%d,\"cpuSeconds\":%.6f}%n",
                    mode, total, latency.size(), total * 1e9 / elapsed, percentile(latency, .5), percentile(latency, .95), percentile(latency, .99),
                    independentProgress, independentLatency.get() / 1e6, recoveryMillis, repairs.get(), peakMessages.get(), peakBytes.get(), peakControl.get(), peakHeap.get(), cpu / 1e9);
        } finally {
            pause.countDown(); sampling.set(false); if (sampler.isAlive()) sampler.join();
            service.close(); publisher.close(); connection.close(); client.shutdown();
        }
    }
}
