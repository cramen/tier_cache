package io.tiercache.tck;

import com.sun.management.GarbageCollectionNotificationInfo;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;

/** Independent, bounded memory acquisition for the strict soak gate. */
final class SoakMemory {
    interface RssReader { long bytes() throws Exception; }
    interface CommandRunner { String run(List<String> command, Duration timeout) throws Exception; }
    interface GcAccess {
        long completed();
        long heapAfterGc();
        void request();
    }
    interface Sleeper { void sleep(Duration duration) throws InterruptedException; }
    record Reading(long heapBytes, long rssBytes, long explicitGcCompletions) { }

    static RssReader rssReader(String os, long pid, CommandRunner runner,
            java.util.function.Supplier<String> linuxStatus) {
        String platform = os.toLowerCase(Locale.ROOT);
        if (platform.contains("linux")) return () -> linuxRss(linuxStatus.get());
        if (platform.contains("mac") || platform.contains("darwin")) {
            return () -> macRss(runner.run(List.of("/bin/ps", "-o", "rss=", "-p", Long.toString(pid)), Duration.ofSeconds(2)));
        }
        throw new IllegalStateException("RSS measurement unsupported on " + os + "; run the strict soak on Linux or macOS");
    }
    static RssReader systemRss() {
        return rssReader(System.getProperty("os.name"), ProcessHandle.current().pid(), SoakMemory::runCommand, () -> {
            try { return Files.readString(Path.of("/proc/self/status")); }
            catch (java.io.IOException e) { throw new IllegalStateException("Cannot read /proc/self/status for RSS", e); }
        });
    }
    static long linuxRss(String status) {
        var matcher = Pattern.compile("(?m)^VmRSS:[ \\t]+([0-9]+)[ \\t]+kB[ \\t]*$").matcher(status);
        if (!matcher.find()) throw new IllegalStateException("Missing or malformed VmRSS (expected KiB) in /proc/self/status");
        long bytes = kibibytes(matcher.group(1));
        if (matcher.find()) throw new IllegalStateException("Duplicate VmRSS measurement");
        return bytes;
    }
    static long macRss(String output) {
        String number = output.strip();
        if (!number.matches("[0-9]+")) throw new IllegalStateException("Malformed ps RSS; expected one positive KiB value");
        return kibibytes(number);
    }
    private static long kibibytes(String value) {
        try {
            long bytes = Math.multiplyExact(Long.parseLong(value), 1024L);
            if (bytes <= 0) throw new IllegalArgumentException("nonpositive RSS");
            return bytes;
        } catch (RuntimeException e) { throw new IllegalStateException("Invalid RSS KiB value: " + value, e); }
    }
    static String runCommand(List<String> command, Duration timeout) throws Exception {
        ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true);
        builder.environment().put("LC_ALL", "C");
        builder.environment().put("LANG", "C");
        Process process = builder.start();
        try {
            if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("RSS command timed out after " + timeout + ": " + command.get(0));
            }
            byte[] output = process.getInputStream().readNBytes(4097);
            if (process.exitValue() != 0 || output.length > 4096) {
                throw new IllegalStateException("RSS command failed or returned oversized output (exit=" + process.exitValue() + ")");
            }
            return new String(output, StandardCharsets.US_ASCII);
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(1, TimeUnit.SECONDS);
            }
            process.getInputStream().close();
            process.getErrorStream().close();
            process.getOutputStream().close();
        }
    }
    static Reading sample(GcAccess gc, RssReader rss, LongSupplier clock, Sleeper sleeper,
            Duration timeout) throws Exception {
        long before = gc.completed();
        long start = clock.getAsLong();
        gc.request();
        while (true) {
            if (clock.getAsLong() - start >= timeout.toNanos()) {
                throw new IllegalStateException("Post-GC heap is inconclusive: no explicit GC completion within " + timeout
                        + "; enable explicit GC (remove -XX:+DisableExplicitGC) and use a collector reporting System.gc() completion");
            }
            if (gc.completed() > before) break;
            sleeper.sleep(Duration.ofMillis(10));
        }
        long heap = gc.heapAfterGc(), resident = rss.bytes();
        if (heap <= 0 || resident <= 0) throw new IllegalStateException("Memory acquisition requires positive heap and RSS bytes");
        return new Reading(heap, resident, gc.completed());
    }

    /** Uses completion notifications, not an assumed sleep or an unrelated young GC. */
    static final class ExplicitGc implements GcAccess, AutoCloseable {
        private final List<NotificationEmitter> emitters = new ArrayList<>();
        private final AtomicLong completed = new AtomicLong(), heap = new AtomicLong();
        private final Set<String> heapPools = new HashSet<>();
        private final NotificationListener listener;
        ExplicitGc() throws Exception {
            for (var pool : ManagementFactory.getMemoryPoolMXBeans()) if (pool.getType() == MemoryType.HEAP) heapPools.add(pool.getName());
            listener = (notification, handback) -> {
                if (!GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION.equals(notification.getType())) return;
                var info = GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData());
                if (!"System.gc()".equals(info.getGcCause())) return;
                long used = info.getGcInfo().getMemoryUsageAfterGc().entrySet().stream()
                        .filter(entry -> heapPools.contains(entry.getKey())).mapToLong(entry -> entry.getValue().getUsed()).sum();
                heap.set(used);
                completed.incrementAndGet();
            };
            try {
                for (var collector : ManagementFactory.getGarbageCollectorMXBeans()) {
                    if (collector instanceof NotificationEmitter emitter) {
                        emitter.addNotificationListener(listener, null, null);
                        emitters.add(emitter);
                    }
                }
                if (emitters.isEmpty()) throw new IllegalStateException("Collector cannot report explicit GC completion; strict post-GC measurement unavailable");
            } catch (Exception e) { close(); throw e; }
        }
        public long completed() { return completed.get(); }
        public long heapAfterGc() { return heap.get(); }
        public void request() { System.gc(); }
        public void close() {
            for (var emitter : emitters) {
                try { emitter.removeNotificationListener(listener); }
                catch (javax.management.ListenerNotFoundException ignored) { }
            }
            emitters.clear();
        }
    }
}
