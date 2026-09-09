package io.tiercache.tck;

import io.tiercache.CacheSettings;
import io.tiercache.InvalidationMode;
import io.tiercache.NullPolicy;
import io.tiercache.TierCache;
import io.tiercache.TierCacheFactory;
import io.tiercache.testkit.InMemoryRemoteCache;
import jdk.jfr.Configuration;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Virtual-thread stress: 100k virtual threads concurrently execute library
 * reads — L1 hits and loader-backed cascades over the in-memory L2 fixture —
 * while a JFR recording captures {@code jdk.VirtualThreadPinned} events.
 * The gate: zero pinned events whose stack contains library
 * ({@code io.tiercache.*}) frames.
 *
 * <p>Pinned detection runs with a zero threshold, so even sub-millisecond
 * pins are recorded; pins outside library frames are counted for diagnostics
 * but do not fail the gate.
 *
 * <p>Requires JDK 21+: this class lives in the {@code vtStress} source set
 * and runs via the {@code vtStressTest} task, which is skipped when no 21+
 * toolchain is installed.
 */
class VirtualThreadStressTest {

    private static final int THREADS = 100_000;
    private static final int OPS_PER_THREAD = 20;
    private static final int HOT_KEYS = 1_000;
    private static final int LOADER_KEYS = 1_000;
    private static final String PINNED_EVENT = "jdk.VirtualThreadPinned";
    private static final String LIBRARY_PREFIX = "io.tiercache.";

    @Test
    void noCarrierPinningOnLibraryPaths() throws Exception {
        TierCacheFactory factory = TierCacheFactory.builder()
                .defaults(new CacheSettings(HOT_KEYS + LOADER_KEYS, Duration.ofMinutes(10), null,
                        Duration.ofHours(1), 0.0, NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024))
                .remoteCache(new InMemoryRemoteCache<>())
                .build();
        TierCache<String, String> cache = factory.getCache("vt-stress");
        // Warm both levels so reads exercise L1 hits and L2-to-L1 warm-up,
        // not the loader path.
        for (int i = 0; i < HOT_KEYS; i++) {
            cache.put("hot-" + i, "v" + i);
        }

        Path jfr = Files.createTempFile("vt-stress", ".jfr");
        try {
            Configuration config = Configuration.getConfiguration("default");
            try (Recording recording = new Recording(config)) {
                recording.enable(PINNED_EVENT).with("threshold", "0 ms");
                recording.setDestination(jfr);
                recording.start();
                try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                    for (int t = 0; t < THREADS; t++) {
                        int id = t;
                        executor.submit(() -> runWorker(cache, id));
                    }
                } // close waits for all workers
                recording.stop();
            }

            List<RecordedEvent> pinned = RecordingFile.readAllEvents(jfr).stream()
                    .filter(event -> event.getEventType().getName().equals(PINNED_EVENT))
                    .toList();
            List<RecordedEvent> libraryPinned = pinned.stream()
                    .filter(VirtualThreadStressTest::hasLibraryFrame)
                    .toList();

            assertTrue(libraryPinned.isEmpty(),
                    () -> "expected zero " + PINNED_EVENT + " events on library frames, got "
                            + libraryPinned.size() + " (of " + pinned.size() + " pinned total); first:\n"
                            + libraryPinned.get(0).getStackTrace());
        } finally {
            Files.deleteIfExists(jfr);
            factory.close();
        }
    }

    private static void runWorker(TierCache<String, String> cache, int id) {
        for (int op = 0; op < OPS_PER_THREAD; op++) {
            int slot = (id + op) % HOT_KEYS;
            if ((op & 1) == 0) {
                cache.get("hot-" + slot); // L1 hit / L2 warm-up path
            } else {
                cache.getOrCompute("loader-" + (slot % LOADER_KEYS), key -> "computed-" + key);
            }
        }
    }

    private static boolean hasLibraryFrame(RecordedEvent event) {
        var stack = event.getStackTrace();
        if (stack == null) {
            return false;
        }
        return stack.getFrames().stream()
                .anyMatch(frame -> frame.getMethod().getType().getName().startsWith(LIBRARY_PREFIX));
    }
}
