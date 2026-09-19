package io.tiercache.reactor;

import io.tiercache.CacheOverride;
import io.tiercache.LookupResult;
import io.tiercache.NullPolicy;
import io.tiercache.spi.StoredEntry;
import io.tiercache.testkit.CountingLocalCache;
import io.tiercache.testkit.CountingRemoteCache;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Spec: reactor-api — Mono variants delegate to the async view with
 * identical effects: a miss is an empty Mono, the caller thread performs
 * no cache I/O, L2 hits warm L1, null-markers and the tri-state lookup
 * behave exactly as on the sync path.
 */
class ReactorTierCacheTest {

    private static ReactorCacheFactory factory() {
        return ReactorCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .build();
    }

    @Test
    void reactorCacheIsMemoizedAndSharesTheEngine() {
        try (ReactorCacheFactory factory = factory()) {
            ReactorTierCache<String, String> a = factory.reactorCache("c");
            ReactorTierCache<String, String> b = factory.reactorCache("c");
            assertThat(a).isSameAs(b);

            StepVerifier.create(a.put("k", "v")).verifyComplete();
            StepVerifier.create(b.get("k")).expectNext("v").verifyComplete();
        }
    }

    // --- Scenario: miss is an empty Mono ---

    @Test
    void missIsAnEmptyMono() {
        try (ReactorCacheFactory factory = factory()) {
            ReactorTierCache<String, String> cache = factory.reactorCache("c");
            StepVerifier.create(cache.get("absent")).verifyComplete();
            StepVerifier.create(cache.getOrCompute("absent", key -> null)).verifyComplete();
        }
    }

    // --- Scenario: caller thread never blocks ---

    @Test
    void callerThreadPerformsNoCacheIoOrLoaderWork() throws Exception {
        CountDownLatch loaderEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<String> loaderThread = new AtomicReference<>();
        AtomicReference<Throwable> subscriptionFailure = new AtomicReference<>();

        try (ReactorCacheFactory factory = factory()) {
            ReactorTierCache<String, String> cache = factory.reactorCache("c");
            Thread caller = new Thread(() -> {
                Mono<String> mono = cache.getOrCompute("k", key -> {
                    loaderThread.set(Thread.currentThread().getName());
                    loaderEntered.countDown();
                    awaitQuietly(release);
                    return "v";
                });
                mono.subscribe(v -> {
                }, subscriptionFailure::set);
            }, "probe-caller");
            caller.start();

            assertThat(loaderEntered.await(5, TimeUnit.SECONDS)).isTrue();
            caller.join(5_000);
            assertThat(caller.isAlive())
                    .as("subscription must return while the loader is still parked")
                    .isFalse();
            assertThat(loaderThread.get())
                    .as("loader work must not run on the calling thread")
                    .isNotEqualTo("probe-caller");
            assertThat(loaderThread.get())
                    .as("work must run on the factory's bounded async executor")
                    .startsWith("tiercache-async");

            release.countDown();
            awaitTrue(() -> subscriptionFailure.get() != null || cache.get("k").block() != null,
                    "the parked loader must complete after release");
            assertThat(subscriptionFailure.get()).isNull();
        }
    }

    // --- Scenario: effects identical to the async view ---

    @Test
    void l2HitWarmsL1() {
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        CountingRemoteCache<String, String> l2 = new CountingRemoteCache<>();
        try (ReactorCacheFactory factory = ReactorCacheFactory.builder()
                .remoteCache(l2)
                .localCacheFactory((name, settings) -> l1)
                .build()) {
            ReactorTierCache<String, String> cache = factory.reactorCache("c");
            l2.put("k", StoredEntry.ofValue("v"), Duration.ofMinutes(1));

            StepVerifier.create(cache.get("k")).expectNext("v").verifyComplete();
            assertThat(l1.puts.get()).as("L2 hit must warm L1").isEqualTo(1);

            int l2GetsBefore = l2.gets.get();
            StepVerifier.create(cache.get("k")).expectNext("v").verifyComplete();
            assertThat(l2.gets.get())
                    .as("subsequent lookup must be served from L1")
                    .isEqualTo(l2GetsBefore);
        }
    }

    @Test
    void nullMarkerUnderAllowSuppressesTheLoader() {
        AtomicInteger loaderCalls = new AtomicInteger();
        try (ReactorCacheFactory factory = ReactorCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .cache("nulls", new CacheOverride().nullPolicy(NullPolicy.allow(Duration.ofSeconds(30))))
                .build()) {
            ReactorTierCache<String, String> cache = factory.reactorCache("nulls");

            StepVerifier.create(cache.getOrCompute("k", key -> {
                loaderCalls.incrementAndGet();
                return null;
            })).verifyComplete();
            StepVerifier.create(cache.lookup("k"))
                    .assertNext(result -> assertThat(result)
                            .as("the null-marker must be distinguishable from a miss")
                            .isInstanceOf(LookupResult.CachedNull.class))
                    .verifyComplete();

            StepVerifier.create(cache.getOrCompute("k", key -> {
                loaderCalls.incrementAndGet();
                return null;
            })).verifyComplete();
            assertThat(loaderCalls.get()).as("the marker must suppress the loader").isEqualTo(1);

            StepVerifier.create(cache.putNull("k2")).verifyComplete();
            StepVerifier.create(cache.lookup("k2"))
                    .assertNext(result -> assertThat(result).isInstanceOf(LookupResult.CachedNull.class))
                    .verifyComplete();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void lookupReportsTheTriState() {
        try (ReactorCacheFactory factory = factory()) {
            ReactorTierCache<String, String> cache = factory.reactorCache("c");

            StepVerifier.create(cache.lookup("k"))
                    .assertNext(result -> assertThat(result).isInstanceOf(LookupResult.Miss.class))
                    .verifyComplete();

            StepVerifier.create(cache.put("k", "v")).verifyComplete();
            StepVerifier.create(cache.lookup("k"))
                    .assertNext(result -> {
                        assertThat(result).isInstanceOf(LookupResult.Hit.class);
                        assertThat(((LookupResult.Hit<String>) result).value()).isEqualTo("v");
                    })
                    .verifyComplete();

            StepVerifier.create(cache.evict("k")).verifyComplete();
            StepVerifier.create(cache.lookup("k"))
                    .assertNext(result -> assertThat(result).isInstanceOf(LookupResult.Miss.class))
                    .verifyComplete();
        }
    }

    @Test
    void writeAndEvictOperationsMatchTheSyncPath() {
        try (ReactorCacheFactory factory = factory()) {
            ReactorTierCache<String, String> cache = factory.reactorCache("c");

            StepVerifier.create(cache.putIfAbsent("k", "v")).expectNext(true).verifyComplete();
            StepVerifier.create(cache.putIfAbsent("k", "w")).expectNext(false).verifyComplete();
            StepVerifier.create(cache.get("k")).expectNext("v").verifyComplete();

            StepVerifier.create(cache.put("t1", "v1", "tag")).verifyComplete();
            StepVerifier.create(cache.put("t2", "v2", "tag")).verifyComplete();
            StepVerifier.create(cache.evictByTag("tag")).verifyComplete();
            StepVerifier.create(cache.get("t1")).verifyComplete();
            StepVerifier.create(cache.get("t2")).verifyComplete();

            StepVerifier.create(cache.put("a", "va")).verifyComplete();
            StepVerifier.create(cache.put("b", "vb")).verifyComplete();
            StepVerifier.create(cache.evictAll(List.of("a"))).verifyComplete();
            StepVerifier.create(cache.get("a")).verifyComplete();
            StepVerifier.create(cache.get("b")).expectNext("vb").verifyComplete();

            StepVerifier.create(cache.evictAll()).verifyComplete();
            StepVerifier.create(cache.get("b")).verifyComplete();
        }
    }

    // --- helpers ---

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private interface BoolProbe {
        boolean getAsBoolean();
    }

    private static void awaitTrue(BoolProbe probe, String description) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (probe.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("timed out waiting for: " + description);
    }
}
