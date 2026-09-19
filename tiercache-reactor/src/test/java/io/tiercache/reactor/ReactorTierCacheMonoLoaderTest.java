package io.tiercache.reactor;

import io.tiercache.LookupResult;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spec: reactor-api — Mono loaders (design D2): concurrent subscribers
 * coalesce onto a single loader execution through the engine's
 * singleflight, a loader error reaches every coalesced subscriber
 * unwrapped, and cancelling one subscriber does not cancel the shared
 * load for the others.
 */
class ReactorTierCacheMonoLoaderTest {

    private static ReactorCacheFactory factory() {
        return ReactorCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                // Roomy enough for every subscriber task to start at once:
                // the coalescing tests need all callers inside one flight
                // before the loader completes or fails.
                .asyncExecutorThreads(40)
                .build();
    }

    @Test
    void concurrentSubscribersCoalesceOntoOneMonoLoaderExecution() throws Exception {
        int subscribers = 32;
        AtomicInteger loads = new AtomicInteger();
        CountDownLatch loaderEntered = new CountDownLatch(1);
        CompletableFuture<String> gated = new CompletableFuture<>();

        try (ReactorCacheFactory factory = factory()) {
            ReactorTierCache<String, String> cache = factory.reactorCache("c");
            List<CompletableFuture<String>> results = new ArrayList<>();
            for (int i = 0; i < subscribers; i++) {
                results.add(cache.getOrComputeMono("hot", key -> {
                    loads.incrementAndGet();
                    loaderEntered.countDown();
                    return Mono.fromFuture(gated);
                }).toFuture());
            }
            assertTrue(loaderEntered.await(5, TimeUnit.SECONDS));
            // Give the herd a chance to pile onto the same key.
            Thread.sleep(200);
            gated.complete("v");

            for (CompletableFuture<String> result : results) {
                assertThat(result.get(5, TimeUnit.SECONDS)).isEqualTo("v");
            }
            assertThat(loads.get()).as("singleflight: exactly one loader execution").isEqualTo(1);
            assertThat(cache.get("hot").block()).isEqualTo("v");
        }
    }

    @Test
    void monoLoaderErrorReachesEverySubscriberUnwrappedAndStoresNothing() throws Exception {
        int subscribers = 16;
        AtomicInteger loads = new AtomicInteger();
        CountDownLatch loaderEntered = new CountDownLatch(1);
        CompletableFuture<String> gated = new CompletableFuture<>();

        try (ReactorCacheFactory factory = factory()) {
            ReactorTierCache<String, String> cache = factory.reactorCache("c");
            List<CompletableFuture<String>> results = new ArrayList<>();
            for (int i = 0; i < subscribers; i++) {
                results.add(cache.getOrComputeMono("hot", key -> {
                    loads.incrementAndGet();
                    loaderEntered.countDown();
                    return Mono.fromFuture(gated)
                            .flatMap(v -> Mono.error(new IllegalStateException("boom")));
                }).toFuture());
            }
            assertTrue(loaderEntered.await(5, TimeUnit.SECONDS));
            Thread.sleep(200);
            gated.complete("x");

            for (CompletableFuture<String> result : results) {
                ExecutionException e = assertThrows(ExecutionException.class,
                        () -> result.get(5, TimeUnit.SECONDS));
                assertThat(e.getCause())
                        .as("the loader error is unwrapped, not CompletionException-wrapped")
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage("boom");
            }
            assertThat(loads.get()).as("one loader execution even on failure").isEqualTo(1);
            assertThat(cache.lookup("hot").block())
                    .as("nothing may be stored on loader failure")
                    .isInstanceOf(LookupResult.Miss.class);
        }
    }

    @Test
    void cancellingOneSubscriberDoesNotCancelTheSharedLoad() throws Exception {
        CountDownLatch loaderEntered = new CountDownLatch(1);
        CompletableFuture<String> gated = new CompletableFuture<>();

        try (ReactorCacheFactory factory = factory()) {
            ReactorTierCache<String, String> cache = factory.reactorCache("c");
            Function<String, Mono<? extends String>> loader = key -> {
                loaderEntered.countDown();
                return Mono.fromFuture(gated);
            };
            List<String> receivedByCancelled = new CopyOnWriteArrayList<>();
            Disposable cancelled = cache.getOrComputeMono("k", loader)
                    .subscribe(receivedByCancelled::add);
            CompletableFuture<String> remaining = cache.getOrComputeMono("k", loader).toFuture();

            assertTrue(loaderEntered.await(5, TimeUnit.SECONDS));
            // Give the second subscriber real time to park on the coalesced future.
            Thread.sleep(100);
            cancelled.dispose();
            gated.complete("v");

            assertThat(remaining.get(5, TimeUnit.SECONDS))
                    .as("the remaining subscriber still receives the shared load's value")
                    .isEqualTo("v");
            assertThat(receivedByCancelled).isEmpty();
            assertThat(cache.lookup("k").block())
                    .as("the shared load stores its result for everyone")
                    .isInstanceOf(LookupResult.Hit.class);
        }
    }
}
