package io.tiercache.micronaut;

import io.micronaut.core.type.Argument;
import io.tiercache.CacheSettings;
import io.tiercache.InvalidationMode;
import io.tiercache.NullPolicy;
import io.tiercache.TierCacheFactory;
import io.tiercache.testkit.InMemoryLockProvider;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Spec: micronaut-integration — AsyncCache adapter behavior (in-memory L2).
 */
class TierCacheMicronautAsyncCacheTest {

    private TierCacheMicronautAsyncCache newCache(String name, NullPolicy nullPolicy) {
        TierCacheFactory factory = TierCacheFactory.builder()
                .defaults(new CacheSettings(10_000, Duration.ofMinutes(5), null,
                        Duration.ofHours(1), 0.0, nullPolicy, InvalidationMode.INVALIDATE, 64 * 1024))
                .remoteCache(new InMemoryRemoteCache<>())
                .lockProvider(new InMemoryLockProvider())
                .build();
        return new TierCacheMicronautAsyncCache(name, factory.getCache(name), factory.asyncCache(name));
    }

    @Test
    void getPutInvalidateRoundTrip() {
        TierCacheMicronautAsyncCache cache = newCache("c", NullPolicy.deny());
        assertThat(cache.get("k", Argument.of(String.class)).join()).isEmpty();
        assertThat(cache.put("k", "v").join()).isTrue();
        assertThat(cache.get("k", Argument.of(String.class)).join()).contains("v");
        assertThat(cache.invalidate("k").join()).isTrue();
        assertThat(cache.get("k", Argument.of(String.class)).join()).isEmpty();
    }

    @Test
    void invalidateAllClearsTheCache() {
        TierCacheMicronautAsyncCache cache = newCache("c", NullPolicy.deny());
        cache.put("k", "v").join();
        assertThat(cache.invalidateAll().join()).isTrue();
        assertThat(cache.get("k", Argument.of(String.class)).join()).isEmpty();
    }

    @Test
    void cachedNullCompletesAsEmptyOptional() {
        TierCacheMicronautAsyncCache cache = newCache("c", NullPolicy.allow(Duration.ofMinutes(1)));
        // Cache the null marker via the loader form.
        assertThat(cache.get("k", Argument.of(String.class), () -> null).join()).isNull();
        assertThat(cache.get("k", Argument.of(String.class)).join()).isEmpty();
        // The tri-state stays available through the native cache.
        assertThat(cache.getNativeCache().lookup("k"))
                .isInstanceOf(io.tiercache.LookupResult.CachedNull.class);
    }

    @Test
    void loaderGetDoesNotBlockTheCaller() throws Exception {
        TierCacheMicronautAsyncCache cache = newCache("c", NullPolicy.deny());
        CountDownLatch loaderEntered = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        CompletableFuture<String> future = cache.get("k", Argument.of(String.class), () -> {
            loaderEntered.countDown();
            try {
                releaseLoader.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "v";
        });
        // The supplier runs off the calling thread: the caller holds a
        // future before the loader is allowed to finish.
        assertThat(loaderEntered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(future).isNotDone();
        releaseLoader.countDown();
        assertThat(future.get(5, TimeUnit.SECONDS)).isEqualTo("v");
    }

    @Test
    void concurrentAsyncLoaderGetsCoalesceOntoOneSupplierExecution() {
        TierCacheMicronautAsyncCache cache = newCache("c", NullPolicy.deny());
        AtomicInteger calls = new AtomicInteger();
        int concurrent = 16;
        var futures = new java.util.ArrayList<CompletableFuture<String>>();
        for (int i = 0; i < concurrent; i++) {
            futures.add(cache.get("hot", Argument.of(String.class), () -> {
                calls.incrementAndGet();
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return "v";
            }));
        }
        for (var f : futures) {
            assertThat(f.join()).isEqualTo("v");
        }
        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    void putIfAbsentReturnsExistingForLoser() {
        TierCacheMicronautAsyncCache cache = newCache("c", NullPolicy.deny());
        assertThat(cache.putIfAbsent("k", "v1").join()).isEmpty();
        assertThat(cache.putIfAbsent("k", "v2").join()).contains("v1");
        assertThat(cache.get("k", Argument.of(String.class)).join()).contains("v1");
    }
}
