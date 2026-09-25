package io.tiercache;

import io.tiercache.spi.*;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class FactoryRegistrationReadinessTest {
    static final class Registration implements InvalidationHandler {
        final CompletableFuture<Void> ready = new CompletableFuture<>();
        final CountDownLatch entered = new CountDownLatch(1);
        public void onLocalWrite(String cache, Object key, Version version, InvalidationMessage.Type type) { }
        public void registerTarget(String cache, InvalidationTarget target) { fail("async hook must be used"); }
        public CompletionStage<Void> registerTargetAsync(String cache, InvalidationTarget target) {
            if (cache.equals("pending")) { entered.countDown(); return ready; }
            return CompletableFuture.completedFuture(null);
        }
        public void close() { ready.completeExceptionally(new CancellationException("closed")); }
    }
    @Test void pendingRegistrationDoesNotHoldFactoryOrMapLocks() throws Exception {
        var registration = new Registration(); var workers = Executors.newFixedThreadPool(3);
        try (var factory = TierCacheFactory.builder().remoteCache(new InMemoryRemoteCache<>()).invalidation(v -> registration).build()) {
            var pending = workers.submit(() -> factory.getCache("pending"));
            assertTrue(registration.entered.await(5, TimeUnit.SECONDS)); assertFalse(pending.isDone());
            assertNotNull(workers.submit(() -> factory.asyncCache("other")).get(5, TimeUnit.SECONDS));
            var same = workers.submit(() -> factory.getCache("pending"));
            registration.ready.complete(null);
            assertSame(pending.get(5, TimeUnit.SECONDS), same.get(5, TimeUnit.SECONDS));
        } finally { registration.ready.complete(null); workers.shutdownNow(); }
    }
    @Test void closeCanCancelRegistrationBeforePublication() throws Exception {
        var registration = new Registration(); var workers = Executors.newFixedThreadPool(2);
        try (var factory = TierCacheFactory.builder().remoteCache(new InMemoryRemoteCache<>()).invalidation(v -> registration).build()) {
            var pending = workers.submit(() -> factory.asyncCache("pending"));
            assertTrue(registration.entered.await(5, TimeUnit.SECONDS));
            workers.submit(factory::close).get(5, TimeUnit.SECONDS);
            var error = assertThrows(ExecutionException.class, () -> pending.get(5, TimeUnit.SECONDS));
            assertInstanceOf(CancellationException.class, error.getCause());
        } finally { registration.ready.complete(null); workers.shutdownNow(); }
    }
    @Test void recursiveCreationFailsInsteadOfWaitingOnItsOwnReadiness() {
        var owner = new java.util.concurrent.atomic.AtomicReference<TierCacheFactory>();
        try (var factory = TierCacheFactory.builder().remoteCache(new InMemoryRemoteCache<>())
                .localCacheFactory((name, settings) -> {
                    owner.get().getCache(name);
                    return new io.tiercache.testkit.CountingLocalCache<>();
                }).build()) {
            owner.set(factory);
            assertTrue(assertThrows(IllegalStateException.class, () -> factory.getCache("recursive"))
                    .getMessage().contains("Recursive creation"));
        }
    }
}
