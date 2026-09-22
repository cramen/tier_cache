package io.tiercache.internal;

import io.tiercache.*;
import io.tiercache.spi.*;
import io.tiercache.testkit.*;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class AsyncBreakerRecoveryTest {
    static final class Rig {
        final AtomicLong now = new AtomicLong(1);
        final List<String> notifications = new CopyOnWriteArrayList<>();
        final Queue<Runnable> tasks = new ConcurrentLinkedQueue<>();
        final Queue<CompletableFuture<Boolean>> recoveries = new ConcurrentLinkedQueue<>();
        final CircuitBreaker breaker = new CircuitBreaker(new CircuitBreaker.Config(10, 0.5, 1, Duration.ZERO, 1),
                new CircuitBreaker.Listener() {
                    public void onOpen() { notifications.add("open:" + Thread.holdsLock(breaker)); }
                    public void onClose() { notifications.add("close:" + Thread.holdsLock(breaker)); }
                }, now::get);
        Rig() { breaker.configureRecovery(tasks::add, () -> recoveries.remove()); }
        CompletableFuture<Boolean> probe() {
            CompletableFuture<Boolean> result = new CompletableFuture<>(); recoveries.add(result);
            breaker.onFailure();
            var permit = breaker.tryAcquirePermit(); assertNotNull(permit); permit.success();
            assertEquals(BreakerState.HALF_OPEN, breaker.state());
            assertNull(breaker.tryAcquirePermit());
            tasks.remove().run();
            return result;
        }
    }

    @Test void successfulProbeReturnsBeforeRecoveryAndClosesOnlyOnCompletion() {
        Rig r = new Rig(); var completion = r.probe();
        assertEquals(List.of("open:false"), r.notifications);
        assertFalse(r.breaker.isOpen()); assertFalse(r.breaker.tryAcquire());
        completion.complete(true);
        assertEquals(BreakerState.CLOSED, r.breaker.state());
        assertEquals(List.of("open:false", "close:false"), r.notifications);
        assertTrue(r.breaker.tryAcquire());
    }

    @Test void failedRecoveryReopensWithExponentialMinimumDelay() {
        Rig r = new Rig(); r.probe().complete(false);
        assertEquals(BreakerState.OPEN, r.breaker.state());
        assertFalse(r.breaker.tryAcquire());
        r.now.addAndGet(Duration.ofMillis(999).toNanos()); assertTrue(r.breaker.isOpen());
        r.now.addAndGet(Duration.ofMillis(1).toNanos());
        var completion = new CompletableFuture<Boolean>(); r.recoveries.add(completion);
        var permit = r.breaker.tryAcquirePermit(); assertNotNull(permit); permit.success(); r.tasks.remove().run();
        completion.completeExceptionally(new IllegalStateException("baseline unavailable"));
        r.now.addAndGet(Duration.ofSeconds(1).toNanos()); assertFalse(r.breaker.tryAcquire());
        r.now.addAndGet(Duration.ofSeconds(1).toNanos()); assertTrue(r.breaker.tryAcquire());
    }

    @Test void lateRecoveryCannotCloseAReopenedEpisode() {
        Rig r = new Rig(); var old = r.probe();
        r.breaker.onFailure(); old.complete(true);
        assertFalse(r.notifications.stream().anyMatch(s -> s.startsWith("close")));
        assertEquals(BreakerState.HALF_OPEN, r.breaker.state());
    }

    @Test void detachReleasesPendingReservationWithoutRecoveredNotification() {
        Rig r = new Rig(); var old = r.probe(); r.breaker.detachRecovery();
        old.complete(true); assertEquals(List.of("open:false"), r.notifications);
        var realProbe = r.breaker.tryAcquirePermit(); assertNotNull(realProbe); realProbe.success();
        assertEquals(BreakerState.CLOSED, r.breaker.state());
        assertEquals(List.of("open:false", "close:false"), r.notifications);
    }

    @Test void detachPreservesOpenWaitAndSkipsQueuedRetiredHook() {
        var now = new AtomicLong(1); var calls = new AtomicInteger();
        var tasks = new ArrayDeque<Runnable>();
        var b = new CircuitBreaker(new CircuitBreaker.Config(2, 1, 1, Duration.ofSeconds(5), 1),
                new CircuitBreaker.Listener() { public void onOpen() { } public void onClose() { } }, now::get);
        b.configureRecovery(tasks::add, () -> { calls.incrementAndGet(); return CompletableFuture.completedFuture(true); });
        b.onFailure(); now.addAndGet(Duration.ofSeconds(2).toNanos()); b.detachRecovery();
        assertFalse(b.tryAcquire()); now.addAndGet(Duration.ofSeconds(3).toNanos()); assertTrue(b.tryAcquire());
        b.configureRecovery(tasks::add, () -> { calls.incrementAndGet(); return CompletableFuture.completedFuture(true); });
        b.onSuccess(); b.detachRecovery(); tasks.remove().run(); assertEquals(0, calls.get());
    }

    @Test void rejectedWorkerAndThrowingHookFailRecoveryInsteadOfClosing() {
        Rig rejected = new Rig();
        rejected.breaker.configureRecovery(r -> { throw new RejectedExecutionException(); }, () -> CompletableFuture.completedFuture(true));
        rejected.breaker.onFailure(); assertTrue(rejected.breaker.tryAcquire()); rejected.breaker.onSuccess();
        assertEquals(BreakerState.OPEN, rejected.breaker.state());
        Rig throwing = new Rig();
        throwing.breaker.configureRecovery(throwing.tasks::add, () -> { throw new AssertionError("hook"); });
        throwing.breaker.onFailure(); assertTrue(throwing.breaker.tryAcquire()); throwing.breaker.onSuccess();
        throwing.tasks.remove().run(); assertEquals(BreakerState.OPEN, throwing.breaker.state());
    }

    @Test void observersCannotTurnSuccessfulCallsIntoInfrastructureFailures() {
        var b = new CircuitBreaker(new CircuitBreaker.Config(2, 1, 1, Duration.ZERO, 1),
                new CircuitBreaker.Listener() {
                    public void onOpen() { throw new IllegalStateException("observer"); }
                    public void onClose() { throw new AssertionError("observer"); }
                });
        assertDoesNotThrow(b::onFailure);
        var permit = b.tryAcquirePermit(); assertNotNull(permit);
        assertDoesNotThrow(permit::success); assertEquals(BreakerState.CLOSED, b.state());
    }

    @Test void permitsIgnoreLateFailuresAndNeutralCompletionDoesNotConsumeProbes() {
        Rig r = new Rig(); var closedCall = r.breaker.tryAcquirePermit();
        r.breaker.onFailure(); closedCall.failure();
        var neutral = r.breaker.tryAcquirePermit(); assertNotNull(neutral); neutral.cancel(); neutral.success();
        var real = r.breaker.tryAcquirePermit(); assertNotNull(real);
        r.recoveries.add(CompletableFuture.completedFuture(true)); real.success(); real.failure();
        r.tasks.remove().run(); assertEquals(BreakerState.CLOSED, r.breaker.state());
    }

    @Test void recoveryHooksRunOffTheBusinessThreadAndSurvivingCacheProgressesAfterClose() throws Exception {
        var started = new CountDownLatch(1); var release = new CountDownLatch(1);
        var hookThread = new AtomicReference<Thread>(); var recovered = new AtomicInteger();
        var remote = new FailingRemoteCache<Object, Object>();
        InvalidationHandler handler = new InvalidationHandler() {
            public void onLocalWrite(String c, Object k, Version v, InvalidationMessage.Type t) { }
            public void registerTarget(String c, InvalidationTarget t) { }
            public void onL2Recovery() {
                hookThread.set(Thread.currentThread()); started.countDown();
                try { release.await(5, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
            public void close() { }
        };
        var factory = TierCacheFactory.builder().remoteCache(remote).invalidation(v -> handler)
                .circuitBreakerConfig(new CircuitBreaker.Config(2, 1, 1, Duration.ZERO, 1))
                .degradationListener(new DegradationListener() { public void onRecovered() { recovered.incrementAndGet(); } }).build();
        try {
            var cache = factory.getCache("c"); remote.fail(); cache.get("outage"); remote.heal();
            assertNull(cache.get("probe")); assertTrue(started.await(5, TimeUnit.SECONDS));
            assertNotSame(Thread.currentThread(), hookThread.get());
            assertEquals(BreakerState.HALF_OPEN, factory.breakerState());
            factory.close();
            remote.put("after", StoredEntry.ofValue("available"), Duration.ofMinutes(1));
            assertEquals("available", cache.get("after"));
            assertEquals(BreakerState.CLOSED, factory.breakerState());
            release.countDown(); assertEquals(0, recovered.get(), "closed coherence must not announce recovery");
        } finally { release.countDown(); factory.close(); }
    }

    @Test void recoveryTargetRejectsUpdateAndResetFromAnOlderClearEpoch() {
        var cache = new DefaultTierCache<String, String>("c", new CountingLocalCache<>(), new CountingRemoteCache<>(),
                CacheSettings.defaults(), true, null, null, new VersionGenerator(), null);
        long old = cache.recoveryGeneration(); cache.evictAllL1();
        var v = new Version(10, UUID.randomUUID());
        var update = new InvalidationMessage("c", "k", v, v.instanceId(), InvalidationMessage.Type.UPDATE, "old");
        assertEquals(-1, cache.applyRecovery(update, old)); assertEquals(-1, cache.resetRecovery(old));
        long current = cache.recoveryGeneration(); assertEquals(current, cache.applyRecovery(update, current));
        assertEquals("old", cache.get("k"));
        var invalidation = new InvalidationMessage("c", "k", new Version(11, v.instanceId()), v.instanceId(), InvalidationMessage.Type.INVALIDATE);
        assertEquals(current, cache.applyRecovery(invalidation, current)); assertNull(cache.get("k"));
        var clear = new InvalidationMessage("c", null, v, v.instanceId(), InvalidationMessage.Type.EVICT_ALL);
        assertEquals(current + 1, cache.applyRecovery(clear, current));
    }
}
