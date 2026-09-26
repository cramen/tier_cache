package io.tiercache.invalidation;

import io.tiercache.*;
import io.tiercache.spi.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;
import static org.junit.jupiter.api.Assertions.*;
import static io.tiercache.invalidation.RecoveryProtocolTest.*;
import static io.tiercache.invalidation.RegistrationReadinessTest.cause;

class InvalidationLifecycleContractsTest {
    @Test void localProofExpiresAndEachCompletedRepairReleasesItsFence() throws Exception {
        var transport = new RegistrationReadinessTest.Transport();
        var releases = new AtomicInteger();
        transport.fence = CompletableFuture.completedFuture(releases::incrementAndGet);
        var target = new Target();
        try (var service = new InvalidationService(transport, null, UUID.randomUUID(), InvalidationListener.NOOP)) {
            service.registerTarget("c", target);
            assertEquals(1, releases.get());
            var proof = transport.gaps.recoverLocalGap("c").toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertTrue(proof.succeeded()); assertEquals(2, releases.get());
            assertTrue(transport.gaps.isLocalRecoveryCurrent("c", proof));
            assertFalse(transport.gaps.isLocalRecoveryCurrent("missing", proof));
            assertFalse(transport.gaps.isLocalRecoveryCurrent("c", null));
            assertFalse(transport.gaps.isLocalRecoveryCurrent("c",
                    new RecoveryResult(proof.status(), proof.baseline(), proof.generation())));
            target.evictAllL1(); assertFalse(transport.gaps.isLocalRecoveryCurrent("c", proof));
            service.close(); assertFalse(transport.gaps.isLocalRecoveryCurrent("c", proof));
        }
    }

    @Test void ownedRecoveryWorkersAreDaemonGuardWaitsAndTerminateOnClose() throws Exception {
        var threads = new CopyOnWriteArraySet<Thread>();
        var owner = new AtomicReference<InvalidationService>();
        var journal = new Journal();
        journal.end = cache -> { threads.add(Thread.currentThread()); return journal.data.endCursor(cache); };
        journal.read = (cache, cursor, count) -> {
            threads.add(Thread.currentThread());
            assertThrows(IllegalStateException.class, owner.get()::checkRegistrationWaitAllowed);
            return journal.data.checkedRead(cache, cursor, count);
        };
        var target = new Target();
        try (var service = new InvalidationService(new RegistrationReadinessTest.Transport(), journal,
                UUID.randomUUID(), InvalidationListener.NOOP)) {
            owner.set(service);
            service.registerTargetAsync("c", target).toCompletableFuture().get(5, TimeUnit.SECONDS);
            journal.append("c", update("c", "key", 1));
            assertTrue(service.recoverAsync(Runnable::run).toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertEquals("v1", target.values.get("key").payload());
            assertFalse(threads.isEmpty());
            for (Thread thread : threads) assertTrue(thread.isDaemon());
            service.close();
            for (Thread thread : threads) { thread.join(2000); assertFalse(thread.isAlive()); }
        }
    }

    @Test void discardedAndReplacedSubscriptionsAreClosedExactlyOnce() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var subscribed = new AtomicInteger(); var closed = new CopyOnWriteArrayList<Integer>();
        InvalidationTransport transport = new InvalidationTransport() {
            public void publish(InvalidationMessage message) { }
            public boolean requiresRegistrationReset() { return true; }
            public AutoCloseable subscribe(String cache, Consumer<InvalidationMessage> handler) {
                int id = subscribed.incrementAndGet();
                if (id == 1) { entered.countDown(); gate(release); }
                return () -> closed.add(id);
            }
            public void close() { }
        };
        try (var service = new InvalidationService(transport, null, UUID.randomUUID(), InvalidationListener.NOOP)) {
            var first = service.registerTargetAsync("c", new Target()).toCompletableFuture();
            gate(entered);
            var second = service.registerTargetAsync("c", new Target()).toCompletableFuture();
            assertInstanceOf(CancellationException.class, cause(first));
            release.countDown(); second.get(5, TimeUnit.SECONDS);
            assertEquals(List.of(1), closed);
            service.registerTargetAsync("c", new Target()).toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertEquals(List.of(1, 2), closed);
            service.close(); service.close();
            assertEquals(List.of(1, 2, 3), closed);
        } finally { release.countDown(); }
    }

    @Test void serviceOwnsAndTerminatesItsPublicationWorker() throws Exception {
        var metrics = new PublicationObserverTest.Metrics();
        var transport = new PublicationObserverTest.Transport();
        var late = new CompletableFuture<PublicationOutcome>();
        try (var service = new InvalidationService(transport, null, UUID.randomUUID(), InvalidationListener.NOOP, metrics)) {
            service.onLocalWrite("c", "key", new Version(1, UUID.randomUUID()), InvalidationMessage.Type.INVALIDATE);
            await(() -> metrics.count(PublicationOutcome.ACKNOWLEDGED) == 1);
            Thread worker = metrics.callbackThread; assertTrue(worker.isDaemon());
            transport.action = () -> late;
            service.onLocalWrite("c", "later", new Version(2, UUID.randomUUID()), InvalidationMessage.Type.INVALIDATE);
            service.close(); worker.join(2000); assertFalse(worker.isAlive());
            late.complete(PublicationOutcome.ACKNOWLEDGED);
            assertEquals(1, metrics.count(PublicationOutcome.ACKNOWLEDGED));
        }
    }

    @Test void closeClearsQueuedBucketsAndStopsUnadmittedCallbacks() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var first = new AtomicBoolean(true);
        var metrics = new PublicationObserverTest.Metrics() {
            public void onPublication(String cache, PublicationOutcome outcome, long count) {
                callbackThread = Thread.currentThread();
                if (first.compareAndSet(true, false)) {
                    entered.countDown();
                    boolean done = false;
                    while (!done) try { done = release.await(5, TimeUnit.SECONDS); }
                    catch (InterruptedException ignored) { }
                }
                super.onPublication(cache, outcome, count);
            }
        };
        var observer = new PublicationObserver(metrics, System::nanoTime);
        try {
            observer.register("a"); observer.register("b");
            observer.admit("a").complete(PublicationOutcome.ACKNOWLEDGED, null); gate(entered);
            observer.admit("a").complete(PublicationOutcome.FAILED, null);
            observer.admit("b").complete(PublicationOutcome.ACKNOWLEDGED, null);
            assertEquals(2, observer.pendingTokens());
            observer.close(); assertEquals(0, observer.pendingTokens());
            release.countDown(); metrics.callbackThread.join(2000);
            assertFalse(metrics.callbackThread.isAlive());
            assertEquals(1, metrics.count(PublicationOutcome.ACKNOWLEDGED));
            assertEquals(0, metrics.count(PublicationOutcome.FAILED));
        } finally { release.countDown(); observer.close(); }
    }

    @Test void stoppingAdmissionReleasesTheGateForOtherThreads() throws Exception {
        var executor = Executors.newSingleThreadExecutor(r -> { var t = new Thread(r); t.setDaemon(true); return t; });
        try (var observer = new PublicationObserver(CacheMetricsListener.NOOP, System::nanoTime)) {
            observer.register("c"); int buckets = observer.bucketCount();
            observer.stopAdmission();
            assertNull(executor.submit(() -> observer.admit("c")).get(5, TimeUnit.SECONDS));
            executor.submit(() -> observer.register("late")).get(5, TimeUnit.SECONDS);
            assertEquals(buckets, observer.bucketCount());
        } finally { executor.shutdownNow(); }
    }
}
