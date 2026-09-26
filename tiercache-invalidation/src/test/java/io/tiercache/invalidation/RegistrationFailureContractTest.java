package io.tiercache.invalidation;

import io.tiercache.Version;
import io.tiercache.spi.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;

import static io.tiercache.invalidation.RecoveryProtocolTest.gate;
import static io.tiercache.invalidation.RegistrationReadinessTest.cause;
import static org.junit.jupiter.api.Assertions.*;

/** Failure and ownership contracts at the asynchronous registration boundary. */
class RegistrationFailureContractTest {
    @ParameterizedTest
    @ValueSource(strings = {"failed", "null-fence", "null-stage"})
    void failedFenceCannotClearOrSubscribeAndRegistrationCanBeRetried(String mode) throws Exception {
        var transport = new RegistrationReadinessTest.Transport();
        var failure = new IllegalArgumentException("fence unavailable");
        transport.fence = switch (mode) {
            case "failed" -> CompletableFuture.failedFuture(failure);
            case "null-fence" -> CompletableFuture.completedFuture(null);
            default -> null;
        };
        var target = new RecoveryProtocolTest.Target();
        var journal = new RecoveryProtocolTest.Journal();
        try (var service = new InvalidationService(transport, journal, UUID.randomUUID(), InvalidationListener.NOOP)) {
            var error = cause(service.registerTargetAsync("c", target).toCompletableFuture());
            if (mode.equals("failed")) assertSame(failure, error);
            else if (mode.equals("null-fence")) assertInstanceOf(IllegalStateException.class, error);
            else assertInstanceOf(NullPointerException.class, error);
            assertEquals(0, target.clears.get());
            assertEquals(0, journal.ends.get());
            assertEquals(0, transport.subscriptions.get());
            var releases = new AtomicInteger();
            transport.fence = CompletableFuture.completedFuture(releases::incrementAndGet);
            service.registerTargetAsync("c", target).toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertEquals(1, target.clears.get());
            assertEquals(1, transport.subscriptions.get());
            assertEquals(1, releases.get(), "registration must release its acquired fence");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"runtime", "error", "checked"})
    void synchronousRegistrationPreservesFailureIdentity(String mode) {
        var transport = new RegistrationReadinessTest.Transport();
        Throwable failure = switch (mode) {
            case "runtime" -> new IllegalArgumentException("registration failed");
            case "error" -> new AssertionError("registration failed");
            default -> new IOException("registration failed");
        };
        transport.fence = CompletableFuture.failedFuture(failure);
        try (var service = new InvalidationService(transport, null, UUID.randomUUID(), InvalidationListener.NOOP)) {
            var thrown = assertThrows(Throwable.class,
                    () -> service.registerTarget("c", new RecoveryProtocolTest.Target()));
            if (mode.equals("checked")) {
                assertInstanceOf(CompletionException.class, thrown);
                assertSame(failure, thrown.getCause());
            } else assertSame(failure, thrown);
            assertEquals(0, transport.subscriptions.get());
        }
    }

    @Test void missingBaselineFailsBeforeResetAndAValidRetrySucceeds() throws Exception {
        var transport = new RegistrationReadinessTest.Transport();
        var journal = new RecoveryProtocolTest.Journal();
        journal.end = cache -> null;
        var target = new RecoveryProtocolTest.Target();
        try (var service = new InvalidationService(transport, journal, UUID.randomUUID(), InvalidationListener.NOOP)) {
            assertInstanceOf(IllegalStateException.class,
                    cause(service.registerTargetAsync("c", target).toCompletableFuture()));
            assertEquals(0, target.clears.get());
            assertEquals(0, transport.subscriptions.get());
            journal.end = journal.data::endCursor;
            service.registerTargetAsync("c", target).toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertEquals(1, target.clears.get());
            assertEquals(1, transport.subscriptions.get());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void supersededResetDoesNotExposeASubscription(boolean epochChangesAfterReset) {
        var transport = new RegistrationReadinessTest.Transport();
        var target = new RecoveryProtocolTest.Target() {
            @Override public long resetRecovery(long expectedGeneration) {
                if (!epochChangesAfterReset) return -1;
                generation.incrementAndGet();
                return expectedGeneration;
            }
        };
        try (var service = new InvalidationService(transport, null, UUID.randomUUID(), InvalidationListener.NOOP)) {
            assertInstanceOf(IllegalStateException.class,
                    cause(service.registerTargetAsync("c", target).toCompletableFuture()));
            assertEquals(0, transport.subscriptions.get());
        }
    }

    @Test void legacyTargetWithDefaultZeroEpochCanRegister() {
        var transport = new RegistrationReadinessTest.Transport();
        var clears = new AtomicInteger();
        InvalidationTarget target = new InvalidationTarget() {
            public Version versionOfL1Entry(Object key) { return null; }
            public void evictL1IfNewer(Object key, Version version) { }
            public void evictAllL1() { clears.incrementAndGet(); }
        };
        try (var service = new InvalidationService(transport, null, UUID.randomUUID(), InvalidationListener.NOOP)) {
            service.registerTarget("c", target);
            assertEquals(0, target.recoveryGeneration());
            assertEquals(1, clears.get());
            assertEquals(1, transport.subscriptions.get());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void registrationWorkerRejectsReentrantWaitAndRestoresBorrowedThread(boolean synchronous) throws Exception {
        var executor = Executors.newSingleThreadScheduledExecutor();
        var reference = new AtomicReference<InvalidationService>();
        var journal = new RecoveryProtocolTest.Journal();
        var checked = new AtomicInteger();
        journal.end = cache -> {
            assertThrows(IllegalStateException.class, reference.get()::checkRegistrationWaitAllowed);
            checked.incrementAndGet();
            return journal.data.endCursor(cache);
        };
        try (var service = new InvalidationService(new RegistrationReadinessTest.Transport(), journal,
                UUID.randomUUID(), InvalidationListener.NOOP)) {
            reference.set(service);
            service.configureRecoveryExecutor(executor);
            if (synchronous) {
                executor.submit(() -> service.registerTarget("c", new RecoveryProtocolTest.Target())).get(5, TimeUnit.SECONDS);
            } else {
                service.registerTargetAsync("c", new RecoveryProtocolTest.Target()).toCompletableFuture().get(5, TimeUnit.SECONDS);
            }
            assertEquals(1, checked.get());
            executor.submit(service::checkRegistrationWaitAllowed).get(5, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test void recoveryGaugeIsOwnedOnceAcrossReplacementAndRepeatedClose() {
        var registrations = new AtomicInteger();
        var removals = new AtomicInteger();
        var transport = new RegistrationReadinessTest.Transport();
        var metrics = new CacheMetricsListener() {
            public AutoCloseable registerRecovery(String cache, BooleanSupplier pending) {
                assertEquals("c", cache);
                registrations.incrementAndGet();
                return removals::incrementAndGet;
            }
        };
        try (var service = new InvalidationService(transport, null, UUID.randomUUID(), InvalidationListener.NOOP, metrics)) {
            service.registerTarget("c", new RecoveryProtocolTest.Target());
            service.registerTarget("c", new RecoveryProtocolTest.Target());
            assertEquals(1, registrations.get());
            assertEquals(0, removals.get());
            service.close();
            service.close();
            assertEquals(1, removals.get());
        }
    }

    @Test void lateGaugeAfterCloseIsDisposedAndBorrowedExecutorRemainsUsable() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var removals = new AtomicInteger();
        var executor = Executors.newSingleThreadScheduledExecutor();
        var metrics = new CacheMetricsListener() {
            public AutoCloseable registerRecovery(String cache, BooleanSupplier pending) {
                entered.countDown(); gate(release);
                return removals::incrementAndGet;
            }
        };
        try (var service = new InvalidationService(new RegistrationReadinessTest.Transport(), null,
                UUID.randomUUID(), InvalidationListener.NOOP, metrics)) {
            service.configureRecoveryExecutor(executor);
            var ready = service.registerTargetAsync("c", new RecoveryProtocolTest.Target()).toCompletableFuture();
            gate(entered);
            service.close();
            assertInstanceOf(CancellationException.class, cause(ready));
            assertFalse(executor.isShutdown());
            release.countDown();
            executor.submit(() -> { }).get(5, TimeUnit.SECONDS);
            assertEquals(1, removals.get());
        } finally {
            release.countDown(); executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test void metricFailureDoesNotFailRegistrationOrDelivery() {
        var transport = new RegistrationReadinessTest.Transport();
        var target = new RecoveryProtocolTest.Target();
        var metrics = new CacheMetricsListener() {
            public AutoCloseable registerRecovery(String cache, BooleanSupplier pending) {
                throw new IllegalStateException("metrics unavailable");
            }
        };
        try (var service = new InvalidationService(transport, null, UUID.randomUUID(), InvalidationListener.NOOP, metrics)) {
            service.registerTarget("c", target);
            transport.handlers.get("c").accept(RecoveryProtocolTest.update("c", "key", 1));
            assertEquals("v1", target.values.get("key").payload());
        }
    }
}
