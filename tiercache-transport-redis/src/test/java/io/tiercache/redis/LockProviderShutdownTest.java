package io.tiercache.redis;

import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;
import static io.tiercache.redis.LockOwnershipTest.proxy;

class LockProviderShutdownTest {
    private static final Duration LEASE = Duration.ofSeconds(1);

    static final class Fixture implements AutoCloseable {
        final RedisCommandTimeoutException failure = new RedisCommandTimeoutException("controlled SET timeout");
        final AtomicInteger deletes = new AtomicInteger(), connectionCloses = new AtomicInteger();
        volatile Runnable set = () -> { };
        volatile Runnable eval = () -> { };
        final RedisCommands<String, String> commands = proxy(RedisCommands.class, (o, m, a) -> {
            if (m.getName().equals("set")) { set.run(); throw failure; }
            if (m.getName().equals("eval")) { deletes.incrementAndGet(); eval.run(); return 0L; }
            return null;
        });
        final StatefulRedisConnection<String, String> connection = proxy(StatefulRedisConnection.class, (o, m, a) -> {
            if (m.getName().equals("sync")) return commands;
            if (m.getName().equals("close")) connectionCloses.incrementAndGet();
            return null;
        });
        final LettuceLockProvider provider = new LettuceLockProvider(connection);
        void acquire() {
            assertSame(failure, assertThrows(RedisCommandTimeoutException.class,
                    () -> provider.tryLock("shutdown", LEASE)));
        }
        @Override public void close() throws InterruptedException {
            provider.close();
            try { LockShutdownProbe.assertStopped(provider); assertEquals(0, connectionCloses.get()); }
            finally {
                var scheduler = LockShutdownProbe.scheduler(provider);
                // Also release deliberately broken mutant executors after an assertion failure.
                if (scheduler != null) scheduler.shutdownNow();
            }
        }
    }

    static void await(CountDownLatch latch) {
        try { assertTrue(latch.await(5, TimeUnit.SECONDS), "gate deadline"); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); }
    }

    @Test void bothPublicationOrdersRemainRetiredAcrossOneHundredRounds() throws Exception {
        var callers = Executors.newSingleThreadExecutor();
        try {
            for (int round = 0; round < 100; round++) {
                var entered = new CountDownLatch(1);
                var resume = new CountDownLatch(1);
                try (var f = new Fixture()) {
                    f.set = () -> { entered.countDown(); await(resume); };
                    var acquire = callers.submit(f::acquire);
                    try {
                        await(entered);
                        if ((round & 1) == 0) {
                            f.provider.close(); resume.countDown(); acquire.get(5, TimeUnit.SECONDS);
                            assertNull(LockShutdownProbe.scheduler(f.provider));
                        } else {
                            resume.countDown(); acquire.get(5, TimeUnit.SECONDS);
                            assertNotNull(LockShutdownProbe.scheduler(f.provider));
                            f.provider.close();
                        }
                        LockShutdownProbe.assertStopped(f.provider);
                    } finally { resume.countDown(); }
                }
            }
        } finally { callers.shutdownNow(); assertTrue(callers.awaitTermination(5, TimeUnit.SECONDS)); }
    }

    @Test void closeReturnsBeforeAdmittedWorkerExitsWithoutLeakingOrRescheduling() throws Exception {
        runningCleanup(false);
    }

    @Test void interruptibleCleanupTerminatesOnClose() throws Exception {
        runningCleanup(true);
    }

    private void runningCleanup(boolean interruptible) throws Exception {
        var entered = new CountDownLatch(1); var resume = new CountDownLatch(1);
        var interrupted = new CountDownLatch(1); var worker = new AtomicReference<Thread>();
        try (var f = new Fixture()) {
            f.eval = () -> {
                worker.set(Thread.currentThread()); entered.countDown();
                boolean done = false;
                while (!done) {
                    try { done = resume.await(5, TimeUnit.SECONDS); assertTrue(done, "release running cleanup"); }
                    catch (InterruptedException e) { interrupted.countDown(); if (interruptible) done = true; }
                }
            };
            try {
                f.acquire(); await(entered);
                var scheduler = LockShutdownProbe.scheduler(f.provider);
                f.provider.close(); await(interrupted);
                assertTrue(scheduler.isShutdown()); assertEquals(0, f.provider.pendingCompensations());
                if (!interruptible) {
                    assertTrue(worker.get().isAlive()); assertFalse(scheduler.isTerminated());
                    System.out.println("Controlled transient: " + LockShutdownProbe.diagnostics(f.provider));
                }
                resume.countDown(); LockShutdownProbe.assertStopped(f.provider);
                assertEquals(1, f.deletes.get(), "admitted attempt finishes; no retry");
            } finally { resume.countDown(); }
        }
    }

    @Test void anotherLiveProviderDoesNotMakeClosedProviderLeak() throws Exception {
        try (var a = new Fixture(); var b = new Fixture()) {
            a.acquire(); b.acquire(); a.provider.close(); LockShutdownProbe.assertStopped(a.provider);
            assertFalse(LockShutdownProbe.scheduler(b.provider).isShutdown());
            b.acquire(); assertTrue(b.provider.pendingCompensations() > 0);
        }
    }

    static final class CountingScheduler extends ScheduledThreadPoolExecutor {
        final AtomicInteger schedules = new AtomicInteger(), shutdowns = new AtomicInteger();
        volatile Runnable captured;
        CountingScheduler() { super(1); }
        @Override public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            captured = command; schedules.incrementAndGet();
            return super.schedule(command, 1, TimeUnit.DAYS);
        }
        @Override public java.util.List<Runnable> shutdownNow() {
            shutdowns.incrementAndGet(); return super.shutdownNow();
        }
    }

    @Test void concurrentCloseDisposesOnceAndRetiredCallbackCannotSchedule() throws Exception {
        var scheduler = new CountingScheduler();
        var callers = Executors.newFixedThreadPool(4);
        try (var f = new Fixture()) {
            LockShutdownProbe.install(f.provider, scheduler);
            f.acquire(); var retired = scheduler.captured;
            var start = new CountDownLatch(1);
            var futures = new java.util.ArrayList<Future<?>>();
            for (int i = 0; i < 4; i++) futures.add(callers.submit(() -> { await(start); f.provider.close(); }));
            start.countDown(); for (var future : futures) future.get(5, TimeUnit.SECONDS);
            assertEquals(1, scheduler.shutdowns.get());
            retired.run();
            assertEquals(0, f.deletes.get(), "retired callback cannot issue Redis I/O");
            assertEquals(1, scheduler.schedules.get(), "retired callback cannot reschedule");
            LockShutdownProbe.assertStopped(f.provider);
        } finally { scheduler.shutdownNow(); callers.shutdownNow(); assertTrue(callers.awaitTermination(5, TimeUnit.SECONDS)); }
    }
}
