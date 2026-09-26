package io.tiercache.internal;

import io.tiercache.BreakerState;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class BreakerAdmissionContractTest {
    static final CircuitBreaker.Listener NOOP = new CircuitBreaker.Listener() {
        public void onOpen() { }
        public void onClose() { }
    };
    static int count(CircuitBreaker b, String name) throws Exception {
        var f = CircuitBreaker.class.getDeclaredField(name); f.setAccessible(true);
        synchronized (b) { return f.getInt(b); }
    }
    @Test void completedOutcomeRingMatchesSequentialReference() throws Exception {
        var now = new AtomicLong();
        var b = new CircuitBreaker(new CircuitBreaker.Config(7, .6, 4, Duration.ofNanos(1), 2), NOOP, now::get);
        var ring = new ArrayDeque<Boolean>(); var random = new Random(2317);
        for (int i = 0; i < 4000; i++) {
            if (b.state() == BreakerState.OPEN) {
                now.incrementAndGet();
                var a = b.tryAcquirePermit(); var c = b.tryAcquirePermit();
                assertNotNull(a); assertNotNull(c); assertNull(b.tryAcquirePermit());
                a.success(); c.success(); ring.clear();
            }
            var permit = b.tryAcquirePermit(); assertNotNull(permit);
            int outcome = random.nextInt(3);
            if (outcome == 0) permit.cancel();
            else {
                boolean failed = outcome == 1;
                if (failed) permit.failure(); else permit.success();
                if (ring.size() == 7) ring.removeFirst(); ring.addLast(failed);
            }
            // Every second completion is a duplicate, including cancellation.
            permit.failure(); permit.success(); permit.cancel();
            int failures = (int) ring.stream().filter(Boolean::booleanValue).count();
            assertEquals(ring.size(), count(b, "windowCount"));
            assertEquals(failures, count(b, "windowFailures"));
            assertEquals(outcome == 1 && ring.size() >= 4 && failures >= Math.ceil(.6 * ring.size())
                    ? BreakerState.OPEN : BreakerState.CLOSED, b.state());
        }
    }
    @Test void closedDetachRetiresOldPermitsWithoutClearingTheRing() throws Exception {
        var b = new CircuitBreaker(new CircuitBreaker.Config(10, .5, 5, Duration.ofDays(1), 2), NOOP);
        b.tryAcquirePermit().success(); var old = b.tryAcquirePermit();
        b.detachRecovery(); old.failure();
        assertEquals(BreakerState.CLOSED, b.state()); assertEquals(1, count(b, "windowCount"));
        b.tryAcquirePermit().failure(); assertEquals(2, count(b, "windowCount"));
        assertEquals(1, count(b, "windowFailures"));
    }
    @Test void concurrentDuplicateCompletionsContributeExactlyOnce() throws Exception {
        var b = new CircuitBreaker(new CircuitBreaker.Config(8, 1, 9, Duration.ZERO, 1), NOOP);
        var pool = Executors.newFixedThreadPool(3);
        try {
            for (int i = 0; i < 100; i++) {
                var permit = b.tryAcquirePermit(); var start = new CyclicBarrier(3);
                int before = count(b, "windowCount");
                var futures = new ArrayList<Future<?>>();
                for (int n = 0; n < 3; n++) {
                    int outcome = n;
                    futures.add(pool.submit(() -> { start.await();
                        if (outcome == 0) permit.cancel(); else if (outcome == 1) permit.success(); else permit.failure();
                        return null;
                    }));
                }
                for (var f : futures) f.get(5, TimeUnit.SECONDS);
                int after = count(b, "windowCount");
                assertTrue(after == before || after == Math.min(8, before + 1));
                assertTrue(count(b, "windowFailures") <= after);
                assertEquals(BreakerState.CLOSED, b.state());
            }
        } finally { pool.shutdownNow(); }
    }
    @Test void halfOpenConcurrentReservationsStayBoundedAndOldCancellationIsNeutral() throws Exception {
        var now = new AtomicLong();
        var b = new CircuitBreaker(new CircuitBreaker.Config(4, 1, 1, Duration.ofNanos(1), 3), NOOP, now::get);
        var retired = b.tryAcquirePermit(); b.onFailure(); now.incrementAndGet();
        var permits = new ConcurrentLinkedQueue<CircuitBreaker.Permit>();
        var pool = Executors.newFixedThreadPool(16); var start = new CountDownLatch(1);
        try {
            var futures = new ArrayList<Future<?>>();
            for (int i = 0; i < 32; i++) futures.add(pool.submit(() -> { start.await();
                var p = b.tryAcquirePermit(); if (p != null) permits.add(p); return null;
            }));
            start.countDown(); for (var f : futures) f.get(5, TimeUnit.SECONDS);
            assertEquals(3, permits.size()); retired.cancel(); assertNull(b.tryAcquirePermit());
            permits.remove().cancel(); var replacement = b.tryAcquirePermit(); assertNotNull(replacement);
            assertNull(b.tryAcquirePermit()); replacement.success();
            for (var p : permits) p.success(); assertEquals(BreakerState.CLOSED, b.state());
        } finally { pool.shutdownNow(); }
    }

    static Object field(Object value,String name) throws Exception {
        var field=value.getClass().getDeclaredField(name);field.setAccessible(true);return field.get(value);
    }
    static void coherent(CircuitBreaker breaker) throws Exception {
        synchronized(breaker) {
            Object view=field(breaker,"admission");
            assertEquals(field(breaker,"state"),field(view,"state"));
            assertEquals(field(breaker,"epoch"),field(view,"epoch"));
        }
    }
    @Test void everyTransitionAndEpochOnlyDetachPublishesCoherentState() throws Exception {
        var now=new AtomicLong();var b=new CircuitBreaker(new CircuitBreaker.Config(2,1,1,Duration.ofNanos(1),1),NOOP,now::get);
        coherent(b);b.detachRecovery();coherent(b);
        b.onFailure();coherent(b);b.detachRecovery();coherent(b);
        now.incrementAndGet();assertEquals(BreakerState.HALF_OPEN,b.state());coherent(b);
        var queued=new ArrayDeque<Runnable>();var pending=new CompletableFuture<Boolean>();
        b.configureRecovery(queued::add,()->pending);
        var probe=b.tryAcquirePermit();assertNotNull(probe);probe.success();coherent(b);assertNull(b.tryAcquirePermit());
        queued.remove().run();pending.complete(true);assertEquals(BreakerState.CLOSED,b.state());coherent(b);
        b.onFailure();now.incrementAndGet();var late=new CompletableFuture<Boolean>();b.configureRecovery(queued::add,()->late);
        b.tryAcquirePermit().success();queued.remove().run();b.detachRecovery();coherent(b);
        Object detached=field(b,"admission");late.complete(true);assertSame(detached,field(b,"admission"));
        b.tryAcquirePermit().success();assertEquals(BreakerState.CLOSED,b.state());coherent(b);
    }
    @Test void nonClosedObservationRechecksAfterConcurrentClose() throws Exception {
        var b=new CircuitBreaker(new CircuitBreaker.Config(2,1,1,Duration.ZERO,1),NOOP);
        b.onFailure();var pool=Executors.newSingleThreadExecutor();var attempted=new CountDownLatch(1);
        var caller=new AtomicReference<Thread>();
        try {
            Future<CircuitBreaker.Permit> request;
            synchronized(b) {
                request=pool.submit(()->{caller.set(Thread.currentThread());attempted.countDown();return b.tryAcquirePermit();});
                assertTrue(attempted.await(5,TimeUnit.SECONDS));
                long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
                while(caller.get().getState()!=Thread.State.BLOCKED && System.nanoTime()<deadline) Thread.onSpinWait();
                assertEquals(Thread.State.BLOCKED,caller.get().getState());
                assertTrue(b.tryAcquire());b.onSuccess();
            }
            assertNotNull(request.get(5,TimeUnit.SECONDS));assertEquals(BreakerState.CLOSED,b.state());coherent(b);
        } finally {pool.shutdownNow();}
    }
}
