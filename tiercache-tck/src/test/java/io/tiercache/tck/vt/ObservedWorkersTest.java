package io.tiercache.tck.vt;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
class ObservedWorkersTest {
    static final Duration BUDGET=Duration.ofSeconds(3), CLEANUP=Duration.ofSeconds(1);
    @Test void observesSuccessfulWorkersAndExactTotals() throws Throwable {
        var report=new HashMap<String,Object>();
        ObservedWorkers.run(Executors.newFixedThreadPool(2),10,i->assertTrue(i<10),BUDGET,CLEANUP,report);
        assertEquals(10,report.get("submitted"));assertEquals(10,report.get("completed"));assertEquals(true,report.get("terminated"));
    }
    @Test void errorsAndIncorrectValuesAreNotSwallowedByFutures() {
        for(boolean cold:new boolean[]{true,false}) {
            var failure=new AssertionError("incorrect value"); var report=new HashMap<String,Object>();
            assertSame(failure,assertThrows(AssertionError.class,()->ObservedWorkers.run(Executors.newFixedThreadPool(1),1,i->{throw failure;},BUDGET,CLEANUP,report)));
            assertEquals(0,report.get("completed"));assertEquals(true,report.get("terminated"));
            assertEquals(cold?"DIAGNOSTIC_COMPLETE":"PASS",VtEvidence.outcome(cold,Map.of("product",0)));
            // A zero-event policy result cannot override the workload failure above.
        }
    }
    @Test void cancellationIsAnObservedFailure() {
        var executor=new ThreadPoolExecutor(1,1,0,TimeUnit.SECONDS,new LinkedBlockingQueue<>()) {
            @Override public Future<?> submit(Runnable job) { var future=new FutureTask<Void>(job,null);future.cancel(false);return future; }
        };
        var report=new HashMap<String,Object>();
        assertThrows(CancellationException.class,()->ObservedWorkers.run(executor,1,i->{},BUDGET,CLEANUP,report));
        assertEquals(0,report.get("completed"));assertEquals(true,report.get("terminated"));
    }
    @Test void oneDeadlineCancelsStuckWorkersAndBoundsCleanup() {
        var interrupted=new CountDownLatch(1); var report=new HashMap<String,Object>();
        assertTimeoutPreemptively(Duration.ofSeconds(4),()-> {
            assertThrows(TimeoutException.class,()->ObservedWorkers.run(Executors.newSingleThreadExecutor(),2,i->{
                try { new CountDownLatch(1).await(); } catch(InterruptedException e) { interrupted.countDown();Thread.currentThread().interrupt(); }
            },Duration.ofMillis(100),CLEANUP,report));
        });
        assertEquals(0,interrupted.getCount());assertEquals(true,report.get("terminated"));
    }
    @Test void cleanupTimeoutIsSuppressedWithoutMaskingWorkerDeadline() throws Exception {
        var release=new CountDownLatch(1); var entered=new CountDownLatch(1);
        var executor=Executors.newSingleThreadExecutor(r->{var t=new Thread(r);t.setDaemon(true);return t;});
        var report=new HashMap<String,Object>();
        try {
            var error=assertThrows(TimeoutException.class,()->ObservedWorkers.run(executor,1,id->{
                entered.countDown();
                boolean done=false;
                while(!done) try { release.await();done=true; } catch(InterruptedException ignored) { }
            },Duration.ofMillis(200),Duration.ofMillis(30),report));
            assertEquals(0,entered.getCount());assertFalse((Boolean)report.get("terminated"));
            assertTrue(Arrays.stream(error.getSuppressed()).anyMatch(e->e instanceof TimeoutException));
        } finally { release.countDown();executor.shutdownNow();assertTrue(executor.awaitTermination(2,TimeUnit.SECONDS)); }
    }
}
