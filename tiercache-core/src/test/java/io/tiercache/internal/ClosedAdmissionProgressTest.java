package io.tiercache.internal;
import io.tiercache.BreakerState;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;
class ClosedAdmissionProgressTest {
    @Test void oldClosedAdmissionProgressesUntilOpeningIsPublished() throws Exception {
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var once=new AtomicBoolean();
        var breaker=new CircuitBreaker(new CircuitBreaker.Config(2,1,1,Duration.ofDays(1),1),BreakerAdmissionContractTest.NOOP,()->{
            if(once.compareAndSet(false,true)) { entered.countDown(); try { assertTrue(release.await(5,TimeUnit.SECONDS)); }catch(InterruptedException e){throw new AssertionError(e);} }
            return 0L;
        });
        var pool=Executors.newFixedThreadPool(2);
        try {
            var opening=pool.submit(breaker::onFailure); assertTrue(entered.await(5,TimeUnit.SECONDS));
            // The real completion owns the monitor and has not published its transition yet.
            var admission=pool.submit(()->{
                assertFalse(breaker.isOpen());assertEquals(BreakerState.CLOSED,breaker.state());
                assertTrue(breaker.tryAcquire());return breaker.tryAcquirePermit();
            });
            CircuitBreaker.Permit old;
            try { old=admission.get(1,TimeUnit.SECONDS);assertNotNull(old); }
            finally { release.countDown();opening.get(5,TimeUnit.SECONDS); }
            assertEquals(BreakerState.OPEN,breaker.state());assertNull(breaker.tryAcquirePermit());
            old.failure();old.success();old.cancel();
            assertEquals(1,BreakerAdmissionContractTest.count(breaker,"windowCount"));
        } finally { release.countDown();pool.shutdownNow(); }
    }
}
