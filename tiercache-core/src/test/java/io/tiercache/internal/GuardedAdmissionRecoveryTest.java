package io.tiercache.internal;
import io.tiercache.*;
import io.tiercache.spi.*;
import io.tiercache.testkit.CountingLocalCache;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;
class GuardedAdmissionRecoveryTest {
    @Test void rejectionProbeBudgetAndDetachedRecoveryPreserveFullCachePath() throws Exception {
        var now=new AtomicLong();var closes=new AtomicInteger();var calls=new AtomicInteger();var fail=new AtomicBoolean(true);
        var entered=new CountDownLatch(2);var release=new CountDownLatch(1);var gate=new AtomicBoolean();
        var b=new CircuitBreaker(new CircuitBreaker.Config(4,1,1,Duration.ofNanos(100),2),new CircuitBreaker.Listener(){
            public void onOpen(){} public void onClose(){closes.incrementAndGet();}
        },now::get);
        RemoteCache<String,String> delegate=new RemoteCache<>() {
            public StoredEntry<String> get(String key) {
                calls.incrementAndGet();if(fail.get())throw new IllegalStateException("Redis unavailable");
                if(gate.get()) {entered.countDown();try{assertTrue(release.await(5,TimeUnit.SECONDS));}catch(InterruptedException e){throw new AssertionError(e);}}
                return StoredEntry.ofValue("value");
            }
            public void put(String k,StoredEntry<String> e,Duration t){} public void evict(String k){}public void clear(){}
            public boolean setIfAbsent(String k,StoredEntry<String> e,Duration t){return false;}
        };
        var cache=new DefaultTierCache<>("c",new CountingLocalCache<String,String>(),new CircuitBreakerRemoteCache<>(delegate,b),
                CacheSettings.defaults(),true,null,null,null,null,b,CacheMetricsListener.NOOP);
        var tasks=new ConcurrentLinkedQueue<Runnable>();var recovered=new CompletableFuture<Boolean>();
        b.configureRecovery(tasks::add,()->{assertFalse(Thread.holdsLock(b));return recovered;});
        assertNull(cache.get("outage"));assertEquals(1,calls.get());
        for(int i=0;i<20;i++)assertNull(cache.get("rejected"+i));assertEquals(1,calls.get());
        fail.set(false);now.set(100);gate.set(true);var pool=Executors.newFixedThreadPool(2);
        try {
            var a=pool.submit(()->cache.get("probe-a"));var c=pool.submit(()->cache.get("probe-b"));
            assertTrue(entered.await(5,TimeUnit.SECONDS));assertNull(cache.get("over-budget"));assertEquals(3,calls.get());
            gate.set(false);release.countDown();assertEquals("value",a.get(5,TimeUnit.SECONDS));assertEquals("value",c.get(5,TimeUnit.SECONDS));
            assertEquals(BreakerState.HALF_OPEN,b.state());assertEquals(1,tasks.size());
            assertNull(cache.get("pending-recovery"));assertEquals(3,calls.get());tasks.remove().run();
            b.detachRecovery();recovered.complete(true);assertEquals(0,closes.get());assertEquals(BreakerState.HALF_OPEN,b.state());
            assertEquals("value",cache.get("fresh-probe-a"));assertEquals("value",cache.get("fresh-probe-b"));
            assertEquals(5,calls.get());assertEquals(BreakerState.CLOSED,b.state());assertEquals(1,closes.get());
        } finally {gate.set(false);release.countDown();pool.shutdownNow();}
    }
}
