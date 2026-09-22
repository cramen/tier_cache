package io.tiercache.internal;

import io.tiercache.*;
import io.tiercache.spi.*;
import io.tiercache.testkit.*;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Function;
import static org.junit.jupiter.api.Assertions.*;

class AuxiliaryLifecycleTest {
    static class RejectingScheduler extends ScheduledThreadPoolExecutor {
        final boolean closing;
        final List<String> events;
        final RejectedExecutionException failure = new RejectedExecutionException("test");
        RejectingScheduler(boolean closing, List<String> events) { super(1); this.closing=closing; this.events=events; }
        @Override public ScheduledFuture<?> scheduleAtFixedRate(Runnable r,long a,long b,TimeUnit u) {
            events.add("schedule"); if (closing) shutdown(); throw failure;
        }
    }
    @Test void watchdogRejectionReleasesBeforeFallbackInBothPaths() throws Exception {
        for (boolean refresh : new boolean[]{false,true}) for (boolean failLoader : new boolean[]{false,true}) {
            var events = new ArrayList<String>(); var failure = new IllegalArgumentException("loader");
            var scheduler = new RejectingScheduler(true,events);
            var cache = cache(events,scheduler);
            Function<String,String> loader = k -> { events.add("load"); if(failLoader) throw failure; return "value"; };
            try {
                if (refresh) {
                    var method=DefaultTierCache.class.getDeclaredMethod("revalidate",Object.class,Function.class,long.class);
                    method.setAccessible(true);
                    if (failLoader) assertSame(failure, assertThrows(java.lang.reflect.InvocationTargetException.class,
                            () -> method.invoke(cache,"x",loader,0L)).getCause());
                    else assertEquals("value", ((LoadClaim.Outcome<?>)method.invoke(cache,"x",loader,0L)).entry().value());
                } else if (failLoader) assertSame(failure,assertThrows(IllegalArgumentException.class,()->cache.getOrCompute("x",loader)));
                else assertEquals("value",cache.getOrCompute("x",loader));
                assertEquals(List.of("acquire","schedule","release","load"),events);
            } finally { scheduler.shutdownNow(); }
        }
    }
    static DefaultTierCache<String,String> cache(List<String> events, ScheduledExecutorService scheduler) {
        DistributedLockProvider provider=(n,d)->{
            events.add("acquire"); return new DistributedLock() {
                public boolean extend(Duration lease) { return true; }
                public void release() { events.add("release"); }
            };
        };
        return new DefaultTierCache<>("c",new CountingLocalCache<>(),new InMemoryRemoteCache<>(),
                CacheSettings.defaults(),true,provider,scheduler,null,null);
    }
    @Test void unrelatedLiveSchedulerRejectionRemainsVisible() {
        var events=new ArrayList<String>(); var scheduler=new RejectingScheduler(false,events);
        try {
            assertSame(scheduler.failure,assertThrows(RejectedExecutionException.class,
                    ()->cache(events,scheduler).getOrCompute("x",k->{ fail("must not load"); return null; })));
            assertEquals(List.of("acquire","schedule","release"),events);
        } finally { scheduler.shutdownNow(); }
    }
    @Test void doubleCheckHitReleasesOnceWithoutScheduling() {
        var l2=new InMemoryRemoteCache<String,String>(); var releases=new AtomicInteger();
        var scheduler=new RejectingScheduler(false,new ArrayList<>());
        try {
            DistributedLockProvider provider=(n,d)->{
                l2.put("x",StoredEntry.ofValue("winner"),Duration.ofMinutes(1));
                return new DistributedLock() { public boolean extend(Duration d){return true;} public void release(){releases.incrementAndGet();} };
            };
            var c=new DefaultTierCache<>("c",new CountingLocalCache<String,String>(),l2,CacheSettings.defaults(),true,provider,scheduler,null,null);
            assertEquals("winner",c.getOrCompute("x",k->{fail("no load");return null;})); assertEquals(1,releases.get());
        } finally { scheduler.shutdownNow(); }
    }
    @Test void factoryCloseDisablesAuxiliariesButPreservesBorrowedResourcesAndSyncView() {
        var acquisitions=new AtomicInteger(); var publications=new AtomicInteger(); var closes=new AtomicInteger();
        class Provider implements DistributedLockProvider,AutoCloseable {
            public DistributedLock tryLock(String n,Duration d){acquisitions.incrementAndGet();throw new AssertionError("post-close acquire");}
            public void close(){closes.incrementAndGet();}
        }
        var l2=new InMemoryRemoteCache<String,String>();
        var factory=TierCacheFactory.builder().remoteCache(l2).lockProvider(new Provider()).invalidation(v->new InvalidationHandler(){
            public void registerTarget(String c,InvalidationTarget t){}
            public void onLocalWrite(String c,Object k,Version v,InvalidationMessage.Type t){publications.incrementAndGet();}
            public void close(){closes.incrementAndGet();}
        }).build();
        TierCache<String,String> c=factory.getCache("c"); var async=factory.asyncCache("c"); factory.close(); factory.close();
        assertEquals("loaded",c.getOrCompute("x",k->"loaded")); c.put("y","written");
        assertEquals("written",l2.get("y").value()); c.evict("x"); c.evictAll();
        assertEquals(0,acquisitions.get()); assertEquals(0,publications.get()); assertEquals(1,closes.get());
        assertThrows(RuntimeException.class,()->async.getAsync("x").toCompletableFuture().join());
    }
    @Test void discardedRefreshClaimDoesNotStrandForegroundReader() throws Exception {
        var queue=new ArrayDeque<Runnable>(); var l2=new InMemoryRemoteCache<String,String>();
        var settings=new CacheSettings(100,Duration.ofMinutes(1),null,Duration.ofMinutes(1),0,NullPolicy.deny(),
                InvalidationMode.INVALIDATE,65536,Duration.ofMinutes(5),false,Duration.ofSeconds(1));
        var c=new DefaultTierCache<>("c",new CountingLocalCache<String,String>(),l2,settings,true,null,null,null,null,
                null,CacheMetricsListener.NOOP,queue::add);
        l2.put("x",StoredEntry.ofValue("stale",new Version(1,UUID.randomUUID()),System.currentTimeMillis()-120000),Duration.ofMinutes(10));
        assertEquals("stale",c.getOrCompute("x",k->"refresh")); assertEquals(1,queue.size());
        l2.evict("x");
        var pool=Executors.newSingleThreadExecutor();
        try {
            var foreground=pool.submit(()->c.getOrCompute("x",k->"foreground"));
            ((DefaultTierCache.DiscardableTask)queue.remove()).discard();
            assertEquals("foreground",foreground.get(2,TimeUnit.SECONDS));
        } finally {pool.shutdownNow();}
    }
    @Test void closedProviderIsNeutralInClosedAndHalfOpenAndLateEpoch() throws Exception {
        var b=new CircuitBreaker(new CircuitBreaker.Config(2,1,1,Duration.ZERO,1),new CircuitBreaker.Listener(){public void onOpen(){}public void onClose(){}});
        var wrapped=new BreakerLockProvider((n,d)->{throw new LockProviderClosedException();},b);
        assertThrows(LockProviderClosedException.class,()->wrapped.tryLock("x",Duration.ofSeconds(1)));
        assertEquals(BreakerState.CLOSED,b.state()); b.onFailure();
        assertThrows(LockProviderClosedException.class,()->wrapped.tryLock("x",Duration.ofSeconds(1)));
        assertEquals(BreakerState.HALF_OPEN,b.state());
        var old=b.tryAcquirePermit(); assertNotNull(old); b.onFailure();
        var current=b.tryAcquirePermit(); assertNotNull(current); old.cancel(); old.cancel();
        assertNull(b.tryAcquirePermit()); current.success(); assertEquals(BreakerState.CLOSED,b.state());
    }
    @Test void closedProviderFallbackDoesNotWaitForContention() {
        var scheduler=Executors.newSingleThreadScheduledExecutor();
        try {
            var c=new DefaultTierCache<>("c",new CountingLocalCache<String,String>(),new InMemoryRemoteCache<String,String>(),
                    CacheSettings.defaults(),true,(n,d)->{throw new LockProviderClosedException();},scheduler,null,null);
            assertTimeoutPreemptively(Duration.ofSeconds(1),()->assertEquals("v",c.getOrCompute("x",k->"v")));
        } finally {scheduler.shutdownNow();}
    }
    @Test void factoryCloseRetiresPendingRecoveryWithoutFalseRecoveryNotification() throws Exception {
        var pending=new CompletableFuture<Boolean>(); var entered=new CountDownLatch(1);
        var recovered=new AtomicInteger(); var l2=new InMemoryRemoteCache<String,String>();
        var factory=TierCacheFactory.builder().remoteCache(l2)
                .circuitBreakerConfig(new CircuitBreaker.Config(2,1,1,Duration.ZERO,1))
                .degradationListener(new DegradationListener(){public void onDegraded(){}public void onRecovered(){recovered.incrementAndGet();}})
                .invalidation(v->new InvalidationHandler(){
                    public void registerTarget(String c,InvalidationTarget t){}
                    public void onLocalWrite(String c,Object k,Version v,InvalidationMessage.Type t){}
                    public CompletionStage<Boolean> recoverAsync(Executor e){entered.countDown();return pending;}
                    public void close(){}
                }).build();
        try {
            TierCache<String,String> cache=factory.getCache("c");
            var field=TierCacheFactory.class.getDeclaredField("breaker");field.setAccessible(true);
            var breaker=(CircuitBreaker)field.get(factory);breaker.onFailure();cache.get("missing");
            assertTrue(entered.await(5,TimeUnit.SECONDS));assertNull(breaker.tryAcquirePermit());
            factory.close();pending.complete(true);assertEquals(0,recovered.get());
            l2.put("after",StoredEntry.ofValue("healthy"),Duration.ofMinutes(1));
            assertEquals("healthy",cache.get("after"));assertEquals(BreakerState.CLOSED,breaker.state());
            assertEquals(0,recovered.get());
        } finally {factory.close();}
    }
    @Test void factoryShutdownDiscardsActualQueuedRefresh() throws Exception {
        var l2=new InMemoryRemoteCache<String,String>();
        var settings=new CacheSettings(100,Duration.ofMinutes(1),null,Duration.ofMinutes(1),0,NullPolicy.deny(),
                InvalidationMode.INVALIDATE,65536,Duration.ofMinutes(5),false,Duration.ofSeconds(1));
        var factory=TierCacheFactory.builder().remoteCache(l2).defaults(settings).build();
        var gate=new CountDownLatch(1);
        try {
            var f=TierCacheFactory.class.getDeclaredField("revalidationExecutor");f.setAccessible(true);
            var executor=(ThreadPoolExecutor)f.get(factory);var started=new CountDownLatch(executor.getCorePoolSize());
            for(int i=0;i<executor.getCorePoolSize();i++) executor.execute(()->{started.countDown();try{gate.await();}catch(InterruptedException ignored){}});
            assertTrue(started.await(5,TimeUnit.SECONDS));
            TierCache<String,String> cache=factory.getCache("c");
            l2.put("x",StoredEntry.ofValue("stale",new Version(1,UUID.randomUUID()),System.currentTimeMillis()-120000),Duration.ofMinutes(10));
            assertEquals("stale",cache.getOrCompute("x",k->{fail("discarded refresh ran");return null;}));
            assertEquals(1,executor.getQueue().size());factory.close();l2.evict("x");
            assertTimeoutPreemptively(Duration.ofSeconds(2),()->assertEquals("new",cache.getOrCompute("x",k->"new")));
        } finally {gate.countDown();factory.close();}
    }

    @Test void closeAfterAcquisitionReleasesBeforeLoaderWithoutScheduling() {
        var open=new AtomicBoolean(true); var events=new ArrayList<String>();
        var scheduler=new RejectingScheduler(false,events);
        DistributedLockProvider provider=(n,d)->{
            open.set(false);events.add("acquire");
            return new DistributedLock(){public boolean extend(Duration d){fail("renewal after close");return false;}public void release(){events.add("release");}};
        };
        try {
            var cache=new DefaultTierCache<>("c",new CountingLocalCache<String,String>(),new InMemoryRemoteCache<String,String>(),
                    CacheSettings.defaults(),true,provider,scheduler,null,null,null,CacheMetricsListener.NOOP,null,new TtlJitter(),open::get);
            assertEquals("v",cache.getOrCompute("x",k->{events.add("load");return "v";}));
            assertEquals(List.of("acquire","release","load"),events);
        } finally {scheduler.shutdownNow();}
    }
    @Test void admittedRenewalStopsCallingProviderAfterAuxiliaryClose() {
        var open=new AtomicBoolean(true); var renewals=new AtomicInteger();var releases=new AtomicInteger();
        var callback=new AtomicReference<Runnable>();
        var scheduler=new ScheduledThreadPoolExecutor(1){
            @Override public ScheduledFuture<?> scheduleAtFixedRate(Runnable r,long a,long b,TimeUnit u){
                callback.set(r);return super.scheduleAtFixedRate(()->{},1,1,TimeUnit.DAYS);
            }
        };
        DistributedLockProvider provider=(n,d)->new DistributedLock(){public boolean extend(Duration d){renewals.incrementAndGet();return true;}public void release(){releases.incrementAndGet();}};
        try {
            var cache=new DefaultTierCache<>("c",new CountingLocalCache<String,String>(),new InMemoryRemoteCache<String,String>(),
                    CacheSettings.defaults(),true,provider,scheduler,null,null,null,CacheMetricsListener.NOOP,null,new TtlJitter(),open::get);
            assertEquals("v",cache.getOrCompute("x",k->{callback.get().run();open.set(false);callback.get().run();return "v";}));
            assertEquals(1,renewals.get());assertEquals(1,releases.get());
        } finally {scheduler.shutdownNow();}
    }
    @Test void dequeuedRefreshObservesCloseAndDiscardIsIdempotent() {
        var queue=new ArrayDeque<Runnable>();var open=new AtomicBoolean(true);var l2=new InMemoryRemoteCache<String,String>();
        var settings=new CacheSettings(100,Duration.ofMinutes(1),null,Duration.ofMinutes(1),0,NullPolicy.deny(),
                InvalidationMode.INVALIDATE,65536,Duration.ofMinutes(5),false,Duration.ofSeconds(1));
        var cache=new DefaultTierCache<>("c",new CountingLocalCache<String,String>(),l2,settings,true,null,null,null,null,
                null,CacheMetricsListener.NOOP,queue::add,new TtlJitter(),open::get);
        l2.put("x",StoredEntry.ofValue("stale",new Version(1,UUID.randomUUID()),System.currentTimeMillis()-120000),Duration.ofMinutes(10));
        assertEquals("stale",cache.getOrCompute("x",k->{fail("closed refresh must not load");return null;}));
        var task=(DefaultTierCache.DiscardableTask)queue.remove();open.set(false);task.run();task.discard();
        l2.evict("x");assertEquals("v",cache.getOrCompute("x",k->"v"));
    }

}
