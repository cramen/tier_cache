package io.tiercache.spring;

import io.tiercache.*;
import io.tiercache.spi.*;
import io.tiercache.testkit.*;
import org.junit.jupiter.api.Test;
import org.springframework.cache.Cache;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class AsyncRetrievalTest {
    static class GatedRemote implements RemoteCache<Object,Object> {
        final CountingRemoteCache<Object,Object> delegate=new CountingRemoteCache<>();
        final CountDownLatch entered=new CountDownLatch(1),release=new CountDownLatch(1);
        volatile boolean gated=true;
        volatile Thread ioThread;
        public StoredEntry<Object> get(Object key){
            ioThread=Thread.currentThread();
            if(gated){entered.countDown();await(release);}return delegate.get(key);
        }
        public void put(Object k,StoredEntry<Object> v,Duration ttl){delegate.put(k,v,ttl);}
        public boolean setIfAbsent(Object k,StoredEntry<Object> v,Duration ttl){return delegate.setIfAbsent(k,v,ttl);}
        public void evict(Object k){delegate.evict(k);}
        public void clear(){delegate.clear();}
    }
    static void await(CountDownLatch latch){
        try{assertTrue(latch.await(5,TimeUnit.SECONDS));}catch(InterruptedException e){Thread.currentThread().interrupt();throw new AssertionError(e);}
    }
    static Cache managed(TierCacheFactory factory){return new TierCacheManager(factory).getCache("c");}
    @Test void retrieveReturnsBeforeGatedL2AndWarmsL1() throws Exception {
        var remote=new GatedRemote();remote.delegate.put("x",StoredEntry.ofValue("value"),Duration.ofMinutes(1));
        var caller=Executors.newSingleThreadExecutor();var callerThread=new AtomicReference<Thread>();
        try(var factory=TierCacheFactory.builder().remoteCache(remote).build()) {
            var cache=managed(factory);
            try {
                var invocation=caller.submit(()->{callerThread.set(Thread.currentThread());return cache.retrieve("x");});
                await(remote.entered);
                var future=invocation.get(1,TimeUnit.SECONDS);
                assertNotNull(future);assertFalse(future.isDone());assertNotSame(callerThread.get(),remote.ioThread);
                remote.gated=false;remote.release.countDown();
                assertEquals("value",((Cache.ValueWrapper)future.get(2,TimeUnit.SECONDS)).get());
                int reads=remote.delegate.gets.get();
                assertEquals("value",((Cache.ValueWrapper)cache.retrieve("x").get(2,TimeUnit.SECONDS)).get());
                assertEquals(reads,remote.delegate.gets.get());
            } finally {remote.release.countDown();caller.shutdownNow();}
        }
    }
    static Throwable root(Throwable error) {
        while((error instanceof CompletionException || error instanceof ExecutionException) && error.getCause()!=null) error=error.getCause();
        return error;
    }
    static Throwable failure(CompletableFuture<?> future) {
        try {future.get(3,TimeUnit.SECONDS);throw new AssertionError("expected failure");}
        catch(ExecutionException | CancellationException e){return root(e);}
        catch(Exception e){throw new AssertionError(e);}
    }
    static TierCacheFactory factory(NullPolicy policy) {
        return TierCacheFactory.builder().remoteCache(new InMemoryRemoteCache<>())
                .cache("c",new CacheOverride().nullPolicy(policy)).asyncExecutorThreads(4).build();
    }
    @Test void singleArgumentDistinguishesHitCachedNullAndMissAndSuppressesHitSuppliers() throws Exception {
        try(var factory=factory(NullPolicy.allow(Duration.ofMinutes(1)))) {
            var cache=managed(factory);cache.put("value","v");cache.put("null",null);
            assertEquals("v",((Cache.ValueWrapper)cache.retrieve("value").get(2,TimeUnit.SECONDS)).get());
            var marker=cache.retrieve("null").get(2,TimeUnit.SECONDS);assertInstanceOf(Cache.ValueWrapper.class,marker);
            assertNull(((Cache.ValueWrapper)marker).get());assertNull(cache.retrieve("absent").get(2,TimeUnit.SECONDS));
            assertEquals("v",cache.retrieve("value",()->{fail("supplier on hit");return null;}).get(2,TimeUnit.SECONDS));
            assertNull(cache.retrieve("null",()->{fail("supplier on cached null");return null;}).get(2,TimeUnit.SECONDS));
        }
    }
    @Test void supplierNullFollowsAllowAndDenyWithoutExposingSpringWrappers() throws Exception {
        for(boolean allow:new boolean[]{false,true})try(var factory=factory(allow?NullPolicy.allow(Duration.ofMinutes(1)):NullPolicy.deny())) {
            var cache=managed(factory);var calls=new AtomicInteger();
            java.util.function.Supplier<CompletableFuture<String>> supplier=()->{calls.incrementAndGet();return CompletableFuture.completedFuture(null);};
            assertNull(cache.retrieve("x",supplier).get(2,TimeUnit.SECONDS));assertNull(cache.retrieve("x",supplier).get(2,TimeUnit.SECONDS));
            assertEquals(allow?1:2,calls.get());var result=cache.retrieve("x").get(2,TimeUnit.SECONDS);
            if(allow){assertInstanceOf(Cache.ValueWrapper.class,result);assertNull(((Cache.ValueWrapper)result).get());}else assertNull(result);
        }
    }
    @Test void loaderThrowsExceptionalStageAndCancellationPreserveCauses() {
        try(var factory=factory(NullPolicy.deny())) {
            var cache=managed(factory);var thrown=new IllegalArgumentException("supplier");
            assertSame(thrown,failure(cache.retrieve("throws",()->{throw thrown;})));
            var failed=new java.io.IOException("stage");
            assertSame(failed,failure(cache.retrieve("failed",()->CompletableFuture.failedFuture(failed))));
            var cancelled=new CompletableFuture<String>();cancelled.cancel(false);
            assertInstanceOf(CancellationException.class,failure(cache.retrieve("cancelled",()->cancelled)));
        }
    }
    @Test void cancelledWaiterDoesNotCancelSharedSupplierOrOtherCaller() throws Exception {
        var entered=new CountDownLatch(1);var joined=new CountDownLatch(1);var source=new CompletableFuture<String>();
        var calls=new AtomicInteger();var loaderThread=new AtomicReference<Thread>();Thread caller=Thread.currentThread();
        try(var factory=TierCacheFactory.builder().remoteCache(new InMemoryRemoteCache<>()).asyncExecutorThreads(2)
                .metricsListener(new CacheMetricsListener(){public void onRequest(String c,Outcome o){if(o==Outcome.COALESCED)joined.countDown();}}).build()) {
            var cache=managed(factory);
            try {
                var first=cache.retrieve("shared",()->{calls.incrementAndGet();loaderThread.set(Thread.currentThread());entered.countDown();return source;});
                await(entered);var second=cache.retrieve("shared",()->{calls.incrementAndGet();return CompletableFuture.completedFuture("wrong");});
                await(joined);assertTrue(first.cancel(true));assertFalse(source.isCancelled());assertFalse(second.isDone());
                source.complete("value");assertEquals("value",second.get(2,TimeUnit.SECONDS));assertEquals(1,calls.get());assertNotSame(caller,loaderThread.get());
                assertTrue(first.isCancelled());
            } finally {source.complete("cleanup");}
        }
    }
    @Test void closeSettlesRunningAndQueuedLookupsAndRejectsLaterRetrieval() throws Exception {
        var remote=new GatedRemote();var factory=TierCacheFactory.builder().remoteCache(remote).asyncExecutorThreads(1).build();
        var manager=new TierCacheManager(factory);var cache=manager.getCache("c");
        try {
            cache.put("done","v");var completed=cache.retrieve("done");assertEquals("v",((Cache.ValueWrapper)completed.get(2,TimeUnit.SECONDS)).get());
            var running=cache.retrieve("running");await(remote.entered);var queued=cache.retrieve("queued");
            var supplierCalls=new AtomicInteger();var queuedSupplier=cache.retrieve("queued-loader",()->{supplierCalls.incrementAndGet();return CompletableFuture.completedFuture("bad");});
            factory.close();
            assertInstanceOf(CancellationException.class,failure(running));assertInstanceOf(CancellationException.class,failure(queued));
            assertInstanceOf(CancellationException.class,failure(queuedSupplier));assertEquals(0,supplierCalls.get());
            assertInstanceOf(CancellationException.class,failure(cache.retrieve("later")));
            assertInstanceOf(CancellationException.class,failure(cache.retrieve("later-loader",()->{fail("loader after close");return null;})));
            assertEquals("v",((Cache.ValueWrapper)completed.get()).get());
            assertThrows(IllegalStateException.class,()->manager.getCache("never-created"));
            assertFalse(manager.getCacheNames().contains("never-created"));
        } finally {remote.release.countDown();factory.close();}
    }
    @Test void closeSettlesRunningSupplierWithoutCancellingApplicationStage() {
        var source=new CompletableFuture<String>();var entered=new CountDownLatch(1);
        var factory=factory(NullPolicy.deny());var cache=managed(factory);
        try {
            var result=cache.retrieve("x",()->{entered.countDown();return source;});await(entered);factory.close();
            assertInstanceOf(CancellationException.class,failure(result));assertFalse(source.isCancelled());
        } finally {source.complete("late-value");factory.close();}
    }
    @Test void saturationFailsBothOverloadsWithoutCallerRunsOrSupplierExecution() throws Exception {
        var source=new CompletableFuture<String>();var entered=new CountDownLatch(1);var calls=new AtomicInteger();
        var factory=TierCacheFactory.builder().remoteCache(new InMemoryRemoteCache<>()).asyncExecutorThreads(1).build();
        try {
            var cache=managed(factory);cache.retrieve("busy",()->{entered.countDown();return source;});await(entered);
            var field=TierCacheFactory.class.getDeclaredField("asyncExecutor");field.setAccessible(true);var executor=(ThreadPoolExecutor)field.get(factory);
            int capacity=executor.getQueue().remainingCapacity();assertTrue(capacity>0);
            for(int i=0;i<capacity;i++)executor.execute(()->{});
            assertInstanceOf(RejectedExecutionException.class,failure(cache.retrieve("overflow")));
            assertInstanceOf(RejectedExecutionException.class,failure(cache.retrieve("overflow-loader",()->{calls.incrementAndGet();return CompletableFuture.completedFuture("bad");})));
            assertEquals(0,calls.get());assertEquals(1,executor.getPoolSize());
        } finally {factory.close();source.complete("cleanup");}
    }
    @Test void legacyConstructorRetainsSyncMethodsButRejectsBothAsyncOverloads() {
        try(var factory=factory(NullPolicy.deny())) {
            var cache=new TierCacheSpringCache("c",factory.getCache("c"));cache.put("x","v");assertEquals("v",cache.get("x").get());
            var error=failure(cache.retrieve("x"));assertInstanceOf(UnsupportedOperationException.class,error);assertTrue(error.getMessage().contains("TierCacheManager"));
            assertInstanceOf(UnsupportedOperationException.class,failure(cache.retrieve("x",()->{fail("legacy supplier");return null;})));
            cache.evict("x");assertNull(cache.get("x"));assertEquals("loaded",cache.get("x",()->"loaded"));
        }
    }

}
