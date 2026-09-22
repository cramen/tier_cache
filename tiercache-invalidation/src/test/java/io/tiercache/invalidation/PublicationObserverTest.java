package io.tiercache.invalidation;

import io.tiercache.*;
import io.tiercache.spi.*;
import io.tiercache.testkit.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import static org.junit.jupiter.api.Assertions.*;

class PublicationObserverTest {
    static class Metrics implements CacheMetricsListener {
        final AtomicLongArray outcomes=new AtomicLongArray(4);
        final AtomicInteger sent=new AtomicInteger();
        volatile Thread callbackThread;
        public void onInvalidation(String c,Direction d){if(d==Direction.SENT)sent.incrementAndGet();}
        public void onPublication(String c,PublicationOutcome outcome,long count){callbackThread=Thread.currentThread();outcomes.addAndGet(outcome.ordinal(),count);}
        long count(PublicationOutcome outcome){return outcomes.get(outcome.ordinal());}
    }
    static class Transport implements InvalidationTransport {
        final AtomicInteger calls=new AtomicInteger();
        volatile Supplier<CompletionStage<PublicationOutcome>> action=()->CompletableFuture.completedFuture(PublicationOutcome.ACKNOWLEDGED);
        public void publish(InvalidationMessage message){throw new AssertionError("async override must be used");}
        public CompletionStage<PublicationOutcome> publishAsync(InvalidationMessage m){calls.incrementAndGet();return action.get();}
        public AutoCloseable subscribe(String c,Consumer<InvalidationMessage> h){return ()->{};}
        public void close(){}
    }
    static void await(BooleanSupplier condition)throws Exception {
        long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
        while(!condition.getAsBoolean()&&System.nanoTime()<end)Thread.sleep(5);
        assertTrue(condition.getAsBoolean());
    }
    static void gate(CountDownLatch latch) {
        try{assertTrue(latch.await(5,TimeUnit.SECONDS));}catch(InterruptedException e){throw new AssertionError(e);}
    }
    static Version version(){return new Version(1,UUID.randomUUID());}
    static InvalidationMessage message(){var v=version();return new InvalidationMessage("c","secret-key",v,v.instanceId(),InvalidationMessage.Type.INVALIDATE);}
    @Test void delayedFailureDoesNotBlockDataWriteAndOnlyCountsFailed() throws Exception {
        var transport=new Transport();var future=new CompletableFuture<PublicationOutcome>();transport.action=()->future;
        var metrics=new Metrics();var remote=new InMemoryRemoteCache<>();
        try(var factory=TierCacheFactory.builder().remoteCache(remote).invalidation(v->new InvalidationService(transport,null,v.instanceId(),InvalidationListener.NOOP,metrics)).build()) {
            var cache=factory.getCache("c");cache.put("x","committed");assertFalse(future.isDone());assertEquals("committed",remote.get("x").value());
            assertEquals(1,metrics.sent.get());assertEquals(0,metrics.count(PublicationOutcome.ACKNOWLEDGED));
            Thread io=new Thread(()->future.completeExceptionally(new IllegalStateException("secret-payload")),"simulated-lettuce-io");io.start();io.join();
            await(()->metrics.count(PublicationOutcome.FAILED)==1);assertNotSame(io,metrics.callbackThread);assertEquals(1,transport.calls.get());
            assertEquals(0,metrics.count(PublicationOutcome.ACKNOWLEDGED));factory.close();cache.put("after","still-usable");
            assertEquals(1,transport.calls.get());assertEquals(1,metrics.sent.get());
        }
    }
    @Test void cancellationSynchronousThrowAndNullStageBecomeOneFailureEach() throws Exception {
        var transport=new Transport();var metrics=new Metrics();
        try(var service=new InvalidationService(transport,null,UUID.randomUUID(),InvalidationListener.NOOP,metrics)) {
            transport.action=()->{throw new IllegalArgumentException("secret");};service.onLocalWrite("c","x",version(),InvalidationMessage.Type.INVALIDATE);
            transport.action=()->{var f=new CompletableFuture<PublicationOutcome>();f.cancel(false);return f;};service.onLocalWrite("c","x",version(),InvalidationMessage.Type.INVALIDATE);
            transport.action=()->null;service.onLocalUpdate("c","x","payload",version());
            await(()->metrics.count(PublicationOutcome.FAILED)==3);assertEquals(3,metrics.sent.get());assertEquals(3,transport.calls.get());
        }
    }
    @Test void duplicateCompletionCannotDoubleCount() throws Exception {
        var metrics=new Metrics();
        try(var observer=new PublicationObserver(metrics,System::nanoTime)) {
            observer.register("c");var attempt=observer.admit("c");
            attempt.complete(PublicationOutcome.ACKNOWLEDGED,null);attempt.complete(null,new IllegalStateException());
            await(()->metrics.count(PublicationOutcome.ACKNOWLEDGED)==1);
            assertEquals(0,attempt.count(PublicationOutcome.FAILED));assertEquals(1,attempt.count(PublicationOutcome.ACKNOWLEDGED));
        }
    }
    @Test void legacyVoidTransportIsUnconfirmedAndLegacyListenerStillWorks() throws Exception {
        var calls=new AtomicInteger();InvalidationTransport legacy=new InvalidationTransport(){
            public void publish(InvalidationMessage m){calls.incrementAndGet();}
            public AutoCloseable subscribe(String c,Consumer<InvalidationMessage> h){return ()->{};}
            public void close(){}
        };
        assertEquals(PublicationOutcome.UNCONFIRMED,legacy.publishAsync(message()).toCompletableFuture().get());assertEquals(1,calls.get());
        new CacheMetricsListener(){}.onPublication("c",PublicationOutcome.UNCONFIRMED,1);
        var metrics=new Metrics();try(var service=new InvalidationService(legacy,null,UUID.randomUUID(),InvalidationListener.NOOP,metrics)) {
            service.onLocalWrite("c","x",version(),InvalidationMessage.Type.INVALIDATE);
            await(()->metrics.count(PublicationOutcome.UNCONFIRMED)==1);assertEquals(2,calls.get());
        }
    }
    @Test void burstAndConcurrentDrainKeepBoundedTokensAndExactTotals() throws Exception {
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var block=new AtomicBoolean(true);var metrics=new Metrics(){
            public void onPublication(String c,PublicationOutcome o,long n){if(block.compareAndSet(true,false)){entered.countDown();gate(release);}super.onPublication(c,o,n);}
        };
        var observer=new PublicationObserver(metrics,System::nanoTime);var producers=Executors.newFixedThreadPool(4);
        try {
            observer.register("a");observer.register("b");observer.admit("a").complete(PublicationOutcome.ACKNOWLEDGED,null);gate(entered);
            var jobs=new ArrayList<Future<?>>();
            for(int thread=0;thread<4;thread++)jobs.add(producers.submit(()->{
                for(int i=0;i<10000;i++)observer.admit(i%2==0?"b":"unknown-"+i).complete(PublicationOutcome.ACKNOWLEDGED,null);
            }));
            for(var job:jobs)job.get(5,TimeUnit.SECONDS);
            assertEquals(3,observer.bucketCount());assertTrue(observer.pendingTokens()<=3);
            release.countDown();await(()->metrics.count(PublicationOutcome.ACKNOWLEDGED)==40001);
            jobs.clear();for(int t=0;t<4;t++)jobs.add(producers.submit(()->{for(int i=0;i<1000;i++)observer.admit("a").complete(PublicationOutcome.NOT_REQUIRED,null);}));
            for(var job:jobs)job.get(5,TimeUnit.SECONDS);await(()->metrics.count(PublicationOutcome.NOT_REQUIRED)==4000);
        } finally {release.countDown();observer.close();producers.shutdownNow();}
    }
    @Test void throwingObserverIsNotRetriedAndDoesNotChangeInternalOutcome() throws Exception {
        var calls=new AtomicInteger();
        try(var observer=new PublicationObserver(new CacheMetricsListener(){public void onPublication(String c,PublicationOutcome o,long n){calls.incrementAndGet();throw new IllegalStateException("observer secret");}},System::nanoTime)) {
            var attempt=observer.admit("c");attempt.complete(PublicationOutcome.FAILED,null);await(()->calls.get()==1);
            assertEquals(1,attempt.count(PublicationOutcome.FAILED));observer.close();assertEquals(1,calls.get());
        }
    }
    @Test void closeDrainsForAtMostOneSecondAndLateCompletionDoesNotResurrectWorker() throws Exception {
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var calls=new AtomicInteger();
        var observer=new PublicationObserver(new CacheMetricsListener(){public void onPublication(String c,PublicationOutcome o,long n){
            calls.incrementAndGet();entered.countDown();boolean done=false;while(!done)try{done=release.await(5,TimeUnit.SECONDS);}catch(InterruptedException ignored){}
        }},System::nanoTime);
        try {
            var late=observer.admit("late");observer.admit("c").complete(PublicationOutcome.ACKNOWLEDGED,null);gate(entered);
            assertTimeoutPreemptively(java.time.Duration.ofSeconds(2),observer::close);
            assertNull(observer.admit("after"));late.complete(PublicationOutcome.ACKNOWLEDGED,null);assertEquals(2,late.count(PublicationOutcome.ACKNOWLEDGED));
            assertEquals(0,observer.pendingTokens());assertEquals(1,calls.get());
            release.countDown();observer.close();
        } finally {release.countDown();observer.close();}
    }
    @Test void observerCanCloseItselfWithoutWaitingOnItsOwnThread() throws Exception {
        var reference=new AtomicReference<PublicationObserver>();var completed=new CountDownLatch(1);
        var observer=new PublicationObserver(new CacheMetricsListener(){public void onPublication(String c,PublicationOutcome o,long n){reference.get().close();completed.countDown();}},System::nanoTime);reference.set(observer);
        observer.admit("c").complete(PublicationOutcome.ACKNOWLEDGED,null);assertTrue(completed.await(2,TimeUnit.SECONDS));observer.close();
    }
    static long warningDeadline(Object bucket) {
        try{var f=bucket.getClass().getDeclaredField("nextWarning");f.setAccessible(true);return f.getLong(bucket);}
        catch(ReflectiveOperationException e){throw new AssertionError(e);}
    }
    @Test void warningRateLimitWorksWithoutMetricsAndRetainsNoThrowable() throws Exception {
        var now=new AtomicLong(1);
        try(var observer=new PublicationObserver(CacheMetricsListener.NOOP,now::get)) {
            observer.register("c");var first=observer.admit("c");var field=first.getClass().getDeclaredField("bucket");field.setAccessible(true);var bucket=field.get(first);
            first.complete(null,new CompletionException(new IllegalArgumentException("sensitive payload")));
            await(()->warningDeadline(bucket)!=0);long deadline=warningDeadline(bucket);
            for(int i=0;i<10000;i++)observer.admit("c").complete(null,new IllegalArgumentException("sensitive payload"));
            observer.close();assertEquals(deadline,warningDeadline(bucket));assertEquals(10001,first.count(PublicationOutcome.FAILED));
            for(var member:bucket.getClass().getDeclaredFields())assertFalse(Throwable.class.isAssignableFrom(member.getType()));
        }
        try(var observer=new PublicationObserver(CacheMetricsListener.NOOP,now::get)) {
            var first=observer.admit("unknown");var field=first.getClass().getDeclaredField("bucket");field.setAccessible(true);var bucket=field.get(first);
            first.complete(PublicationOutcome.FAILED,null);await(()->warningDeadline(bucket)!=0);
            long deadline=warningDeadline(bucket);now.addAndGet(TimeUnit.SECONDS.toNanos(31));
            observer.admit("different-unknown").complete(PublicationOutcome.FAILED,null);await(()->warningDeadline(bucket)>deadline);
            assertEquals(1,observer.bucketCount());
        }
    }

    @Test void firstCompletionAfterCloseDoesNotStartAWorker() throws Exception {
        var metrics=new Metrics();var observer=new PublicationObserver(metrics,System::nanoTime);
        var attempt=observer.admit("not-registered");observer.close();
        attempt.complete(PublicationOutcome.ACKNOWLEDGED,null);
        var field=PublicationObserver.class.getDeclaredField("workerThread");field.setAccessible(true);
        assertNull(field.get(observer));assertEquals(1,attempt.count(PublicationOutcome.ACKNOWLEDGED));
        assertEquals(0,metrics.count(PublicationOutcome.ACKNOWLEDGED));assertEquals(0,observer.pendingTokens());
        observer.register("late-registration");assertEquals(1,observer.bucketCount());
    }

}
