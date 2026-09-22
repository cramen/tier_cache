package io.tiercache.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.Test;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class LockOwnershipTest {
    static final Duration LEASE = Duration.ofSeconds(30);
    @SuppressWarnings("unchecked")
    static <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler);
    }
    static class Client extends RedisClient {
        final AtomicInteger connects = new AtomicInteger(), closes = new AtomicInteger(), sets = new AtomicInteger();
        volatile Runnable connecting = () -> {};
        volatile Runnable resolving = () -> {};
        final RedisCommands<String, String> commands = proxy(RedisCommands.class, (p,m,a) -> {
            if (m.getName().equals("set")) { sets.incrementAndGet(); return "OK"; }
            if (m.getName().equals("eval")) return 1L;
            return null;
        });
        final StatefulRedisConnection<String,String> connection = proxy(StatefulRedisConnection.class, (p,m,a) -> {
            if (m.getName().equals("sync")) { resolving.run(); return commands; }
            if (m.getName().equals("close")) closes.incrementAndGet();
            return null;
        });
        @Override public StatefulRedisConnection<String,String> connect() {
            connects.incrementAndGet(); connecting.run(); return connection;
        }
    }
    static void await(CountDownLatch latch) {
        try { assertTrue(latch.await(5, TimeUnit.SECONDS)); }
        catch (InterruptedException e) { throw new AssertionError(e); }
    }
    @Test void ownsConnectionButNotClient() {
        var c = new Client();
        try {
            var p = new LettuceLockProvider(c);
            p.tryLock("x", LEASE).release(); p.close(); p.close();
            assertEquals(1,c.closes.get());
            assertFalse(c.getResources().eventExecutorGroup().isShuttingDown());
        } finally { c.shutdown(); }
    }
    @Test void borrowedConnectionSurvivesAndOldHandleDoesNotRenew() {
        var c = new Client();
        try {
            var p = new LettuceLockProvider(c.connection);
            var lock = p.tryLock("x",LEASE); p.close();
            assertFalse(lock.extend(LEASE)); lock.release();
            assertEquals(0,c.closes.get()); assertEquals(0,c.connects.get());
        } finally { c.shutdown(); }
    }
    @Test void closeBeforeFirstUseNeverConnects() {
        var c = new Client();
        try {
            var p = new LettuceLockProvider(c); p.close();
            assertThrows(IllegalStateException.class, () -> p.tryLock("x",LEASE));
            assertEquals(0,c.connects.get()); assertEquals(0,p.pendingCompensations());
        } finally { c.shutdown(); }
    }
    @Test void lateConnectCannotPublish() throws Exception {
        var c = new Client(); var pool = Executors.newFixedThreadPool(2);
        var entered = new CountDownLatch(1); var resume = new CountDownLatch(1);
        c.connecting = () -> { entered.countDown(); await(resume); };
        var p = new LettuceLockProvider(c);
        try {
            var call = pool.submit(() -> p.tryLock("x",LEASE)); await(entered);
            p.close(); resume.countDown();
            assertThrows(ExecutionException.class, () -> call.get(5,TimeUnit.SECONDS));
            assertEquals(1,c.closes.get()); assertEquals(0,c.sets.get());
        } finally { resume.countDown(); p.close(); pool.shutdownNow(); c.shutdown(); }
    }
    @Test void proxyFailureDisposesOwnedConnectionAndAllowsRetry() {
        var c = new Client(); var p = new LettuceLockProvider(c);
        try {
            c.resolving = () -> { throw new IllegalStateException("proxy"); };
            assertThrows(IllegalStateException.class, () -> p.tryLock("x",LEASE));
            assertEquals(1,c.closes.get()); assertEquals(0,p.pendingCompensations());
            c.resolving = () -> {}; assertNotNull(p.tryLock("x",LEASE));
            p.close(); assertEquals(2,c.closes.get());
        } finally { p.close(); c.shutdown(); }
    }
    @Test void concurrentFirstUseSharesOneInitialization() throws Exception {
        var c=new Client(); var p=new LettuceLockProvider(c); var pool=Executors.newFixedThreadPool(8);
        var entered=new CountDownLatch(1); var resume=new CountDownLatch(1);
        c.connecting=()->{entered.countDown();await(resume);};
        try {
            var futures=new java.util.ArrayList<Future<?>>();
            for(int i=0;i<8;i++) futures.add(pool.submit(()->p.tryLock("x",LEASE)));
            await(entered); resume.countDown();
            for(var f:futures) assertNotNull(f.get(5,TimeUnit.SECONDS));
            assertEquals(1,c.connects.get());
            p.close(); assertEquals(1,c.closes.get());
        } finally {resume.countDown();p.close();pool.shutdownNow();c.shutdown();}
    }
    @Test void closeWhileResolvingProxyClosesLateOwnedConnection() throws Exception {
        var c=new Client(); var p=new LettuceLockProvider(c); var pool=Executors.newFixedThreadPool(2);
        var entered=new CountDownLatch(1); var resume=new CountDownLatch(1);
        c.resolving=()->{entered.countDown();await(resume);};
        try {
            var creator=pool.submit(()->p.tryLock("x",LEASE)); await(entered);
            var waiter=pool.submit(()->p.tryLock("y",LEASE));
            p.close();
            assertThrows(ExecutionException.class,()->waiter.get(1,TimeUnit.SECONDS));
            resume.countDown(); assertThrows(ExecutionException.class,()->creator.get(5,TimeUnit.SECONDS));
            assertEquals(1,c.closes.get()); assertEquals(0,c.sets.get());
        } finally {resume.countDown();p.close();pool.shutdownNow();c.shutdown();}
    }
    @Test void acquireCompletingAfterCloseCleansCapturedToken() throws Exception {
        var entered=new CountDownLatch(1); var resume=new CountDownLatch(1); var deletes=new AtomicInteger();
        RedisCommands<String,String> commands=proxy(RedisCommands.class,(o,m,a)->{
            if(m.getName().equals("set")){entered.countDown();await(resume);return "OK";}
            if(m.getName().equals("eval")){deletes.incrementAndGet();return 1L;}
            return null;
        });
        StatefulRedisConnection<String,String> connection=proxy(StatefulRedisConnection.class,(o,m,a)->m.getName().equals("sync")?commands:null);
        var p=new LettuceLockProvider(connection);var pool=Executors.newSingleThreadExecutor();
        try {
            var call=pool.submit(()->p.tryLock("x",LEASE));await(entered);p.close();resume.countDown();
            var failure=assertThrows(ExecutionException.class,()->call.get(5,TimeUnit.SECONDS));
            assertInstanceOf(io.tiercache.internal.LockProviderClosedException.class,failure.getCause());
            assertEquals(1,deletes.get()); assertEquals(0,p.pendingCompensations());
        } finally {resume.countDown();p.close();pool.shutdownNow();}
    }
    @Test void closeDuringCompensationRetiresBookkeepingOnce() throws Exception {
        var entered=new CountDownLatch(1); var resume=new CountDownLatch(1);
        var original=new IllegalArgumentException("acquire");
        RedisCommands<String,String> commands=proxy(RedisCommands.class,(o,m,a)->{
            if(m.getName().equals("set")) throw original;
            if(m.getName().equals("eval")){entered.countDown();try{resume.await(5,TimeUnit.SECONDS);}catch(InterruptedException ignored){}return 0L;}
            return null;
        });
        StatefulRedisConnection<String,String> connection=proxy(StatefulRedisConnection.class,(o,m,a)->m.getName().equals("sync")?commands:null);
        var p=new LettuceLockProvider(connection);
        try {
            assertSame(original,assertThrows(IllegalArgumentException.class,()->p.tryLock("x",LEASE)));
            await(entered);p.close();resume.countDown();p.close();assertEquals(0,p.pendingCompensations());
        } finally {resume.countDown();p.close();}
    }

}
