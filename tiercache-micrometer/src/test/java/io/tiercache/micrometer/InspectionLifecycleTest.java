package io.tiercache.micrometer;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.tiercache.TierCacheFactory;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;
import javax.management.*;
import java.lang.reflect.*;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class InspectionLifecycleTest {
    static final ObjectName NAME=InspectionRegressionTest.NAME;
    static class Rig implements AutoCloseable {
        final SimpleMeterRegistry registry=new SimpleMeterRegistry();
        final TierCacheFactory factory=TierCacheFactory.builder().remoteCache(new InMemoryRemoteCache<>()).build();
        final TiercacheInspection inspection;
        Rig(String name,MBeanServer server){inspection=new TiercacheInspection(registry,factory,null,List.of(name),()->server);}
        public void close(){inspection.close();factory.close();registry.close();}
    }
    static MBeanServer proxy(InvocationHandler handler) {
        return (MBeanServer)Proxy.newProxyInstance(InspectionLifecycleTest.class.getClassLoader(),new Class<?>[]{MBeanServer.class},handler);
    }
    static Object invoke(MBeanServer server,Method method,Object[] args)throws Throwable {
        try{return method.invoke(server,args);}catch(InvocationTargetException e){throw e.getCause();}
    }
    static void await(CountDownLatch latch) {
        try{assertTrue(latch.await(5,TimeUnit.SECONDS));}catch(InterruptedException e){throw new AssertionError(e);}
    }
    static String owner(MBeanServer server)throws Exception{return ((String[])server.getAttribute(NAME,"CacheNames"))[0];}

    @Test void failedRegistrationNeverGrantsCleanupAuthorityAndCanBeRetried() throws Exception {
        var server=MBeanServerFactory.newMBeanServer();var fail=new AtomicBoolean(true);var unregisters=new AtomicInteger();
        var failure=new MBeanRegistrationException(new Exception("registration failed"));
        var instrumented=proxy((p,m,a)->{
            if(m.getName().equals("registerMBean")&&fail.getAndSet(false))throw failure;
            if(m.getName().equals("unregisterMBean"))unregisters.incrementAndGet();
            return invoke(server,m,a);
        });
        try(var a=new Rig("a",instrumented);var b=new Rig("b",server)) {
            assertSame(failure,assertThrows(IllegalStateException.class,a.inspection::register).getCause());
            b.inspection.register();a.inspection.close();assertEquals(0,unregisters.get());assertEquals("b",owner(server));
            b.inspection.close();a.inspection.register();assertEquals("a",owner(server));a.inspection.close();assertEquals(1,unregisters.get());
        }
    }
    @Test void unregisterFailureConsumesOwnershipBeforeAReplacementAppears() throws Exception {
        var server=MBeanServerFactory.newMBeanServer();var unregisters=new AtomicInteger();
        var instrumented=proxy((p,m,a)->{
            if(m.getName().equals("unregisterMBean")){unregisters.incrementAndGet();throw new SecurityException("denied");}
            return invoke(server,m,a);
        });
        try(var a=new Rig("a",instrumented);var b=new Rig("b",server)) {
            a.inspection.register();assertDoesNotThrow(a.inspection::close);assertEquals(1,unregisters.get());
            server.unregisterMBean(NAME);b.inspection.register();a.inspection.close();assertEquals(1,unregisters.get());assertEquals("b",owner(server));
        }
    }
    @Test void externallyAbsentOwnedNameIsBenignAndAllowsNewOwnershipPeriod() throws Exception {
        var server=MBeanServerFactory.newMBeanServer();
        try(var rig=new Rig("a",server)) {
            rig.inspection.register();server.unregisterMBean(NAME);assertDoesNotThrow(rig.inspection::close);
            rig.inspection.register();assertEquals("a",owner(server));rig.inspection.close();assertFalse(server.isRegistered(NAME));
        }
    }
    @Test void concurrentRegistrantsHaveOneOwnerAndLoserCannotRemoveWinner() throws Exception {
        var server=MBeanServerFactory.newMBeanServer();var meet=new CyclicBarrier(2);var occupied=new AtomicInteger();
        var instrumented=proxy((p,m,a)->{
            if(m.getName().equals("registerMBean"))meet.await(5,TimeUnit.SECONDS);
            try{return invoke(server,m,a);}catch(InstanceAlreadyExistsException e){occupied.incrementAndGet();throw e;}
        });
        var pool=Executors.newFixedThreadPool(2);
        try(var a=new Rig("a",instrumented);var b=new Rig("b",instrumented)) {
            var first=pool.submit(a.inspection::register);var second=pool.submit(b.inspection::register);
            first.get(5,TimeUnit.SECONDS);second.get(5,TimeUnit.SECONDS);assertEquals(1,occupied.get());
            String name=owner(server);var winner=name.equals("a")?a:b;var loser=name.equals("a")?b:a;
            loser.inspection.close();assertEquals(name,owner(server));winner.inspection.register();assertEquals(name,owner(server));
            winner.inspection.close();assertFalse(server.isRegistered(NAME));
        } finally {pool.shutdownNow();}
    }
    @Test void closeWaitsForRegistrationPublicationAndSkippedCompetitorRemainsUnowned() throws Exception {
        var server=MBeanServerFactory.newMBeanServer();var registered=new CountDownLatch(1);var release=new CountDownLatch(1);var closing=new CountDownLatch(1);
        var instrumented=proxy((p,m,a)->{
            Object result=invoke(server,m,a);
            if(m.getName().equals("registerMBean")){registered.countDown();await(release);}
            return result;
        });
        var pool=Executors.newFixedThreadPool(2);
        try(var a=new Rig("a",instrumented);var b=new Rig("b",server)) {
            var registration=pool.submit(a.inspection::register);await(registered);
            var close=pool.submit(()->{closing.countDown();a.inspection.close();});await(closing);
            b.inspection.register();b.inspection.close();assertEquals("a",owner(server));
            release.countDown();registration.get(5,TimeUnit.SECONDS);close.get(5,TimeUnit.SECONDS);assertFalse(server.isRegistered(NAME));
            b.inspection.register();a.inspection.close();assertEquals("b",owner(server));
        } finally {release.countDown();pool.shutdownNow();}
    }
    @Test void registrationFollowingAnInFlightCloseStartsANewOwnershipPeriod() throws Exception {
        var server=MBeanServerFactory.newMBeanServer();var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var gate=new AtomicBoolean(true);
        var instrumented=proxy((p,m,a)->{
            if(m.getName().equals("unregisterMBean")&&gate.getAndSet(false)){entered.countDown();await(release);}
            return invoke(server,m,a);
        });
        var pool=Executors.newFixedThreadPool(2);
        try(var rig=new Rig("a",instrumented)) {
            rig.inspection.register();var close=pool.submit(rig.inspection::close);await(entered);
            var register=pool.submit(rig.inspection::register);release.countDown();close.get(5,TimeUnit.SECONDS);register.get(5,TimeUnit.SECONDS);
            assertEquals("a",owner(server));rig.inspection.close();assertFalse(server.isRegistered(NAME));
        } finally {release.countDown();pool.shutdownNow();}
    }
}
