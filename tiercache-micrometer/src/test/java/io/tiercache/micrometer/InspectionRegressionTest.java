package io.tiercache.micrometer;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.tiercache.*;
import io.tiercache.spi.CacheMetricsListener;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

@ResourceLock("tiercache-platform-inspection")
@ResourceLock("java.util.Locale.default")
class InspectionRegressionTest {
    static final ObjectName NAME;
    static {try{NAME=new ObjectName("io.tiercache:type=Inspection");}catch(Exception e){throw new ExceptionInInitializerError(e);}}
    static final MBeanServer SERVER=ManagementFactory.getPlatformMBeanServer();
    static class Rig implements AutoCloseable {
        final SimpleMeterRegistry registry=new SimpleMeterRegistry();
        final TierCacheFactory factory=TierCacheFactory.builder().remoteCache(new InMemoryRemoteCache<>()).build();
        final TiercacheInspection inspection;
        Rig(String name){inspection=new TiercacheInspection(registry,factory,null,List.of(name));}
        public void close(){inspection.close();factory.close();registry.close();}
    }
    static void clean() throws Exception {if(SERVER.isRegistered(NAME))SERVER.unregisterMBean(NAME);}
    static void owner(String name) throws Exception {assertArrayEquals(new String[]{name},(String[])SERVER.getAttribute(NAME,"CacheNames"));}
    @Test void closingSkippedRegistrantPreservesTheOwner() throws Exception {
        assertFalse(SERVER.isRegistered(NAME));
        try(var a=new Rig("a");var b=new Rig("b")) {
            a.inspection.register();b.inspection.register();b.inspection.close();owner("a");
        } finally {clean();}
    }
    @Test void repeatedCloseCannotRemoveReplacement() throws Exception {
        assertFalse(SERVER.isRegistered(NAME));
        try(var a=new Rig("a");var b=new Rig("b")) {
            a.inspection.register();a.inspection.close();b.inspection.register();a.inspection.close();owner("b");
        } finally {clean();}
    }
    public interface ForeignMBean {String getMarker();}
    public static class Foreign implements ForeignMBean {public String getMarker(){return "foreign";}}
    @Test void skippedInspectionCannotRemoveForeignRegistration() throws Exception {
        assertFalse(SERVER.isRegistered(NAME));
        try(var rig=new Rig("a")) {
            SERVER.registerMBean(new Foreign(),NAME);rig.inspection.register();rig.inspection.close();
            assertEquals("foreign",SERVER.getAttribute(NAME,"Marker"));
        } finally {clean();}
    }
    @Test void repeatedRegisterPreservesOwnershipAndExplicitReregisterWorks() throws Exception {
        assertFalse(SERVER.isRegistered(NAME));
        try(var rig=new Rig("a")) {
            rig.inspection.register();rig.inspection.register();owner("a");rig.inspection.close();assertFalse(SERVER.isRegistered(NAME));
            rig.inspection.close();rig.inspection.register();owner("a");rig.inspection.close();assertFalse(SERVER.isRegistered(NAME));
        } finally {clean();}
    }
    @Test void turkishLocaleKeepsAsciiTagsAndQueryableJmxRatiosAcrossLocaleChanges() throws Exception {
        assertFalse(SERVER.isRegistered(NAME));Locale previous=Locale.getDefault();
        try(var rig=new Rig("IstanbulCache")) {
            var metrics=new MicrometerCacheMetrics(rig.registry);Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            metrics.onRequest("IstanbulCache",CacheMetricsListener.Outcome.L1_HIT);
            metrics.onRequest("IstanbulCache",CacheMetricsListener.Outcome.L2_HIT);
            metrics.onRequest("IstanbulCache",CacheMetricsListener.Outcome.MISS);
            metrics.onInvalidation("IstanbulCache",CacheMetricsListener.Direction.RECEIVED);
            rig.inspection.register();
            for(String tag:new String[]{"l1_hit","l2_hit","miss"})assertNotNull(rig.registry.find("tiercache.requests").tags("cache","IstanbulCache","result",tag).counter());
            assertNotNull(rig.registry.find("tiercache.invalidation").tags("cache","IstanbulCache","direction","received").counter());
            assertEquals(1.0/3,(double)SERVER.invoke(NAME,"getL1HitRatio",new Object[]{"IstanbulCache"},new String[]{String.class.getName()}),0.00001);
            assertEquals(1.0/3,(double)SERVER.invoke(NAME,"getL2HitRatio",new Object[]{"IstanbulCache"},new String[]{String.class.getName()}),0.00001);
            int meters=rig.registry.getMeters().size();Locale.setDefault(Locale.ROOT);
            metrics.onRequest("IstanbulCache",CacheMetricsListener.Outcome.L1_HIT);
            metrics.onRequest("IstanbulCache",CacheMetricsListener.Outcome.L2_HIT);
            metrics.onRequest("IstanbulCache",CacheMetricsListener.Outcome.MISS);
            metrics.onInvalidation("IstanbulCache",CacheMetricsListener.Direction.RECEIVED);
            assertEquals(meters,rig.registry.getMeters().size());
            assertEquals(2,rig.registry.get("tiercache.requests").tags("cache","IstanbulCache","result","l1_hit").counter().count());
            assertEquals(1.0/3,(double)SERVER.invoke(NAME,"getL1HitRatio",new Object[]{"IstanbulCache"},new String[]{String.class.getName()}),0.00001);
            owner("IstanbulCache");
        } finally {Locale.setDefault(previous);clean();}
    }
    @Test void allEnumLabelsStayStableAcrossDefaultLocaleChanges() {
        Locale previous=Locale.getDefault();var registry=new SimpleMeterRegistry();
        try {
            var metrics=new MicrometerCacheMetrics(registry);String cache="IstanbulCache";
            for(var locale:List.of(Locale.forLanguageTag("tr-TR"),Locale.ROOT)) {
                Locale.setDefault(locale);
                for(var outcome:CacheMetricsListener.Outcome.values())metrics.onRequest(cache,outcome);
                for(var direction:CacheMetricsListener.Direction.values())metrics.onInvalidation(cache,direction);
                for(var level:CacheMetricsListener.Level.values())metrics.onLatency(cache,level,1000);
            }
            for(var outcome:CacheMetricsListener.Outcome.values())assertEquals(2,registry.get("tiercache.requests")
                    .tags("cache",cache,"result",outcome.name().toLowerCase(Locale.ROOT)).counter().count());
            for(var direction:CacheMetricsListener.Direction.values())assertEquals(2,registry.get("tiercache.invalidation")
                    .tags("cache",cache,"direction",direction.name().toLowerCase(Locale.ROOT)).counter().count());
            for(var level:CacheMetricsListener.Level.values())assertEquals(2,registry.get("tiercache.latency")
                    .tags("cache",cache,"level",level.name().toLowerCase(Locale.ROOT)).timer().count());
            assertEquals(CacheMetricsListener.Outcome.values().length+CacheMetricsListener.Direction.values().length
                    +CacheMetricsListener.Level.values().length,registry.getMeters().size());
        } finally {Locale.setDefault(previous);registry.close();}
    }

    @Test void publicJmxNameAttributesAndOperationsStayUnchanged() throws Exception {
        assertFalse(SERVER.isRegistered(NAME));
        try(var rig=new Rig("a")) {
            rig.inspection.register();var info=SERVER.getMBeanInfo(NAME);
            assertEquals(Set.of("CacheNames","BreakerState"),Arrays.stream(info.getAttributes()).map(MBeanAttributeInfo::getName).collect(java.util.stream.Collectors.toSet()));
            assertEquals(Set.of("getL1HitRatio","getL2HitRatio","getJournalSize"),Arrays.stream(info.getOperations()).map(MBeanOperationInfo::getName).collect(java.util.stream.Collectors.toSet()));
        } finally {clean();}
    }

}
