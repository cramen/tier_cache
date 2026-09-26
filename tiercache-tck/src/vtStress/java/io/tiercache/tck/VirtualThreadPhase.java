package io.tiercache.tck;

import io.tiercache.*;
import io.tiercache.testkit.InMemoryRemoteCache;
import io.tiercache.tck.vt.*;
import jdk.jfr.*;
import java.lang.management.ManagementFactory;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAdder;

/** Shared workload, never shared JVM/state between the two phase tasks. */
final class VirtualThreadPhase {
    static final int WORKERS=100_000, OPS=20, KEYS=1000, WARMUP_WORKERS=1000;
    static Duration limit(String name,long fallback) {
        long seconds=Long.parseLong(System.getProperty("tiercache.vt."+name,Long.toString(fallback)));
        if(seconds<=0 || seconds>3600) throw new IllegalArgumentException("Invalid VT deadline: "+name);
        return Duration.ofSeconds(seconds);
    }
    @FunctionalInterface interface CheckedAction { void run() throws Throwable; }
    static void run(boolean cold) throws Throwable { run(cold, () -> { }); }
    static void run(boolean cold, CheckedAction beforeWorkers) throws Throwable {
        String phase=cold?"cold":"steady", runId=UUID.randomUUID().toString();
        int actual=Runtime.version().feature();
        Path dir=Path.of(System.getProperty("tiercache.vt.output","build/reports/vt"))
                .resolve("jdk-"+actual).resolve(phase).resolve(runId);
        Files.createDirectories(dir);
        var report=new LinkedHashMap<String,Object>();
        report.put("runId",runId); report.put("phase",phase); report.put("status","INCOMPLETE");
        report.put("pid",ProcessHandle.current().pid()); report.put("startedAt",Instant.now().toString());
        report.put("javaVersion",System.getProperty("java.runtime.version")); report.put("javaVendor",System.getProperty("java.vendor"));
        report.put("requestedJdk",System.getProperty("tiercache.vt.requestedJdk","missing"));
        report.put("os",System.getProperty("os.name")); report.put("architecture",System.getProperty("os.arch"));
        report.put("jvmArguments",ManagementFactory.getRuntimeMXBean().getInputArguments());
        report.put("revision",System.getProperty("tiercache.vt.revision","unknown"));
        report.put("dirty",System.getProperty("tiercache.vt.dirty","unknown"));
        report.put("warmupWorkers",cold?0:WARMUP_WORKERS); report.put("warmupOperationsPerWorker",OPS);
        report.put("workers",WORKERS); report.put("operationsPerWorker",OPS); report.put("hotKeys",KEYS); report.put("loaderKeys",KEYS);
        report.put("jfr","recording.jfr");
        VtEvidence.write(dir.resolve("summary.json"),report);
        Throwable failure=null; Recording recording=null;
        try {
            if(actual<21 || actual!=Integer.parseInt(System.getProperty("tiercache.vt.requestedJdk","0")))
                throw new IllegalStateException("Actual JVM differs from required VT runtime");
            Duration warm=limit("warmupSeconds",30), work=limit("workSeconds",120), cleanup=limit("cleanupSeconds",10);
            report.put("deadlinesSeconds",Map.of("warmup",warm.toSeconds(),"work",work.toSeconds(),"cleanup",cleanup.toSeconds()));
            if(!cold) {
                var warmReport=new LinkedHashMap<String,Object>(); report.put("warmup",warmReport);
                workload(WARMUP_WORKERS,warm,cleanup,warmReport);
            }
            if(FlightRecorder.getFlightRecorder().getEventTypes().stream().noneMatch(t->t.getName().equals(VtEvidence.PIN)))
                throw new IllegalStateException("VirtualThreadPinned event unavailable");
            recording=new Recording(); recording.enable(VtEvidence.Proof.class);
            recording.enable(VtEvidence.PIN).withThreshold(Duration.ZERO).withStackTrace();
            VtEvidence.settings(recording.getSettings(),VtEvidence.PIN);
            report.put("recordingSettings",recording.getSettings());
            recording.setDestination(dir.resolve("recording.jfr")); recording.start();
            var measured=new LinkedHashMap<String,Object>(); report.put("measured",measured);
            beforeWorkers.run();
            workload(WORKERS,work,cleanup,measured);
        } catch(Throwable error) { failure=error; }
        finally {
            if(recording!=null) {
                try {
                    VtEvidence.settings(recording.getSettings(),VtEvidence.PIN);
                    if(recording.getState()==RecordingState.RUNNING) {
                        var proof=new VtEvidence.Proof(); proof.runId=runId; proof.commit(); recording.stop();
                    }
                } catch(Throwable error) { if(failure==null) failure=error; else failure.addSuppressed(error); }
                finally {
                    try { recording.close(); }
                    catch(Throwable error) { if(failure==null) failure=error; else failure.addSuppressed(error); }
                }
            }
            try {
                var events=VtEvidence.read(dir.resolve("recording.jfr"),runId,VtEvidence.PIN); report.put("events",events);
                if(failure==null) report.put("status",VtEvidence.outcome(cold,events));
            } catch(Throwable error) { if(failure==null) failure=error; else failure.addSuppressed(error); }
            if(failure!=null) { report.put("status","FAILED"); report.put("failure",failure.toString());
                report.put("suppressed",Arrays.stream(failure.getSuppressed()).map(Throwable::toString).toList()); }
            report.put("finishedAt",Instant.now().toString());
            try { VtEvidence.write(dir.resolve("summary.json"),report); }
            catch(Throwable error) { if(failure==null) failure=error; else failure.addSuppressed(error); }
            System.out.println("VT evidence: "+dir+" status="+report.get("status"));
        }
        if(failure!=null) throw failure;
    }
    static void workload(int workers,Duration timeout,Duration cleanup,Map<String,Object> report) throws Throwable {
        var settings=new CacheSettings(KEYS*2,Duration.ofMinutes(10),null,Duration.ofHours(1),0,
                NullPolicy.deny(),InvalidationMode.INVALIDATE,65536);
        var remote=new InMemoryRemoteCache<String,String>();
        var local=new io.tiercache.internal.CaffeineLocalCache<String,String>(settings);
        try(var factory=TierCacheFactory.builder().defaults(settings).remoteCache(remote)
                .localCacheFactory((name,configured)->local).build()) {
            TierCache<String,String> cache=factory.getCache("vt-workload");
            for(int i=0;i<KEYS;i++) cache.put("hot-"+i,"v"+i);
            // Check absence without invoking cache read paths before the measured workers.
            for(int i=0;i<KEYS;i++) {
                if(local.get("loader-"+i)!=null || remote.get("loader-"+i)!=null)
                    throw new AssertionError("Measured loader key already populated");
            }
            var operations=new LongAdder(); var loads=new LongAdder();
            report.put("setupHotPuts",KEYS); report.put("initialLoaderKeys",0); report.put("verifiedAbsentLoaderKeys",KEYS);
            try {
                ObservedWorkers.run(Executors.newVirtualThreadPerTaskExecutor(),workers,id -> {
                    for(int op=0;op<OPS;op++) {
                        int slot=(id+op)%KEYS; String expected,actual;
                        if((op&1)==0) { expected="v"+slot; actual=cache.get("hot-"+slot); }
                        else { String key="loader-"+slot; expected="computed-"+key;
                            actual=cache.getOrCompute(key,k->{loads.increment();return "computed-"+k;}); }
                        if(!expected.equals(actual)) throw new AssertionError("Wrong value for worker "+id+" op "+op);
                        operations.increment();
                    }
                },timeout,cleanup,report);
                if(operations.sum()!=(long)workers*OPS || loads.sum()==0) throw new AssertionError("Missing measured work");
            } finally { report.put("operations",operations.sum()); report.put("loaderExecutions",loads.sum()); }
        }
    }
}
