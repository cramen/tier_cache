package io.tiercache.tck;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class SoakHarnessTest {
    @TempDir Path directory;
    static List<SoakGate.Sample> samples(long[] heap, long[] rss, long[] journal) {
        List<SoakGate.Sample> result=new ArrayList<>();
        for(int i=0;i<heap.length;i++)result.add(new SoakGate.Sample(i,heap[i],rss[i],journal[i],i*100,i+1,List.of()));
        return result;
    }
    @Test void independentSeriesRejectHeapOnlyRssOnlyAndCumulativeGrowth() {
        long[] stable={100,100,100,100}, growth={100,100,103,106}, journal={2000,2000,2000,2000};
        assertThrows(AssertionError.class,()->SoakGate.assess(samples(growth,stable,journal),2000).requirePass());
        assertThrows(AssertionError.class,()->SoakGate.assess(samples(stable,growth,journal),2000).requirePass());
        var okay=SoakGate.assess(samples(new long[]{9999,100,103,105},stable,journal),2000);
        assertDoesNotThrow(okay::requirePass);assertEquals(100,okay.heapBaseline());assertEquals(1,okay.discarded());assertEquals(3,okay.steadySamples());
    }
    @Test void insufficientSamplesInvalidBaselinesAndShortDurationsFail() {
        assertThrows(AssertionError.class,()->SoakGate.assess(samples(new long[]{100,100,100},new long[]{100,100,100},new long[]{1,1,1}),2000));
        assertThrows(AssertionError.class,()->SoakGate.assess(samples(new long[]{100,0,100,100},new long[]{100,100,100,100},new long[]{1,1,1,1}),2000));
        assertThrows(AssertionError.class,()->SoakGate.assess(samples(new long[]{100,100,100,100},new long[]{100,100,-1,100},new long[]{1,1,1,1}),2000));
        for(long seconds:new long[]{-1,0,30,60,89})assertThrows(IllegalArgumentException.class,()->SoakGate.validateDuration(Duration.ofSeconds(seconds),Duration.ofSeconds(30)));
        assertDoesNotThrow(()->SoakGate.validateDuration(Duration.ofSeconds(90),Duration.ofSeconds(30)));
    }
    @Test void journalPeakAndHalfMeanChecksRemainSeparateAndUnchanged() {
        long[] memory={100,100,100,100};
        assertThrows(AssertionError.class,()->SoakGate.assess(samples(memory,memory,new long[]{2000,2000,2000,5000}),2000).requirePass());
        var trend=SoakGate.assess(samples(memory,memory,new long[]{2000,2000,2700,2700}),2000);
        assertTrue(trend.journalPeak()<=4000);assertThrows(AssertionError.class,trend::requirePass);
    }
    @Test void rssReadersUseResidentKibibytesAndRejectUnavailableOrMalformedValues() throws Exception {
        assertEquals(12*1024,SoakMemory.linuxRss("VmSize: 999999 kB\nVmRSS:\t12 kB\n"));
        assertEquals(128*1024,SoakMemory.macRss("  128\n"));
        for(String text:new String[]{"", "-1", "0", "1.5", "NaN", "Infinity", "12 13", "9223372036854775807"})assertThrows(IllegalStateException.class,()->SoakMemory.macRss(text));
        for(String text:new String[]{"VmSize: 12 kB", "VmRSS: -1 kB", "VmRSS: 0 kB", "VmRSS: 12 MB", "VmRSS: 12 kB\nVmRSS: 13 kB"})assertThrows(IllegalStateException.class,()->SoakMemory.linuxRss(text));
        assertThrows(IllegalStateException.class,()->SoakMemory.rssReader("Windows",42,(c,t)->"12",()->"unused"));
        var mac=SoakMemory.rssReader("Mac OS X",42,(command,timeout)->{
            assertEquals(List.of("/bin/ps","-o","rss=","-p","42"),command);assertEquals(Duration.ofSeconds(2),timeout);return "32";
        },()->{throw new AssertionError("wrong source");});assertEquals(32768,mac.bytes());
        var timeout=new IllegalStateException("RSS timeout");
        var failed=SoakMemory.rssReader("macOS",42,(c,t)->{throw timeout;},()->"unused");assertSame(timeout,assertThrows(IllegalStateException.class,failed::bytes));
    }
    static boolean supportedHost() {
        String os=System.getProperty("os.name").toLowerCase(Locale.ROOT);
        return os.contains("linux")||os.contains("mac")||os.contains("darwin");
    }
    @Test void commandTimeoutAndExitFailureAreBoundedAndNotZeroRss() {
        if (!supportedHost()) { assertThrows(IllegalStateException.class,SoakMemory::systemRss); return; }
        assertTimeoutPreemptively(Duration.ofSeconds(3),()->assertThrows(IllegalStateException.class,
                ()->SoakMemory.runCommand(List.of("/bin/sleep","5"),Duration.ofMillis(20))));
        assertThrows(IllegalStateException.class,()->SoakMemory.runCommand(List.of("/bin/ps","--not-a-supported-option"),Duration.ofSeconds(1)));
    }
    static class FakeGc implements SoakMemory.GcAccess {
        long completed,heap=123;Runnable request=()->{};
        public long completed(){return completed;}public long heapAfterGc(){return heap;}public void request(){request.run();}
    }
    @Test void gcProgressIsConfirmedAndIgnoredOrSlowExplicitGcFails() throws Exception {
        var clock=new AtomicLong();var gc=new FakeGc();
        var reading=SoakMemory.sample(gc,()->456,clock::get,d->{clock.addAndGet(d.toNanos());gc.completed++;},Duration.ofMillis(20));
        assertEquals(123,reading.heapBytes());assertEquals(456,reading.rssBytes());assertEquals(1,reading.explicitGcCompletions());
        var ignored=new FakeGc();clock.set(0);
        var failure=assertThrows(IllegalStateException.class,()->SoakMemory.sample(ignored,()->456,clock::get,d->clock.addAndGet(d.toNanos()),Duration.ofMillis(20)));
        assertTrue(failure.getMessage().contains("DisableExplicitGC"));
        var slow=new FakeGc();clock.set(0);slow.request=()->{clock.addAndGet(Duration.ofSeconds(1).toNanos());slow.completed++;};
        assertThrows(IllegalStateException.class,()->SoakMemory.sample(slow,()->456,clock::get,d->{},Duration.ofMillis(20)));
        var invalid=new FakeGc();invalid.request=()->invalid.completed++;invalid.heap=0;clock.set(0);
        assertThrows(IllegalStateException.class,()->SoakMemory.sample(invalid,()->456,clock::get,d->{},Duration.ofMillis(20)));
        invalid.heap=123;
        assertThrows(IllegalStateException.class,()->SoakMemory.sample(invalid,()->0,clock::get,d->{},Duration.ofMillis(20)));
    }
    @Test void realHostProvidesRssAndConfirmedPostGcHeap() throws Exception {
        if (!supportedHost()) { assertThrows(IllegalStateException.class,SoakMemory::systemRss); return; }
        var rss=SoakMemory.systemRss();assertTrue(rss.bytes()>0);
        try(var gc=new SoakMemory.ExplicitGc()) {
            var reading=SoakMemory.sample(gc,rss,System::nanoTime,d->TimeUnit.NANOSECONDS.sleep(d.toNanos()),Duration.ofSeconds(5));
            assertTrue(reading.heapBytes()>0);assertTrue(reading.rssBytes()>0);assertTrue(reading.explicitGcCompletions()>0);
        }
    }
    @Test void futureCapturedErrorFailsEvenWhenExceptionCounterRemainsZero() throws Exception {
        var errors=new AtomicInteger();var original=new AssertionError("worker assertion");
        try(var workers=new SoakWorkers(1,System.nanoTime()+TimeUnit.SECONDS.toNanos(10),System::nanoTime,w->{
            try{throw original;}catch(Exception error){errors.incrementAndGet();}
        })) {
            var failure=assertThrows(AssertionError.class,()->workers.awaitHealthy(Duration.ofSeconds(1)));
            assertSame(original,failure.getCause());assertEquals(0,errors.get());assertEquals(true,workers.snapshots().get(0).get("futureObserved"));
        }
    }
    @Test void earlyCompletionAndEmptyWorkerCannotPass() {
        var clock=new AtomicLong(1);
        try(var workers=new SoakWorkers(1,10,clock::get,w->w.succeeded())) {
            assertTrue(assertThrows(AssertionError.class,()->workers.awaitHealthy(Duration.ofSeconds(1))).getCause().getMessage().contains("prematurely"));
        }
        try(var workers=new SoakWorkers(1,10,clock::get,w->clock.set(10))) {
            assertTrue(assertThrows(AssertionError.class,()->workers.awaitHealthy(Duration.ofSeconds(1))).getCause().getMessage().contains("no successful"));
        }
    }
    @Test void cancellationAndHungWorkerCannotPassAndCleanupObservesEveryFuture() throws Exception {
        for(boolean cancelled:new boolean[]{false,true}) {
            var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
            var workers=new SoakWorkers(2,System.nanoTime()+TimeUnit.SECONDS.toNanos(10),System::nanoTime,w->{
                w.succeeded();entered.countDown();while(release.getCount()>0)try{release.await(10,TimeUnit.MILLISECONDS);}catch(InterruptedException ignored){}
            });
            try {
                assertTrue(entered.await(1,TimeUnit.SECONDS));if(cancelled)workers.cancel(0);
                assertTimeoutPreemptively(Duration.ofSeconds(2),()->assertThrows(AssertionError.class,()->workers.awaitHealthy(Duration.ofMillis(30))));
            } finally {release.countDown();workers.close();}
            assertTrue(workers.terminated());assertTrue(workers.snapshots().stream().allMatch(row->Boolean.TRUE.equals(row.get("futureObserved"))));
        }
    }
    @Test void successfulWorkHasObservedFuturesAndOperationCounts() throws Exception {
        var now=new AtomicLong(1);
        try(var workers=new SoakWorkers(1,10,now::get,w->{w.succeeded();now.set(10);})) {
            workers.awaitHealthy(Duration.ofSeconds(1));assertEquals(1,workers.operations());
            assertEquals("COMPLETED",workers.snapshots().get(0).get("state"));assertEquals(true,workers.snapshots().get(0).get("futureObserved"));
        }
    }
    @Test void failedReportRetainsUnitsAndEscapesDiagnostics() throws Exception {
        var report=Map.<String,Object>of("status","FAIL","units",Map.of("memory","bytes"),"failure","quote\" slash\\ newline\n");
        Path path=directory.resolve("report.json");SoakReport.write(path,report);String json=Files.readString(path);
        assertTrue(json.contains("\\\""));assertTrue(json.contains("\\\\"));assertTrue(json.contains("\\u000a"));assertTrue(json.contains("\"status\":\"FAIL\""));
        assertThrows(IllegalArgumentException.class,()->SoakReport.json(Double.NaN));
    }
}
