package io.tiercache.redis;

import io.lettuce.core.RedisClient;
import io.tiercache.*;
import io.tiercache.spi.*;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Full factory -> stock cache -> guarded Redis path; timed runs never simulate Redis latency. */
public final class ClosedBreakerRedisBenchmark {
    static final int KEYS=512, HOT=64, SAMPLES=131072;
    static final int WARMUP_SECONDS=Integer.getInteger("breaker.warmupSeconds",2);
    static final int MEASURE_SECONDS=Integer.getInteger("breaker.measureSeconds",3);
    static final int SAMPLE_INTERVAL=Integer.getInteger("breaker.sampleInterval",31);
    static final class ObservedRemote implements RemoteCache<Object,Object> {
        final LettuceRemoteCache<Object,Object> delegate;
        final LongAdder gets=new LongAdder();
        ObservedRemote(LettuceRemoteCache<Object,Object> delegate){this.delegate=delegate;}
        public StoredEntry<Object> get(Object key){gets.increment();return delegate.get(key);}
        public void put(Object k,StoredEntry<Object> v,Duration ttl){delegate.put(k,v,ttl);}
        public boolean setIfAbsent(Object k,StoredEntry<Object> v,Duration ttl){return delegate.setIfAbsent(k,v,ttl);}
        public void evict(Object k){delegate.evict(k);}
        public void clear(){delegate.clear();}
    }
    static final class Result {
        long operations,workerBytes; final long[] samples=new long[SAMPLES]; int count;
        void sample(long nanos){samples[count++%SAMPLES]=nanos;}
    }
    public static void main(String[] args) throws Exception {
        String uri=args[0], profile=args[1], model=args[3]; int threads=Integer.parseInt(args[2]);
        if(WARMUP_SECONDS<=0 || MEASURE_SECONDS<=0 || SAMPLE_INTERVAL<=0) throw new IllegalArgumentException("Positive benchmark settings required");
        if(!Set.of("l2","multi","mixed","hot").contains(profile))throw new IllegalArgumentException(profile);
        if(!Set.of("platform","virtual").contains(model))throw new IllegalArgumentException(model);
        ExecutorService pool=model.equals("virtual")
                ? (ExecutorService)Executors.class.getMethod("newVirtualThreadPerTaskExecutor").invoke(null)
                : Executors.newFixedThreadPool(threads);
        var os=(com.sun.management.OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean();
        var allocation=(com.sun.management.ThreadMXBean)ManagementFactory.getThreadMXBean();
        if(!allocation.isThreadAllocatedMemorySupported())throw new IllegalStateException("Allocation counter unavailable");
        allocation.setThreadAllocatedMemoryEnabled(true);
        java.lang.reflect.Method totalAllocated;
        try { totalAllocated=com.sun.management.ThreadMXBean.class.getMethod("getTotalThreadAllocatedBytes"); }
        catch(NoSuchMethodException missing){totalAllocated=null;}
        var remotes=new ConcurrentHashMap<String,ObservedRemote>();
        var client=RedisClient.create(uri); String prefix="breaker-bench-"+UUID.randomUUID()+"-";
        String[] keys=new String[KEYS],values=new String[KEYS];for(int i=0;i<KEYS;i++){keys[i]="k"+i;values[i]="v"+i;}
        var settings=new CacheSettings(1,Duration.ofMinutes(10),null,Duration.ofHours(1),0,NullPolicy.deny(),InvalidationMode.INVALIDATE,65536);
        try(var factory=TierCacheFactory.builder().defaults(settings)
                .cache("hot",new CacheOverride().l1MaxSize(10000))
                .remoteCacheFactory(name->{var remote=new ObservedRemote(LettuceRemoteCache.builder(uri).client(client).cacheName(prefix+name).build());remotes.put(name,remote);return remote;}).build()) {
            int caches=profile.equals("multi")?6:1;
            var cold=new ArrayList<TierCache<String,String>>();
            for(int i=0;i<caches;i++) {
                String name="cold"+i; cold.add(factory.getCache(name));
                for(int k=0;k<KEYS;k++)remotes.get(name).put(keys[k],StoredEntry.ofValue(values[k]),Duration.ofHours(1));
            }
            TierCache<String,String> hot=factory.getCache("hot");
            for(int k=0;k<HOT;k++)hot.put(keys[k],values[k]);
            var rows=new ArrayList<String>();
            for(int round=-2;round<3;round++) {
                Duration duration=Duration.ofSeconds(round<0?WARMUP_SECONDS:MEASURE_SECONDS);
                var ready=new CountDownLatch(threads);var start=new CountDownLatch(1);var deadline=new AtomicLong();
                var results=new ArrayList<Future<Result>>();
                for(int thread=0;thread<threads;thread++) {
                    int worker=thread;
                    results.add(pool.submit(()->{
                        var result=new Result();ready.countDown();if(!start.await(10,TimeUnit.SECONDS))throw new AssertionError("start");
                        long before=allocation.getThreadAllocatedBytes(Thread.currentThread().getId());
                        for(long i=0;System.nanoTime()<deadline.get();i++) {
                            boolean useHot=profile.equals("hot") || profile.equals("mixed") && i%20!=0;
                            int key=useHot?(int)((i+worker)%HOT):(int)((i*17+worker*31)&(KEYS-1));
                            // A coprime interval avoids over-sampling the one-in-20 cold requests.
                            boolean sample=i%SAMPLE_INTERVAL==0;long tick=sample?System.nanoTime():0;
                            String actual=useHot?hot.get(keys[key]):cold.get((int)((i+worker)%caches)).get(keys[key]);
                            if(sample)result.sample(System.nanoTime()-tick);
                            if(!values[key].equals(actual))throw new AssertionError("incorrect value "+actual);
                            result.operations++;
                        }
                        long after=allocation.getThreadAllocatedBytes(Thread.currentThread().getId());
                        result.workerBytes=before<0||after<0?-1:after-before;return result;
                    }));
                }
                if(!ready.await(10,TimeUnit.SECONDS))throw new AssertionError("workers never ready");
                long readsBefore=remotes.values().stream().mapToLong(r->r.gets.sum()).sum();
                long allocatedBefore=totalAllocated==null?-1:((Number)totalAllocated.invoke(allocation)).longValue();
                long cpu=os.getProcessCpuTime(), began=System.nanoTime();deadline.set(began+duration.toNanos());start.countDown();
                long operations=0,bytes=0;boolean workerAllocation=true;var samples=new ArrayList<Long>();
                for(var future:results) {
                    Result r=future.get(15,TimeUnit.SECONDS);operations+=r.operations;
                    if(r.workerBytes<0)workerAllocation=false;else bytes+=r.workerBytes;
                    for(int i=0;i<Math.min(r.count,SAMPLES);i++)samples.add(r.samples[i]);
                }
                long elapsed=System.nanoTime()-began,cpuElapsed=os.getProcessCpuTime()-cpu;
                long allocatedAfter=totalAllocated==null?-1:((Number)totalAllocated.invoke(allocation)).longValue();
                long reads=remotes.values().stream().mapToLong(r->r.gets.sum()).sum()-readsBefore;
                if(operations==0 || cpu<0 || cpuElapsed<0 || samples.isEmpty())throw new AssertionError("empty or invalid measurement");
                if(profile.equals("hot") && reads!=0)throw new AssertionError("false hot control: Redis calls "+reads);
                if((profile.equals("l2")||profile.equals("multi")) && reads<operations*.90)throw new AssertionError("not L2-heavy");
                if(factory.breakerState()!=BreakerState.CLOSED)throw new AssertionError("benchmark degraded");
                samples.sort(Long::compare);double p99=samples.get(Math.min(samples.size()-1,(int)(samples.size()*.99)))/1e6;
                if(round>=0)rows.add(String.format(Locale.ROOT,
                        "{\"throughput\":%.3f,\"p99Ms\":%.6f,\"operations\":%d,\"redisCalls\":%d,\"samples\":%d,\"processCpuNsPerOp\":%.3f,\"workerAllocatedBytesPerOp\":%s,\"processAllocatedBytesPerOp\":%s}",
                        operations*1e9/elapsed,p99,operations,reads,samples.size(),(double)cpuElapsed/operations,
                        workerAllocation?Double.toString((double)bytes/operations):"null",
                        allocatedBefore>=0 && allocatedAfter>=allocatedBefore?Double.toString((double)(allocatedAfter-allocatedBefore)/operations):"null"));
            }
            System.out.println("{\"profile\":\""+profile+"\",\"threads\":"+threads+",\"model\":\""+model+"\",\"jdk\":\""+System.getProperty("java.runtime.version")+"\",\"coherent\":true,\"warmupSeconds\":"+WARMUP_SECONDS+",\"measureSeconds\":"+MEASURE_SECONDS+",\"sampleInterval\":"+SAMPLE_INTERVAL+",\"rounds\":["+String.join(",",rows)+"]}");
        } finally {
            pool.shutdownNow();pool.awaitTermination(10,TimeUnit.SECONDS);
            for(var remote:remotes.values()) { remote.delegate.clear();remote.delegate.close(); }
            client.shutdown();
        }
    }
}
