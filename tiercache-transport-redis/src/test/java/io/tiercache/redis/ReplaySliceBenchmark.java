package io.tiercache.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.codec.ByteArrayCodec;
import io.tiercache.*;
import io.tiercache.invalidation.*;
import io.tiercache.spi.*;
import io.tiercache.testkit.InMemoryRemoteCache;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.LockSupport;

/** Ordered real Pub/Sub alongside real checked replay into the stock Caffeine engine. */
public final class ReplaySliceBenchmark {
    static final JdkCacheSerializer<Object> CODEC = new JdkCacheSerializer<>();
    static final UUID WRITER = UUID.randomUUID();
    static final String PUBLISH = "redis.call('xadd',KEYS[1],'*','t',ARGV[1],'k',ARGV[2],'v',ARGV[3],'p',ARGV[4]); return redis.call('publish',KEYS[2],ARGV[5])";
    static final int BACKLOG = Integer.getInteger("replay.backlog", 32768);
    static final int LIVE = Integer.getInteger("replay.live", 1000);
    static final int ROUNDS = Integer.getInteger("replay.rounds", 4);
    record Sent(long nanos, boolean recovering, boolean sameCache) { }
    static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    static Object field(Object object, String name) throws Exception {
        var f = object.getClass().getDeclaredField(name); f.setAccessible(true); return f.get(object);
    }
    static double percentile(List<Long> values, double fraction) {
        if (values.isEmpty()) return -1;
        var sorted = new ArrayList<>(values); sorted.sort(Long::compare);
        return sorted.get(Math.min(sorted.size()-1, (int)(sorted.size()*fraction))) / 1e6;
    }
    public static void main(String[] args) throws Exception {
        String mode = args[1];
        var same = new CopyOnWriteArrayList<Long>(); var other = new CopyOnWriteArrayList<Long>();
        var all = new CopyOnWriteArrayList<Long>();
        var recoveries = new ArrayList<Double>(); var readsPerRound = new ArrayList<Integer>();
        long measuredNanos = 0, cpuNanos = 0;
        var os = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        var client = RedisClient.create(args[0]);
        try (var connection = client.connect(ByteArrayCodec.INSTANCE)) {
            for (int round = -1; round < ROUNDS; round++) {
                String a = "replay-a-" + UUID.randomUUID(), b = "replay-b-" + UUID.randomUUID();
                var underlying = new RedisStreamJournal(connection, 200000, CODEC);
                var reads = new AtomicInteger();
                InvalidationJournal journal = new InvalidationJournal() {
                    public String append(String c, InvalidationMessage m) { return underlying.append(c,m); }
                    public List<JournalRow> readRange(String c,String cursor) { return underlying.readRange(c,cursor); }
                    public CheckedRange checkedRead(String c,String cursor,int n) { if(c.equals(a)) reads.incrementAndGet(); return underlying.checkedRead(c,cursor,n); }
                    public String endCursor(String c) { return underlying.endCursor(c); }
                    public boolean isTrimmed(String c,String cursor) { return underlying.isTrimmed(c,cursor); }
                };
                var settings = new CacheSettings(100000, Duration.ofMinutes(5), null, Duration.ofHours(1), 0,
                        NullPolicy.deny(), InvalidationMode.UPDATE, 65536);
                try (var factory = TierCacheFactory.builder().defaults(settings).remoteCache(new InMemoryRemoteCache<>()).build();
                     var transport = new LettucePubSubInvalidationTransport(client, CODEC);
                     var service = new InvalidationService(transport, journal, UUID.randomUUID(), InvalidationListener.NOOP)) {
                    TierCache<String,String> ca = factory.getCache(a), cb = factory.getCache(b);
                    service.registerTarget(a,(InvalidationTarget)ca); service.registerTarget(b,(InvalidationTarget)cb);
                    int count = mode.equals("healthy") ? 0 : BACKLOG;
                    List<CompletableFuture<?>> appends = new ArrayList<>();
                    for (int i=1;i<=count;i++) {
                        boolean clear = mode.equals("clear") && i == count/2;
                        Map<byte[],byte[]> values = new LinkedHashMap<>();
                        values.put(bytes("t"),new byte[]{(byte)(clear?InvalidationMessage.Type.EVICT_ALL:InvalidationMessage.Type.UPDATE).ordinal()});
                        values.put(bytes("k"),clear?new byte[0]:CODEC.toBytes("history-"+i));
                        values.put(bytes("v"),bytes(new Version(i,WRITER).toWire()));
                        values.put(bytes("p"),clear?new byte[0]:CODEC.toBytes("history-value"));
                        appends.add(connection.async().xadd(RedisKeyspace.journal(a),values).toCompletableFuture());
                        if(appends.size()==256) { CompletableFuture.allOf(appends.toArray(CompletableFuture[]::new)).join();appends.clear(); }
                    }
                    CompletableFuture.allOf(appends.toArray(CompletableFuture[]::new)).join();
                    var sent = new ConcurrentHashMap<Long,Sent>(); var observed = new AtomicInteger();
                    boolean measure = round>=0;
                    service.setEventListener((cache,message)-> {
                        Sent start=sent.remove(message.version().sequence());
                        if(start==null)return;
                        long elapsed=System.nanoTime()-start.nanos();
                        if(measure) {
                            all.add(elapsed);
                            if(start.recovering() || mode.equals("healthy")) (start.sameCache()?same:other).add(elapsed);
                        }
                        observed.incrementAndGet();
                    });
                    long cpu=os.getProcessCpuTime(), start=System.nanoTime();
                    var recovery=service.recoverAsync(Runnable::run).toCompletableFuture();
                    var finished=new AtomicLong(); recovery.whenComplete((value,error)->finished.set(System.nanoTime()));
                    for(int i=0;i<LIVE;i++) {
                        long delay=start+i*2_000_000L-System.nanoTime();if(delay>0)LockSupport.parkNanos(delay);
                        boolean isA=i%2==0;String cache=isA?a:b;long sequence=1_000_000L+i;
                        var version=new Version(sequence,WRITER);byte[] key=CODEC.toBytes("live"),value=CODEC.toBytes("v"+i);
                        var wire=new InvalidationMessage(cache,"live",version,WRITER,InvalidationMessage.Type.UPDATE,value);
                        byte[] frame=MessageCodec.encode(wire,key);
                        sent.put(sequence,new Sent(System.nanoTime(),!recovery.isDone(),isA));
                        connection.sync().eval(PUBLISH,ScriptOutputType.INTEGER,
                                new byte[][]{RedisKeyspace.journal(cache),RedisKeyspace.channel(cache)},
                                new byte[]{(byte)InvalidationMessage.Type.UPDATE.ordinal()},key,bytes(version.toWire()),value,frame);
                    }
                    if(!recovery.get(30,TimeUnit.SECONDS))throw new AssertionError("recovery failed");
                    // A final checked pass accounts for the fixed live schedule's tail.
                    if(!service.recoverAsync(Runnable::run).toCompletableFuture().get(30,TimeUnit.SECONDS))throw new AssertionError("tail failed");
                    long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
                    while(observed.get()!=LIVE && System.nanoTime()<end)Thread.sleep(1);
                    if(observed.get()!=LIVE)throw new AssertionError("missing live observations: "+observed);
                    if(!("v"+(LIVE-2)).equals(ca.get("live")) || !("v"+(LIVE-1)).equals(cb.get("live")))throw new AssertionError("live versions");
                    if(count>0 && !"history-value".equals(ca.get("history-"+count)))throw new AssertionError("history tail");
                    if(mode.equals("clear") && ca.get("history-1")!=null)throw new AssertionError("clear resurrected prefix");
                    Object state=((Map<?,?>)field(service,"states")).get(a);
                    String endCursor = journal.endCursor(a);
                    synchronized(state) {
                        if(!endCursor.equals(field(state,"cursor")) || Boolean.TRUE.equals(field(state,"pending")))throw new AssertionError("cursor/pending");
                    }
                    if(measure) {
                        recoveries.add((finished.get()-start)/1e6);readsPerRound.add(reads.get());
                        measuredNanos+=System.nanoTime()-start;cpuNanos+=os.getProcessCpuTime()-cpu;
                    }
                }
                connection.sync().del(RedisKeyspace.journal(a),RedisKeyspace.journal(b));
            }
        } finally { client.shutdown(); }
        System.out.printf(Locale.ROOT,"{\"mode\":\"%s\",\"backlog\":%d,\"rounds\":%d,\"sameSamples\":%d,\"otherSamples\":%d,\"sameP50Ms\":%.6f,\"sameP95Ms\":%.6f,\"sameP99Ms\":%.6f,\"otherP99Ms\":%.6f,\"allP99Ms\":%.6f,\"liveThroughput\":%.3f,\"cpuSeconds\":%.6f,\"catchupMillis\":%s,\"checkedReads\":%s,\"coherent\":true}%n",
                mode,mode.equals("healthy")?0:BACKLOG,ROUNDS,same.size(),other.size(),percentile(same,.5),percentile(same,.95),percentile(same,.99),percentile(other,.99),percentile(all,.99),LIVE*ROUNDS*1e9/measuredNanos,cpuNanos/1e9,recoveries,readsPerRound);
    }
}
