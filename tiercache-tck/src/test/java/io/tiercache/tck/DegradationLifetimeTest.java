package io.tiercache.tck;

import io.tiercache.*;
import io.tiercache.internal.*;
import io.tiercache.redis.*;
import io.tiercache.spi.*;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class DegradationLifetimeTest {
    @Test void realRedisOutageKeepsFreshnessAndBoundsCoalescedStaleFallback() throws Exception {
        var now=new AtomicLong(1);var stale=new AtomicInteger();var loads=new AtomicInteger();
        var joined=new CountDownLatch(7);var releaseLoader=new CountDownLatch(1);
        var settings=new CacheSettings(100,Duration.ofMinutes(30),null,Duration.ofHours(2),0,
                NullPolicy.deny(),InvalidationMode.INVALIDATE,65536,Duration.ZERO,false,
                Duration.ofSeconds(1),Duration.ofMinutes(30));
        try(var redis=new GenericContainer<>(DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379)) {
            redis.start();String uri="redis://"+redis.getHost()+":"+redis.getMappedPort(6379);
            try(var remote=LettuceRemoteCache.<String,String>builder(uri).commandTimeout(Duration.ofSeconds(1)).build()) {
                var breaker=new CircuitBreaker(new CircuitBreaker.Config(1,1,1,Duration.ofSeconds(5),1),
                        new CircuitBreaker.Listener(){public void onOpen(){}public void onClose(){}});
                var l1=new CaffeineLocalCache<String,String>(settings,now::get);
                var cache=new DefaultTierCache<>("lifetime",l1,new CircuitBreakerRemoteCache<>(remote,breaker),settings,true,
                        null,null,new VersionGenerator(),null,breaker,new CacheMetricsListener(){
                            public void onRequest(String c,Outcome o){if(o==Outcome.STALE_DEGRADED)stale.incrementAndGet();if(o==Outcome.COALESCED)joined.countDown();}
                        },null,new TtlJitter(),()->true,now::get);
                cache.put("x","original");l1.clear();assertEquals("original",cache.get("x"));
                assertNull(remote.get("x").localFreshness());
                redis.getDockerClient().pauseContainerCmd(redis.getContainerId()).exec();
                var pool=Executors.newFixedThreadPool(8);
                try {
                    assertNull(cache.get("uncached-probe"));assertTrue(breaker.isOpen());
                    now.set(1+Duration.ofMinutes(29).toNanos());
                    assertEquals("original",cache.getOrCompute("x",k->{fail("fresh load");return null;}));assertEquals(0,stale.get());
                    for(int minute=30;minute<60;minute++) {
                        now.set(1+Duration.ofMinutes(minute).toNanos());
                        assertEquals("original",cache.getOrCompute("x",k->{fail("stale load");return null;}));
                    }
                    assertEquals(30,stale.get());now.set(1+Duration.ofMinutes(60).toNanos());
                    var requests=new ArrayList<Future<String>>();
                    for(int i=0;i<8;i++) requests.add(pool.submit(()->cache.getOrCompute("x",k->{
                        loads.incrementAndGet();try{assertTrue(releaseLoader.await(5,TimeUnit.SECONDS));}catch(InterruptedException e){throw new AssertionError(e);}return "local-new";
                    })));
                    assertTrue(joined.await(3,TimeUnit.SECONDS));releaseLoader.countDown();
                    for(var request:requests)assertEquals("local-new",request.get(3,TimeUnit.SECONDS));assertEquals(1,loads.get());
                } finally {
                    releaseLoader.countDown();pool.shutdownNow();
                    redis.getDockerClient().unpauseContainerCmd(redis.getContainerId()).exec();
                }
                long deadline=System.nanoTime()+Duration.ofSeconds(10).toNanos();
                while(breaker.state()!=BreakerState.CLOSED&&System.nanoTime()<deadline){cache.get("recovery-probe");Thread.sleep(25);}
                assertEquals(BreakerState.CLOSED,breaker.state());
                // A disconnected local write does not rewrite the old remote copy on recovery.
                assertEquals("original",remote.get("x").value());cache.evictAllL1();
                assertEquals("original",cache.get("x"));
            }
        }
    }
}
