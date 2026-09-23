package io.tiercache.spring;

import io.tiercache.*;
import io.tiercache.spi.CacheMetricsListener;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.*;
import org.springframework.context.annotation.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import reactor.core.publisher.Mono;
import static org.junit.jupiter.api.Assertions.*;

class AsyncCacheInterceptionTest {
    static class State {
        final AtomicInteger calls=new AtomicInteger();
        final CompletableFuture<String> result=new CompletableFuture<>();
        final CountDownLatch entered=new CountDownLatch(1),joined=new CountDownLatch(1);
        volatile Thread loaderThread;
    }
    public static class Service {
        final State state;
        Service(State state){this.state=state;}
        @Cacheable(cacheNames="future-values",sync=true)
        public CompletableFuture<String> future(String key){
            state.calls.incrementAndGet();state.loaderThread=Thread.currentThread();state.entered.countDown();return state.result;
        }
        @Cacheable(cacheNames="mono-values",sync=true)
        public Mono<String> mono(String key){
            state.calls.incrementAndGet();state.loaderThread=Thread.currentThread();state.entered.countDown();return Mono.fromFuture(state.result);
        }
        @Cacheable(cacheNames="sync-values",sync=true)
        public String sync(String key){
            state.calls.incrementAndGet();state.entered.countDown();return state.result.join();
        }
    }
    @Configuration(proxyBeanMethods=false) @EnableCaching
    static class Config {
        @Bean State state(){return new State();}
        @Bean(destroyMethod="close") TierCacheFactory factory(State state){
            return TierCacheFactory.builder().remoteCache(new InMemoryRemoteCache<>()).asyncExecutorThreads(4)
                    .metricsListener(new CacheMetricsListener(){public void onRequest(String c,Outcome o){if(o==Outcome.COALESCED)state.joined.countDown();}}).build();
        }
        @Bean CacheManager cacheManager(TierCacheFactory factory){return new TierCacheManager(factory);}
        @Bean Service service(State state){return new Service(state);}
    }
    @Test void synchronizedFutureAnnotationUsesNonBlockingCoalescedRetrieval() throws Exception {
        try(var context=new AnnotationConfigApplicationContext(Config.class)) {
            var state=context.getBean(State.class);var service=context.getBean(Service.class);Thread caller=Thread.currentThread();
            try {
                var first=service.future("x");AsyncRetrievalTest.await(state.entered);
                var second=service.future("x");AsyncRetrievalTest.await(state.joined);
                assertFalse(first.isDone());assertNotSame(caller,state.loaderThread);assertEquals(1,state.calls.get());
                state.result.complete("value");assertEquals("value",first.get(2,TimeUnit.SECONDS));assertEquals("value",second.get(2,TimeUnit.SECONDS));
                assertEquals("value",service.future("x").get(2,TimeUnit.SECONDS));assertEquals(1,state.calls.get());
            } finally {state.result.complete("cleanup");}
        }
    }
    @Test void synchronizedMonoAnnotationUsesSameCoalescedRetrieval() throws Exception {
        try(var context=new AnnotationConfigApplicationContext(Config.class)) {
            var state=context.getBean(State.class);var service=context.getBean(Service.class);Thread caller=Thread.currentThread();
            try {
                var first=service.mono("x").toFuture();AsyncRetrievalTest.await(state.entered);
                var second=service.mono("x").toFuture();AsyncRetrievalTest.await(state.joined);
                assertFalse(first.isDone());assertNotSame(caller,state.loaderThread);assertEquals(1,state.calls.get());
                state.result.complete("value");assertEquals("value",first.get(2,TimeUnit.SECONDS));assertEquals("value",second.get(2,TimeUnit.SECONDS));
                assertEquals("value",service.mono("x").toFuture().get(2,TimeUnit.SECONDS));assertEquals(1,state.calls.get());
            } finally {state.result.complete("cleanup");}
        }
    }
    @Test void synchronousSyncTrueStillCoalescesThroughCallablePath() throws Exception {
        var callers=Executors.newFixedThreadPool(2);
        try(var context=new AnnotationConfigApplicationContext(Config.class)) {
            var state=context.getBean(State.class);var service=context.getBean(Service.class);
            try {
                var first=callers.submit(()->service.sync("x"));AsyncRetrievalTest.await(state.entered);
                var second=callers.submit(()->service.sync("x"));AsyncRetrievalTest.await(state.joined);
                state.result.complete("value");assertEquals("value",first.get(2,TimeUnit.SECONDS));assertEquals("value",second.get(2,TimeUnit.SECONDS));
                assertEquals("value",service.sync("x"));assertEquals(1,state.calls.get());
            } finally {state.result.complete("cleanup");}
        } finally {callers.shutdownNow();}
    }
    @Test void annotatedFuturePreservesExceptionalResultCause() {
        try(var context=new AnnotationConfigApplicationContext(Config.class)) {
            var state=context.getBean(State.class);var service=context.getBean(Service.class);
            var result=service.future("failed");AsyncRetrievalTest.await(state.entered);
            var error=new IllegalArgumentException("application failure");state.result.completeExceptionally(error);
            assertSame(error,AsyncRetrievalTest.failure(result));
        }
    }

}
