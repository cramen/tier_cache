package io.tiercache.micronaut;

import io.micronaut.context.ApplicationContext;
import io.tiercache.*;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DegradationWindowBindingTest {
    private static CacheSettings actual(TierCacheFactory factory,String name)throws Exception {
        var cache=factory.getCache(name);var field=cache.getClass().getDeclaredField("settings");
        field.setAccessible(true);return (CacheSettings)field.get(cache);
    }
    @Test void omittedWindowIsOffInActualEngine() throws Exception {
        try(var context=ApplicationContext.run(Map.of("tiercache.enabled",true),"tiercache-inmemory-l2")) {
            assertThat(actual(context.getBean(TierCacheFactory.class),"orders").degradationStaleTtl()).isEqualTo(Duration.ZERO);
        }
    }
    @Test void inheritanceOverrideAndExplicitZeroReachActualEngine() throws Exception {
        try(var context=ApplicationContext.run(Map.of("tiercache.enabled",true,
                "tiercache.defaults.degradation-stale-ttl","10m",
                "tiercache.caches.orders.degradation-stale-ttl","20m",
                "tiercache.caches.off.degradation-stale-ttl","0s"),"tiercache-inmemory-l2")) {
            var factory=context.getBean(TierCacheFactory.class);
            assertThat(actual(factory,"inherit").degradationStaleTtl()).isEqualTo(Duration.ofMinutes(10));
            assertThat(actual(factory,"orders").degradationStaleTtl()).isEqualTo(Duration.ofMinutes(20));
            assertThat(actual(factory,"off").degradationStaleTtl()).isEqualTo(Duration.ZERO);
            assertThat(actual(factory,"off").l2Ttl()).isEqualTo(actual(factory,"inherit").l2Ttl());
        }
    }
    @Test void negativeGlobalAndNamedWindowsFailWithContext() {
        for(String name:new String[]{"defaults","caches.orders"}) {
            var error=assertThrows(RuntimeException.class,()->{
                try(var context=ApplicationContext.run(Map.of("tiercache.enabled",true,
                        "tiercache."+name+".degradation-stale-ttl","-1s"),"tiercache-inmemory-l2")) {
                    context.getBean(TierCacheFactory.class);
                }
            });
            StringBuilder messages=new StringBuilder();Throwable cause=error;
            while(cause!=null){messages.append(cause.getMessage());cause=cause.getCause();}
            assertThat(messages.toString()).contains("degradationStaleTtl",name.equals("defaults")?"global defaults":"orders");
        }
    }
}
