package io.tiercache.spring;

import io.tiercache.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import java.time.Duration;
import static org.assertj.core.api.Assertions.assertThat;

class DegradationWindowBindingTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(TiercacheAutoConfiguration.class))
            .withUserConfiguration(TiercacheAutoConfigurationTest.InMemoryL2Config.class)
            .withPropertyValues("tiercache.enabled=true");
    private static CacheSettings actual(TierCacheFactory factory, String name) throws Exception {
        var cache=factory.getCache(name);var field=cache.getClass().getDeclaredField("settings");
        field.setAccessible(true);return (CacheSettings)field.get(cache);
    }
    @Test void omittedWindowIsOffInActualEngine() {
        runner.run(context->{assertThat(context).hasNotFailed();
            assertThat(actual(context.getBean(TierCacheFactory.class),"orders").degradationStaleTtl()).isEqualTo(Duration.ZERO);});
    }
    @Test void inheritanceOverrideAndExplicitZeroReachActualEngine() {
        runner.withPropertyValues("tiercache.defaults.degradation-stale-ttl=10m",
                "tiercache.caches.orders.degradation-stale-ttl=20m",
                "tiercache.caches.off.degradation-stale-ttl=0s").run(context->{
            assertThat(context).hasNotFailed();var factory=context.getBean(TierCacheFactory.class);
            assertThat(actual(factory,"inherit").degradationStaleTtl()).isEqualTo(Duration.ofMinutes(10));
            assertThat(actual(factory,"orders").degradationStaleTtl()).isEqualTo(Duration.ofMinutes(20));
            assertThat(actual(factory,"off").degradationStaleTtl()).isEqualTo(Duration.ZERO);
            assertThat(actual(factory,"off").l2Ttl()).isEqualTo(actual(factory,"inherit").l2Ttl());
        });
    }
    @Test void negativeGlobalAndNamedWindowsFailWithContext() {
        for(String name:new String[]{"defaults","caches.orders"}) {
            runner.withPropertyValues("tiercache."+name+".degradation-stale-ttl=-1s").run(context->{
                assertThat(context).hasFailed();var error=context.getStartupFailure();
                StringBuilder messages=new StringBuilder();while(error!=null){messages.append(error.getMessage());error=error.getCause();}
                assertThat(messages.toString()).contains("degradationStaleTtl",name.equals("defaults")?"global defaults":"orders");
            });
        }
    }
}
