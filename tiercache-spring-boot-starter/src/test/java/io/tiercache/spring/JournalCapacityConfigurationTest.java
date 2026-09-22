package io.tiercache.spring;
import io.tiercache.TierCacheFactory;
import io.tiercache.redis.RedisStreamJournal;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import static org.junit.jupiter.api.Assertions.*;

class JournalCapacityConfigurationTest {
    final ApplicationContextRunner runner=new ApplicationContextRunner().withConfiguration(AutoConfigurations.of(TiercacheAutoConfiguration.class))
            .withPropertyValues("tiercache.enabled=true");
    @Test void invalidEnabledCapacitiesFailBeforeConnectionAndTraffic() {
        for(int value:new int[]{-1,0,1,32,63,64}) runner.withPropertyValues("tiercache.redis-uri=redis://127.0.0.1:1",
                "tiercache.invalidation.journal-capacity="+value).run(context->{
            assertNotNull(context.getStartupFailure());check(context.getStartupFailure(),value);
            assertThrows(IllegalStateException.class,()->context.getBean(TierCacheFactory.class));
        });
    }
    @Test void boundaryDefaultAndDisabledSettingsStartAndServeTraffic() {
        try(var redis=new GenericContainer<>(DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379)) {
            redis.start();String uri="redis://"+redis.getHost()+":"+redis.getMappedPort(6379);
            for(String value:new String[]{"65","10000","omitted"}) {
                var configured=runner.withPropertyValues("tiercache.redis-uri="+uri);
                if(!value.equals("omitted"))configured=configured.withPropertyValues("tiercache.invalidation.journal-capacity="+value);
                configured.run(context->{assertNull(context.getStartupFailure());
                    assertEquals(value.equals("65")?65:10000,context.getBean(RedisStreamJournal.class).capacity());
                    var cache=context.getBean(TierCacheFactory.class).getCache("c");cache.put("x","v");assertEquals("v",cache.get("x"));
                });
            }
            runner.withPropertyValues("tiercache.redis-uri="+uri,"tiercache.invalidation.enabled=false","tiercache.invalidation.journal-capacity=1")
                    .run(context->{assertNull(context.getStartupFailure());assertTrue(context.getBeansOfType(RedisStreamJournal.class).isEmpty());
                        var cache=context.getBean(TierCacheFactory.class).getCache("c");cache.put("x","v");assertEquals("v",cache.get("x"));});
        }
    }

    static class NeverConnect extends io.lettuce.core.RedisClient {
        int connects;
        @Override public <K,V> io.lettuce.core.api.StatefulRedisConnection<K,V> connect(io.lettuce.core.codec.RedisCodec<K,V> codec) {
            connects++;throw new AssertionError("invalid configuration attempted a connection");
        }
    }
    @Test void suppliedClientIsNotConnectedBeforeValidation() {
        try(var client=new NeverConnect()) {
            for(int value:new int[]{-1,0,1,32,63,64}) {
                var properties=new TiercacheProperties(); properties.getInvalidation().setJournalCapacity(value);
                var failure=assertThrows(IllegalArgumentException.class,
                        ()->new TiercacheAutoConfiguration().tiercacheInvalidationJournal(client,properties));
                check(failure,value);assertEquals(0,client.connects);
            }
        }
    }
    static String messages(Throwable cause) {
        StringBuilder out=new StringBuilder();while(cause!=null){out.append(cause.getMessage()).append("\n");cause=cause.getCause();}return out.toString();
    }
    static void check(Throwable failure,int capacity) {
        String text=messages(failure);assertTrue(text.contains("tiercache.invalidation.journal-capacity="+capacity),text);
        assertTrue(text.contains("65"),text);assertTrue(text.contains("64-event"),text);
    }
}
