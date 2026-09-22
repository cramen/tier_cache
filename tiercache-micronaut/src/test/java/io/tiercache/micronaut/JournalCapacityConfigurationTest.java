package io.tiercache.micronaut;
import io.tiercache.TierCacheFactory;
import io.tiercache.redis.RedisStreamJournal;
import io.micronaut.context.ApplicationContext;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class JournalCapacityConfigurationTest {
    @Test void invalidEnabledCapacitiesFailBeforeConnectionAndTraffic() {
        for(int value:new int[]{-1,0,1,32,63,64}) {
            var failure=assertThrows(RuntimeException.class,()->{
                try(var context=ApplicationContext.run(Map.of("tiercache.enabled",true,"tiercache.redis-uri","redis://127.0.0.1:1",
                        "tiercache.invalidation.journal-capacity",value))) {context.getBean(TierCacheFactory.class);}
            });check(failure,value);
        }
    }
    @Test void boundaryDefaultAndDisabledSettingsStartAndServeTraffic() {
        try(var redis=new GenericContainer<>(DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379)) {
            redis.start();String uri="redis://"+redis.getHost()+":"+redis.getMappedPort(6379);
            for(String value:new String[]{"65","10000","omitted"}) {
                Map<String,Object> props=new HashMap<>();props.put("tiercache.enabled",true);props.put("tiercache.redis-uri",uri);
                if(!value.equals("omitted"))props.put("tiercache.invalidation.journal-capacity",value);
                try(var context=ApplicationContext.run(props)) {
                    assertEquals(value.equals("65")?65:10000,context.getBean(RedisStreamJournal.class).capacity());
                    var cache=context.getBean(TierCacheFactory.class).getCache("c");cache.put("x","v");assertEquals("v",cache.get("x"));
                }
            }
            try(var context=ApplicationContext.run(Map.of("tiercache.enabled",true,"tiercache.redis-uri",uri,
                    "tiercache.invalidation.enabled",false,"tiercache.invalidation.journal-capacity",1))) {
                assertFalse(context.containsBean(RedisStreamJournal.class));
                var cache=context.getBean(TierCacheFactory.class).getCache("c");cache.put("x","v");assertEquals("v",cache.get("x"));
            }
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
                var properties=new TiercacheProperties(true,null,null,new TiercacheProperties.InvalidationProps(true,null,value),null);
                var failure=assertThrows(IllegalArgumentException.class,
                        ()->new TiercacheMicronautConfiguration().tiercacheInvalidationJournal(client,properties));
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
