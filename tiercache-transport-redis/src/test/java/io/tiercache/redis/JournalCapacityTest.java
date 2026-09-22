package io.tiercache.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class JournalCapacityTest {
    @SuppressWarnings("unchecked")
    static StatefulRedisConnection<byte[],byte[]> connection(AtomicInteger calls) {
        return (StatefulRedisConnection<byte[],byte[]>)Proxy.newProxyInstance(JournalCapacityTest.class.getClassLoader(),
                new Class<?>[]{StatefulRedisConnection.class},(p,m,a)->{calls.incrementAndGet();return null;});
    }
    @ParameterizedTest @ValueSource(ints={-1,0,1,32,63,64})
    void invalidBothConstructorsDoNotAccessConnection(int capacity) {
        var calls=new AtomicInteger();var connection=connection(calls);var serializer=new JdkCacheSerializer<Object>();
        for(boolean separate:new boolean[]{false,true}) {
            var error=assertThrows(IllegalArgumentException.class,()->{
                if(separate)new RedisStreamJournal(connection,capacity,serializer,serializer);
                else new RedisStreamJournal(connection,capacity,serializer);
            });
            assertTrue(error.getMessage().contains("tiercache.invalidation.journal-capacity"));
            assertTrue(error.getMessage().contains(String.valueOf(capacity)));assertTrue(error.getMessage().contains("65"));
            assertTrue(error.getMessage().contains("64"));assertEquals(0,calls.get());
        }
    }
    @ParameterizedTest @ValueSource(ints={65,10000})
    void validBoundariesReachConnectionWithoutClamping(int capacity) {
        var calls=new AtomicInteger();var serializer=new JdkCacheSerializer<Object>();
        assertEquals(capacity,new RedisStreamJournal(connection(calls),capacity,serializer).capacity());
        assertEquals(capacity,new RedisStreamJournal(connection(calls),capacity,serializer,serializer).capacity());
        assertEquals(2,calls.get());
    }
}
