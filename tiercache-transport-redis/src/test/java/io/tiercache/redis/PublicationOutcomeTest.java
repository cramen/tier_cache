package io.tiercache.redis;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.tiercache.*;
import io.tiercache.invalidation.InvalidationService;
import io.tiercache.spi.*;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class PublicationOutcomeTest {
    static InvalidationMessage message(){var id=UUID.randomUUID();return new InvalidationMessage("no-subscribers-"+id,"key",new Version(1,id),id,InvalidationMessage.Type.INVALIDATE);}
    @Test void nativeSuccessWithNoSubscribersAndClosedConnectionFailure() throws Exception {
        try(var server=new GenericContainer<>(DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379)) {
            server.start();String uri="redis://"+server.getHost()+":"+server.getMappedPort(6379);
            try(var client=RedisClient.create(uri);var remote=LettuceRemoteCache.<Object,Object>builder(uri).client(client).build()) {
                var transport=new LettucePubSubInvalidationTransport(client,new JdkCacheSerializer<>());
                assertEquals(PublicationOutcome.ACKNOWLEDGED,transport.publishAsync(message()).toCompletableFuture().get(3,TimeUnit.SECONDS));
                var original=new IllegalArgumentException("serializer payload must never be logged");
                CacheSerializer<Object> failingSerializer=new CacheSerializer<>() {
                    public byte[] toBytes(Object value){throw original;}
                    public Object fromBytes(byte[] bytes){throw new UnsupportedOperationException();}
                };
                try(var failing=new LettucePubSubInvalidationTransport(client,failingSerializer)) {
                    var error=assertThrows(CompletionException.class,()->failing.publishAsync(message()).toCompletableFuture().join());
                    assertSame(original,error.getCause());assertDoesNotThrow(()->failing.publish(message()));
                }
                var base=message();var update=new InvalidationMessage(base.cache(),base.key(),base.version(),base.originInstanceId(),InvalidationMessage.Type.UPDATE,"oversized");
                var received=new CompletableFuture<InvalidationMessage>();
                try(var capped=new LettucePubSubInvalidationTransport(client,new JdkCacheSerializer<>(),new JdkCacheSerializer<>(),1);
                    var subscription=transport.subscribe(update.cache(),received::complete)) {
                    assertEquals(PublicationOutcome.ACKNOWLEDGED,capped.publishAsync(update).toCompletableFuture().get(3,TimeUnit.SECONDS));
                    assertEquals(InvalidationMessage.Type.INVALIDATE,received.get(3,TimeUnit.SECONDS).type());
                }
                var outcomes=new AtomicLongArray(4);var sent=new AtomicInteger();
                var metrics=new CacheMetricsListener(){
                    public void onPublication(String c,PublicationOutcome o,long count){outcomes.addAndGet(o.ordinal(),count);}
                    public void onInvalidation(String c,Direction d){if(d==Direction.SENT)sent.incrementAndGet();}
                };
                try(var factory=TierCacheFactory.builder().remoteCache(remote)
                        .invalidation(v->new InvalidationService(transport,null,v.instanceId(),InvalidationListener.NOOP,metrics)).build()) {
                    var cache=factory.getCache("c");var field=LettucePubSubInvalidationTransport.class.getDeclaredField("connection");field.setAccessible(true);
                    ((StatefulConnection<?,?>)field.get(transport)).close();
                    assertDoesNotThrow(()->cache.put("x","committed"));assertEquals("committed",remote.get("x").value());
                    long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
                    while(outcomes.get(PublicationOutcome.FAILED.ordinal())==0 && System.nanoTime()<until)Thread.sleep(5);
                    assertEquals(1,sent.get());assertEquals(1,outcomes.get(PublicationOutcome.FAILED.ordinal()));assertEquals(0,outcomes.get(PublicationOutcome.ACKNOWLEDGED.ordinal()));
                }
                try(var streams=new LettuceStreamsInvalidationTransport(client,new JdkCacheSerializer<>(),new JdkCacheSerializer<>())) {
                    assertEquals(PublicationOutcome.NOT_REQUIRED,streams.publishAsync(message()).toCompletableFuture().get());
                }
            }
        }
    }
}
