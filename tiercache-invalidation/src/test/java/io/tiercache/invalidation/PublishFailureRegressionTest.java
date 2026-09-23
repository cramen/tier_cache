package io.tiercache.invalidation;
import io.tiercache.*;
import io.tiercache.spi.*;
import io.tiercache.testkit.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PublishFailureRegressionTest {
    @Test void synchronousPublicationFailureCannotReplaceCommittedWriteResult() {
        var remote=new InMemoryRemoteCache<Object,Object>();
        InvalidationTransport transport=new InvalidationTransport(){
            public void publish(InvalidationMessage m){throw new IllegalStateException("publication failed");}
            public AutoCloseable subscribe(String c,java.util.function.Consumer<InvalidationMessage> h){return ()->{};}
            public void close(){}
        };
        try(var factory=TierCacheFactory.builder().remoteCache(remote)
                .invalidation(v->new InvalidationService(transport,null,v.instanceId(),InvalidationListener.NOOP)).build()) {
            var cache=factory.getCache("c");assertDoesNotThrow(()->cache.put("x","committed"));
            assertEquals("committed",remote.get("x").value());assertEquals("committed",cache.get("x"));
        }
    }
}
