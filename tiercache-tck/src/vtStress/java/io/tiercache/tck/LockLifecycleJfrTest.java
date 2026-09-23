package io.tiercache.tck;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.tiercache.redis.LettuceLockProvider;
import io.tiercache.internal.LockProviderClosedException;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordingFile;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import java.time.Duration;
import java.nio.file.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;

class LockLifecycleJfrTest {
    @Test void lazyConnectionCloseRaceDoesNotPinVirtualThreads() throws Exception {
        var entered=new CountDownLatch(1); var resume=new CountDownLatch(1);
        var connection=new AtomicReference<StatefulRedisConnection<String,String>>();
        try(var redis=new GenericContainer<>(DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379)) {
            redis.start();
            class PausedClient extends RedisClient {
                PausedClient(){super(null,RedisURI.create("redis://"+redis.getHost()+":"+redis.getMappedPort(6379)));}
                @Override public StatefulRedisConnection<String,String> connect(){
                    var c=super.connect(); connection.set(c); entered.countDown();
                    try {assertTrue(resume.await(10,TimeUnit.SECONDS));}catch(InterruptedException e){throw new AssertionError(e);}
                    return c;
                }
            }
            Path path=Path.of("build/reports/lock-lifecycle-jdk21.jfr");Files.createDirectories(path.getParent());
            try(var client=new PausedClient();var threads=Executors.newVirtualThreadPerTaskExecutor();var recording=new Recording()) {
                recording.enable("jdk.VirtualThreadPinned").withThreshold(Duration.ZERO).withStackTrace();
                recording.setDestination(path);recording.start();
                var provider=new LettuceLockProvider(client);
                var call=threads.submit(()->provider.tryLock("jfr-close",Duration.ofSeconds(10)));
                try {
                    assertTrue(entered.await(10,TimeUnit.SECONDS));
                    threads.submit(provider::close).get(1,TimeUnit.SECONDS);
                } finally {resume.countDown();provider.close();}
                assertInstanceOf(LockProviderClosedException.class,
                        assertThrows(ExecutionException.class,()->call.get(5,TimeUnit.SECONDS)).getCause());
                assertFalse(connection.get().isOpen());recording.stop();
            }
            long pinned=RecordingFile.readAllEvents(path).stream()
                    .filter(e->e.getEventType().getName().equals("jdk.VirtualThreadPinned"))
                    .filter(e->e.getStackTrace()!=null && e.getStackTrace().getFrames().stream()
                            .anyMatch(f->f.getMethod().getType().getName().startsWith("io.tiercache.redis.LettuceLockProvider")))
                    .count();
            assertEquals(0,pinned,"lazy connection/close must not hold an intrinsic monitor");
        }
    }
}
