package io.tiercache.invalidation;

import io.tiercache.*;
import io.tiercache.spi.*;
import io.tiercache.testkit.InMemoryJournal;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class GapAuthorizationTest {
    static class Transport implements InvalidationTransport {
        InvalidationGapHandler gaps; int subscriptions;
        public void publish(InvalidationMessage message) { }
        public AutoCloseable subscribe(String cache, java.util.function.Consumer<InvalidationMessage> handler) { subscriptions++; return () -> { }; }
        public void setGapHandler(InvalidationGapHandler handler) { gaps = handler; }
        public boolean requiresRegistrationReset() { return true; }
        public void close() { }
    }
    @Test void registrationAndResetProofsAreCacheAndGenerationScoped() throws Exception {
        var transport = new Transport(); var target = new RecoveryProtocolTest.Target();
        try (var service = new InvalidationService(transport, new InMemoryJournal(100), UUID.randomUUID(), InvalidationListener.NOOP)) {
            service.registerTarget("c", target); assertEquals(1, target.clears.get());
            var first = transport.gaps.registrationBaseline("c"); assertNotNull(first);
            assertTrue(transport.gaps.isCurrent("c", first)); assertFalse(transport.gaps.isCurrent("other", first));
            assertFalse(transport.gaps.isCurrent("c", new RecoveryResult(first.status(), first.baseline(), first.generation())));
            var reset = transport.gaps.reset("c").toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertTrue(transport.gaps.isCurrent("c", reset)); assertFalse(transport.gaps.isCurrent("c", first));
            target.evictAllL1(); assertFalse(transport.gaps.isCurrent("c", reset));
            var again = transport.gaps.reset("c").toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertTrue(transport.gaps.isCurrent("c", again));
            service.registerTarget("c", new RecoveryProtocolTest.Target());
            assertEquals(2, transport.subscriptions); assertFalse(transport.gaps.isCurrent("c", again));
            service.close(); assertFalse(transport.gaps.isCurrent("c", again));
        }
    }
    @Test void retargetBeforeResetCompletionCannotIssueTheOldProof() throws Exception {
        var transport = new Transport(); var first = new RecoveryProtocolTest.Target(); var second = new RecoveryProtocolTest.Target();
        var owner = new AtomicReference<InvalidationService>(); var once = new AtomicBoolean();
        var replacement = new CompletableFuture<CompletionStage<Void>>();
        try (var service = new InvalidationService(transport, new InMemoryJournal(100), UUID.randomUUID(), cache -> {
            if (once.compareAndSet(false, true)) replacement.complete(owner.get().registerTargetAsync(cache, second));
        })) {
            owner.set(service); service.registerTarget("c", first);
            var result = transport.gaps.reset("c").toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertFalse(transport.gaps.isCurrent("c", result), "retired waiter must not issue a proof");
            replacement.get(5, TimeUnit.SECONDS).toCompletableFuture().get(5, TimeUnit.SECONDS);
            var current = transport.gaps.reset("c").toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertTrue(transport.gaps.isCurrent("c", current)); assertEquals(2, second.clears.get());
        }
    }
}
