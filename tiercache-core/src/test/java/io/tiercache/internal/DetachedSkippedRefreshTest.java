package io.tiercache.internal;

import io.tiercache.NullPolicy;
import io.tiercache.spi.StoredEntry;
import org.junit.jupiter.api.Test;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.jupiter.api.Assertions.*;

/** Run the same skip/promotion and cleanup races through detached async dispatch. */
class DetachedSkippedRefreshTest extends SkippedRefreshTest {
    @Override boolean detached() { return true; }

    @Test
    void discardedRefreshCancelsDetachedDemandBeforeViewDrain() throws Exception {
        try (Rig rig = new Rig(false, NullPolicy.deny(), 1)) {
            AtomicBoolean open = new AtomicBoolean(true);
            AsyncAdmission budget = new AsyncAdmission(10);
            var view = new DefaultAsyncTierCache<>(rig.cache, rig.workers, budget, open::get);
            rig.queueRefresh(key -> fail("discarded refresh loader"));
            var refresh = (DefaultTierCache.DiscardableTask) rig.refresh.take();
            var claim = rig.claims.get(KEY);
            rig.removeCachedValue();
            var follower = view.getOrComputeAsync(KEY, key -> fail("no replacement after close"));
            AsyncResourceOwnershipTest.waitFor(() -> claim.result.getNumberOfDependents() == 1);
            open.set(false); // Factory CLOSED is visible before view drain.
            refresh.discard();
            assertTrue(follower.toCompletableFuture().isCancelled());
            assertTrue(rig.claims.isEmpty());
            view.closeOutstanding();
            AsyncResourceOwnershipTest.waitFor(() -> AsyncResourceOwnershipTest.occupied(budget) == 0);
        }
    }

    @Test
    void completionDuringAttachmentRegistrationRetiresExactlyOnce() throws Exception {
        try (Rig rig = new Rig(false, NullPolicy.deny(), 0)) {
            var claim = new LoadClaim<String, String>(null);
            claim.result.complete(LoadClaim.Outcome.result(StoredEntry.ofValue("done", null)));
            rig.claims.put(KEY, claim);
            AsyncAdmission budget = new AsyncAdmission(1);
            var view = new DefaultAsyncTierCache<>(rig.cache, rig.workers, budget, () -> true);
            assertEquals("done", view.getOrComputeAsync(KEY, key -> fail("already completed"))
                    .toCompletableFuture().get(5, TimeUnit.SECONDS));
            AsyncResourceOwnershipTest.waitFor(() -> AsyncResourceOwnershipTest.occupied(budget) == 0);
            view.closeOutstanding();
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void refreshRetiresBeforeBlockingUserNotification(boolean failure) throws Exception {
        var callbackGate = new java.util.concurrent.CountDownLatch(1);
        var entered = new java.util.concurrent.CountDownLatch(1);
        var nextGate = new java.util.concurrent.CountDownLatch(1);
        var nextEntered = new java.util.concurrent.CountDownLatch(1);
        try (Rig rig = new Rig(false, NullPolicy.deny(), 0)) {
            rig.queueRefresh(key -> {
                if (failure) throw new IllegalStateException("refresh failed");
                return "old";
            });
            var refresh = rig.refresh.take();
            var old = rig.claims.get(KEY);
            rig.removeCachedValue();
            var follower = rig.async.getOrComputeAsync(KEY, key -> fail("duplicate refresh"));
            AsyncResourceOwnershipTest.waitFor(() -> old.result.getNumberOfDependents() == 1);
            follower.whenComplete((value, error) -> { entered.countDown(); await(callbackGate); });
            var runner = rig.workers.submit(refresh);
            await(entered);
            rig.removeCachedValue();
            var next = rig.async.getOrComputeAsync(KEY, key -> {
                nextEntered.countDown(); await(nextGate); return "new";
            });
            await(nextEntered);
            var replacement = rig.claims.get(KEY);
            assertNotSame(old, replacement);
            callbackGate.countDown();
            runner.get(5, TimeUnit.SECONDS);
            assertSame(replacement, rig.claims.get(KEY));
            var another = rig.async.getOrComputeAsync(KEY, key -> fail("removed replacement"));
            AsyncResourceOwnershipTest.waitFor(() -> replacement.result.getNumberOfDependents() == 1);
            nextGate.countDown();
            assertEquals("new", next.toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertEquals("new", another.toCompletableFuture().get(5, TimeUnit.SECONDS));
        } finally { callbackGate.countDown(); nextGate.countDown(); }
    }
}
