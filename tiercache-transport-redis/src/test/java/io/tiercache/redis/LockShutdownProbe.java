package io.tiercache.redis;

import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

/** Test-only identity observation; no production lifecycle hook is needed. */
final class LockShutdownProbe {
    private LockShutdownProbe() { }

    static ScheduledExecutorService scheduler(LettuceLockProvider provider) {
        try {
            var field = LettuceLockProvider.class.getDeclaredField("compensationScheduler");
            field.setAccessible(true);
            return (ScheduledExecutorService) field.get(provider);
        } catch (ReflectiveOperationException e) { throw new AssertionError(e); }
    }

    static void install(LettuceLockProvider provider, ScheduledExecutorService scheduler) {
        try {
            var field = LettuceLockProvider.class.getDeclaredField("compensationScheduler");
            field.setAccessible(true);
            assertNull(field.get(provider), "install only before provider use");
            field.set(provider, scheduler);
        } catch (ReflectiveOperationException e) { throw new AssertionError(e); }
    }

    static void assertStopped(LettuceLockProvider provider) throws InterruptedException {
        assertTrue(provider.isClosed());
        assertEquals(0, provider.pendingCompensations());
        var scheduler = scheduler(provider);
        if (scheduler == null) return;
        assertTrue(scheduler.isShutdown(), () -> diagnostics(provider));
        assertTrue(scheduler.awaitTermination(5, TimeUnit.SECONDS), () -> diagnostics(provider));
    }

    static String diagnostics(LettuceLockProvider provider) {
        var scheduler = scheduler(provider);
        var result = new StringBuilder("provider=").append(System.identityHashCode(provider))
                .append(" scheduler=").append(scheduler).append(" closed=").append(provider.isClosed())
                .append(" pending=").append(provider.pendingCompensations())
                .append(" atNanos=").append(System.nanoTime());
        // Supplemental stacks only: names are not evidence of ownership.
        Thread.getAllStackTraces().forEach((thread, stack) -> {
            if (thread.getName().startsWith("tiercache-lock-compensation"))
                result.append('\n').append(thread).append(Arrays.toString(stack));
        });
        return result.toString();
    }
}
