package io.tiercache.internal;

import io.tiercache.internal.CircuitBreaker.Config;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Circuit breaker state machine: all transitions. */
class CircuitBreakerTest {

    private static final Config FAST = new Config(10, 0.5, 3, Duration.ofMillis(100), 2);

    @Test
    void opensAfterThresholdFailures() {
        List<String> events = new ArrayList<>();
        CircuitBreaker breaker = new CircuitBreaker(FAST, new CircuitBreaker.Listener() {
            @Override
            public void onOpen() {
                events.add("open");
            }

            @Override
            public void onClose() {
                events.add("close");
            }
        });
        breaker.onFailure();
        breaker.onFailure();
        assertFalse(breaker.isOpen(), "below minimum calls");
        breaker.onFailure(); // 3/3 failures = 100% >= 50%
        assertTrue(breaker.isOpen());
        assertEquals(List.of("open"), events);
        assertFalse(breaker.tryAcquire(), "open breaker fails fast");
    }

    @Test
    void successesKeepBreakerClosed() {
        CircuitBreaker breaker = new CircuitBreaker(FAST, new CircuitBreaker.Listener() {
            @Override
            public void onOpen() {
            }

            @Override
            public void onClose() {
            }
        });
        for (int i = 0; i < 10; i++) {
            breaker.onSuccess();
        }
        breaker.onFailure();
        breaker.onFailure();
        assertFalse(breaker.isOpen(), "2 failures in a window of 10 mostly-successful calls");
    }

    @Test
    void halfOpenProbeClosesAfterSuccesses() throws InterruptedException {
        List<String> events = new ArrayList<>();
        CircuitBreaker breaker = new CircuitBreaker(FAST, new CircuitBreaker.Listener() {
            @Override
            public void onOpen() {
                events.add("open");
            }

            @Override
            public void onClose() {
                events.add("close");
            }
        });
        breaker.onFailure();
        breaker.onFailure();
        breaker.onFailure();
        assertTrue(breaker.isOpen());
        Thread.sleep(150); // past halfOpenAfter
        assertTrue(breaker.tryAcquire(), "probe allowed after wait");
        breaker.onSuccess();
        assertTrue(breaker.tryAcquire());
        breaker.onSuccess();
        assertFalse(breaker.isOpen(), "closed after 2 successful probes");
        assertEquals(List.of("open", "close"), events);
    }

    @Test
    void failedProbeReopens() throws InterruptedException {
        CircuitBreaker breaker = new CircuitBreaker(FAST, new CircuitBreaker.Listener() {
            @Override
            public void onOpen() {
            }

            @Override
            public void onClose() {
            }
        });
        breaker.onFailure();
        breaker.onFailure();
        breaker.onFailure();
        assertTrue(breaker.isOpen());
        Thread.sleep(150);
        assertTrue(breaker.tryAcquire());
        breaker.onFailure(); // probe fails
        assertFalse(breaker.tryAcquire(), "reopened: fail fast again");
        Thread.sleep(150);
        assertTrue(breaker.tryAcquire(), "probe allowed after the second wait");
        breaker.onSuccess();
        breaker.onSuccess();
        assertFalse(breaker.isOpen());
    }

    @Test
    void invalidConfigRejected() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> new Config(0, 0.5, 3, Duration.ofSeconds(1), 2));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> new Config(10, 0.0, 3, Duration.ofSeconds(1), 2));
        org.junit.jupiter.api.Assertions.assertThrows(NullPointerException.class,
                () -> new Config(10, 0.5, 3, null, 2));
    }

    @Test
    void overwrittenFailureLeavesTheWindow() {
        // Window of 3, opens at >= 3 failures. F,F,S fills the window with 2
        // failures; the next F overwrites the oldest F — the failure count
        // must stay at 2, not grow.
        Config config = new Config(3, 0.75, 3, Duration.ofMillis(100), 1);
        CircuitBreaker breaker = new CircuitBreaker(config, new CircuitBreaker.Listener() {
            @Override
            public void onOpen() {
            }

            @Override
            public void onClose() {
            }
        });
        breaker.onFailure();
        breaker.onFailure();
        breaker.onSuccess();
        assertFalse(breaker.isOpen(), "2 failures in a full window of 3");
        breaker.onFailure(); // overwrites the oldest failure slot
        assertFalse(breaker.isOpen(),
                "overwritten failure must leave the window: still 2 of 3");
    }
}
