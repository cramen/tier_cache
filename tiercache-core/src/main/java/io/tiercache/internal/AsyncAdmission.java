package io.tiercache.internal;

import java.util.concurrent.atomic.AtomicLong;

/** Factory-wide bound on retained async work; internal, not a supported API. */
public final class AsyncAdmission {
    private final long capacity;
    private final AtomicLong occupied = new AtomicLong();

    public AsyncAdmission(int workers) {
        if (workers <= 0) throw new IllegalArgumentException("workers must be positive");
        capacity = (long) workers + 10_000;
    }

    boolean acquire() {
        long current;
        do {
            current = occupied.get();
            if (current == capacity) return false;
        } while (!occupied.compareAndSet(current, current + 1));
        return true;
    }

    void release() { occupied.decrementAndGet(); }
}
