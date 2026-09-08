package io.tiercache.testkit;

import io.tiercache.spi.DistributedLock;
import io.tiercache.spi.DistributedLockProvider;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory {@link DistributedLockProvider} for tests and TCK harnesses.
 * Token-checked release and extend; leases expire on their own.
 */
public final class InMemoryLockProvider implements DistributedLockProvider {

    private final Map<String, HeldLock> locks = new ConcurrentHashMap<>();

    @Override
    public DistributedLock tryLock(String name, Duration lease) {
        HeldLock candidate = new HeldLock(name, UUID.randomUUID().toString(),
                System.nanoTime() + lease.toNanos());
        HeldLock result = locks.merge(name, candidate,
                (existing, candidateLock) ->
                        System.nanoTime() >= existing.expiresAtNanos ? candidateLock : existing);
        return result == candidate ? candidate : null;
    }

    final class HeldLock implements DistributedLock {
        private final String name;
        private final String token;
        private volatile long expiresAtNanos;

        HeldLock(String name, String token, long expiresAtNanos) {
            this.name = name;
            this.token = token;
            this.expiresAtNanos = expiresAtNanos;
        }

        @Override
        public synchronized boolean extend(Duration lease) {
            HeldLock current = locks.get(name);
            if (current != this || System.nanoTime() >= expiresAtNanos) {
                return false; // lost or expired
            }
            expiresAtNanos = System.nanoTime() + lease.toNanos();
            return true;
        }

        @Override
        public void release() {
            // Token-safe: never removes another holder's lock.
            locks.remove(name, this);
        }
    }
}
