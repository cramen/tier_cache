package io.tiercache.reactor;

import io.tiercache.CacheOverride;
import io.tiercache.CacheSettings;
import io.tiercache.InvalidationMessage;
import io.tiercache.TierCacheFactory;
import io.tiercache.VersionGenerator;
import io.tiercache.spi.CacheMetricsListener;
import io.tiercache.spi.DegradationListener;
import io.tiercache.spi.DistributedLockProvider;
import io.tiercache.spi.InvalidationEventListener;
import io.tiercache.spi.InvalidationHandler;
import io.tiercache.spi.LocalCache;
import io.tiercache.spi.RemoteCache;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Reactor entry point over {@link TierCacheFactory}: hands out
 * {@link ReactorTierCache} facades and exposes inbound invalidation events
 * as cold {@link Flux}es (design D3).
 *
 * <p>The factory must be built through {@link #builder()}: the builder
 * installs a fan-out listener on the core invalidation-event hook at build
 * time, so exactly one transport subscription is shared by any number of
 * flux subscribers.
 */
public final class ReactorCacheFactory implements AutoCloseable {

    /** Default subscriber-side buffer capacity for {@link #invalidationEvents}. */
    public static final int DEFAULT_INVALIDATION_BUFFER_CAPACITY = 64;

    private final TierCacheFactory delegate;
    private final InvalidationEventFanout fanout;
    private final ConcurrentHashMap<String, ReactorTierCache<?, ?>> caches = new ConcurrentHashMap<>();

    private ReactorCacheFactory(TierCacheFactory delegate, InvalidationEventFanout fanout) {
        this.delegate = delegate;
        this.fanout = fanout;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the Reactor facade for the named cache, memoized by name.
     * The underlying {@link TierCacheFactory#getCache} and
     * {@link TierCacheFactory#asyncCache} are memoized, so all facades for
     * one name share the same cache and the same async view (whose
     * operations run on the factory's shared daemon executor).
     */
    @SuppressWarnings("unchecked")
    public <K, V> ReactorTierCache<K, V> reactorCache(String name) {
        return (ReactorTierCache<K, V>) caches.computeIfAbsent(name,
                n -> new ReactorTierCache<>(delegate.asyncCache(n)));
    }

    /** True while the L2 circuit breaker is open (L1-only degraded mode). */
    public boolean isDegraded() {
        return delegate.isDegraded();
    }

    /**
     * Cold flux of inbound invalidation events applied to {@code cacheName},
     * in arrival order, with the default buffer capacity and drops ignored.
     * See {@link #invalidationEvents(String, int, Consumer)}.
     */
    public Flux<InvalidationMessage> invalidationEvents(String cacheName) {
        return invalidationEvents(cacheName, DEFAULT_INVALIDATION_BUFFER_CAPACITY, event -> {
        });
    }

    /**
     * Cold flux of inbound invalidation events applied to {@code cacheName},
     * in arrival order. Each subscriber registers its own listener on
     * subscription and unregisters on cancellation or termination.
     *
     * <p>Producers never block: the bridge uses
     * {@link FluxSink.OverflowStrategy#IGNORE} (emission is unconditional)
     * followed by a bounded subscriber-side buffer of {@code bufferCapacity}
     * events; once that buffer is full, further events are dropped for that
     * subscriber (newest dropped first) and each drop is reported through
     * {@code onEventDropped}.
     */
    public Flux<InvalidationMessage> invalidationEvents(
            String cacheName,
            int bufferCapacity,
            Consumer<InvalidationMessage> onEventDropped) {
        Objects.requireNonNull(onEventDropped, "onEventDropped");
        return Flux.<InvalidationMessage>create(sink -> {
            Runnable unregister = fanout.register(cacheName, sink::next);
            sink.onCancel(unregister::run);
            sink.onDispose(unregister::run);
        }, FluxSink.OverflowStrategy.IGNORE)
                .onBackpressureBuffer(bufferCapacity, onEventDropped,
                        BufferOverflowStrategy.DROP_LATEST);
    }

    /** Subscriber count for {@code cacheName}; test/diagnostic probe. */
    int subscriberCount(String cacheName) {
        return fanout.registrationCount(cacheName);
    }

    @Override
    public void close() {
        delegate.close();
    }

    /**
     * Builder mirroring {@link TierCacheFactory.Builder}; pass-throughs
     * delegate verbatim, only {@link #invalidationEventListener} is composed
     * with the internal fan-out.
     */
    public static final class Builder {

        private final TierCacheFactory.Builder delegate = TierCacheFactory.builder();
        private final InvalidationEventFanout fanout = new InvalidationEventFanout();
        private InvalidationEventListener userEventListener = InvalidationEventListener.NOOP;

        public Builder defaults(CacheSettings defaults) {
            delegate.defaults(defaults);
            return this;
        }

        public Builder cache(String name, CacheOverride override) {
            delegate.cache(name, override);
            return this;
        }

        /** The L2 implementation shared by all caches. Required. */
        public Builder remoteCache(RemoteCache<?, ?> remoteCache) {
            delegate.remoteCache(remoteCache);
            return this;
        }

        public Builder localCacheFactory(BiFunction<String, CacheSettings, LocalCache<?, ?>> factory) {
            delegate.localCacheFactory(factory);
            return this;
        }

        public Builder lockProvider(DistributedLockProvider lockProvider) {
            delegate.lockProvider(lockProvider);
            return this;
        }

        public Builder invalidation(Function<VersionGenerator, InvalidationHandler> invalidationFactory) {
            delegate.invalidation(invalidationFactory);
            return this;
        }

        public Builder metricsListener(CacheMetricsListener listener) {
            delegate.metricsListener(listener);
            return this;
        }

        public Builder degradationListener(DegradationListener listener) {
            delegate.degradationListener(listener);
            return this;
        }

        /**
         * Application-facing observer of inbound invalidation events;
         * invoked after the internal flux fan-out.
         */
        public Builder invalidationEventListener(InvalidationEventListener listener) {
            userEventListener = Objects.requireNonNull(listener, "listener");
            return this;
        }

        public Builder disableSingleflight() {
            delegate.disableSingleflight();
            return this;
        }

        public Builder disableDistributedCoordination() {
            delegate.disableDistributedCoordination();
            return this;
        }

        public Builder disableCircuitBreaker() {
            delegate.disableCircuitBreaker();
            return this;
        }

        public ReactorCacheFactory build() {
            delegate.invalidationEventListener((cache, event) -> {
                fanout.onEvent(cache, event);
                userEventListener.onEvent(cache, event);
            });
            return new ReactorCacheFactory(delegate.build(), fanout);
        }
    }
}

/**
 * In-process fan-out over the single core invalidation-event hook: each flux
 * subscriber holds its own registration; dispatch is an unconditional
 * {@code FluxSink.next} on a serialized sink, so event producers are never
 * blocked by slow subscribers. Duplicated from the Kotlin module's registry
 * (that one is {@code internal} to its module).
 */
final class InvalidationEventFanout {

    private record Registration(String cacheName, Consumer<InvalidationMessage> handler) {
    }

    private final List<Registration> registrations = new CopyOnWriteArrayList<>();

    /** Registers {@code handler} for {@code cacheName}; returns the unregister action. */
    Runnable register(String cacheName, Consumer<InvalidationMessage> handler) {
        Registration registration = new Registration(cacheName, handler);
        registrations.add(registration);
        return () -> registrations.remove(registration);
    }

    void onEvent(String cacheName, InvalidationMessage event) {
        for (Registration registration : registrations) {
            if (registration.cacheName().equals(cacheName)) {
                registration.handler().accept(event);
            }
        }
    }

    int registrationCount(String cacheName) {
        int count = 0;
        for (Registration registration : registrations) {
            if (registration.cacheName().equals(cacheName)) {
                count++;
            }
        }
        return count;
    }
}
