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
 *
 * @since 0.4.0
 */
public final class ReactorCacheFactory implements AutoCloseable {

    /**
     * Default subscriber-side buffer capacity for {@link #invalidationEvents}.
     *
     * @since 0.4.0
     */
    public static final int DEFAULT_INVALIDATION_BUFFER_CAPACITY = 64;

    private final TierCacheFactory delegate;
    private final InvalidationEventFanout fanout;
    private final ConcurrentHashMap<String, ReactorTierCache<?, ?>> caches = new ConcurrentHashMap<>();

    private ReactorCacheFactory(TierCacheFactory delegate, InvalidationEventFanout fanout) {
        this.delegate = delegate;
        this.fanout = fanout;
    }

    /**
     * Returns a new builder for a {@link ReactorCacheFactory}.
     *
     * @return a new builder; the L2 implementation
     *     ({@link Builder#remoteCache}) is required before {@link Builder#build()}
     * @since 0.4.0
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the Reactor facade for the named cache, memoized by name.
     * The underlying {@link TierCacheFactory#getCache} and
     * {@link TierCacheFactory#asyncCache} are memoized, so all facades for
     * one name share the same cache and the same async view (whose
     * operations run on the factory's shared daemon executor).
     *
     * @param <K> key type
     * @param <V> value type
     * @param name the cache name, as configured on the builder
     * @return the memoized {@link ReactorTierCache} facade for {@code name}
     * @since 0.4.0
     */
    @SuppressWarnings("unchecked")
    public <K, V> ReactorTierCache<K, V> reactorCache(String name) {
        return (ReactorTierCache<K, V>) caches.computeIfAbsent(name,
                n -> new ReactorTierCache<>(delegate.asyncCache(n)));
    }

    /**
     * True while the L2 circuit breaker is open (L1-only degraded mode).
     *
     * @return {@code true} while the cache runs in L1-only degraded mode
     * @since 0.4.0
     */
    public boolean isDegraded() {
        return delegate.isDegraded();
    }

    /**
     * Cold flux of inbound invalidation events applied to {@code cacheName},
     * in arrival order, with the default buffer capacity and drops ignored.
     * See {@link #invalidationEvents(String, int, Consumer)}.
     *
     * @param cacheName the cache whose inbound invalidation events are
     *     observed
     * @return a cold {@code Flux} of invalidation events for
     *     {@code cacheName}
     * @since 0.4.0
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
     *
     * @param cacheName the cache whose inbound invalidation events are
     *     observed
     * @param bufferCapacity subscriber-side buffer capacity in events; must
     *     be positive
     * @param onEventDropped callback invoked with each event dropped for
     *     this subscriber because the buffer was full; must not be
     *     {@code null}
     * @return a cold {@code Flux} of invalidation events for
     *     {@code cacheName}
     * @throws NullPointerException if {@code onEventDropped} is {@code null}
     * @throws IllegalArgumentException if {@code bufferCapacity} is not
     *     positive
     * @since 0.4.0
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

    /**
     * Closes the underlying {@link TierCacheFactory}: shuts down the shared
     * executor, the transport subscriptions, and all caches handed out so
     * far. In-flight flux subscriptions terminate with the transport.
     *
     * @since 0.4.0
     */
    @Override
    public void close() {
        delegate.close();
    }

    /**
     * Builder mirroring {@link TierCacheFactory.Builder}; pass-throughs
     * delegate verbatim, only {@link #invalidationEventListener} is composed
     * with the internal fan-out.
     *
     * @since 0.4.0
     */
    public static final class Builder {

        private final TierCacheFactory.Builder delegate = TierCacheFactory.builder();
        private final InvalidationEventFanout fanout = new InvalidationEventFanout();
        private InvalidationEventListener userEventListener = InvalidationEventListener.NOOP;

        /**
         * Sets the global default settings applied to every cache without a
         * per-cache override.
         *
         * @param defaults the global default settings; must not be
         *     {@code null}
         * @return this builder
         * @see TierCacheFactory.Builder#defaults(CacheSettings)
         * @since 0.4.0
         */
        public Builder defaults(CacheSettings defaults) {
            delegate.defaults(defaults);
            return this;
        }

        /**
         * Registers a named cache with its per-cache settings override.
         *
         * @param name the cache name
         * @param override the per-cache override applied on top of the
         *     global defaults
         * @return this builder
         * @see TierCacheFactory.Builder#cache(String, CacheOverride)
         * @since 0.4.0
         */
        public Builder cache(String name, CacheOverride override) {
            delegate.cache(name, override);
            return this;
        }

        /**
         * The L2 implementation shared by all caches. Required.
         *
         * @param remoteCache the remote (L2) cache implementation; must not
         *     be {@code null}
         * @return this builder
         * @see TierCacheFactory.Builder#remoteCache(RemoteCache)
         * @since 0.4.0
         */
        public Builder remoteCache(RemoteCache<?, ?> remoteCache) {
            delegate.remoteCache(remoteCache);
            return this;
        }

        /**
         * Sets the factory creating the L1 for each named cache.
         *
         * @param factory creates the L1 for a cache name and its effective
         *     settings
         * @return this builder
         * @see TierCacheFactory.Builder#localCacheFactory(BiFunction)
         * @since 0.4.0
         */
        public Builder localCacheFactory(BiFunction<String, CacheSettings, LocalCache<?, ?>> factory) {
            delegate.localCacheFactory(factory);
            return this;
        }

        /**
         * Sets the distributed lock provider used for cluster-wide rebuild
         * coordination.
         *
         * @param lockProvider the lock provider implementation
         * @return this builder
         * @see TierCacheFactory.Builder#lockProvider(DistributedLockProvider)
         * @since 0.4.0
         */
        public Builder lockProvider(DistributedLockProvider lockProvider) {
            delegate.lockProvider(lockProvider);
            return this;
        }

        /**
         * Sets the factory creating the invalidation handler from the
         * version generator.
         *
         * @param invalidationFactory creates the invalidation handler
         * @return this builder
         * @see TierCacheFactory.Builder#invalidation(Function)
         * @since 0.4.0
         */
        public Builder invalidation(Function<VersionGenerator, InvalidationHandler> invalidationFactory) {
            delegate.invalidation(invalidationFactory);
            return this;
        }

        /**
         * Sets the listener receiving cache metrics events.
         *
         * @param listener the metrics listener
         * @return this builder
         * @see TierCacheFactory.Builder#metricsListener(CacheMetricsListener)
         * @since 0.4.0
         */
        public Builder metricsListener(CacheMetricsListener listener) {
            delegate.metricsListener(listener);
            return this;
        }

        /**
         * Sets the listener notified of circuit-breaker degradation
         * transitions.
         *
         * @param listener the degradation listener
         * @return this builder
         * @see TierCacheFactory.Builder#degradationListener(DegradationListener)
         * @since 0.4.0
         */
        public Builder degradationListener(DegradationListener listener) {
            delegate.degradationListener(listener);
            return this;
        }

        /**
         * Application-facing observer of inbound invalidation events;
         * invoked after the internal flux fan-out.
         *
         * @param listener the invalidation event listener; must not be
         *     {@code null}
         * @return this builder
         * @throws NullPointerException if {@code listener} is {@code null}
         * @see TierCacheFactory.Builder#invalidationEventListener(InvalidationEventListener)
         * @since 0.4.0
         */
        public Builder invalidationEventListener(InvalidationEventListener listener) {
            userEventListener = Objects.requireNonNull(listener, "listener");
            return this;
        }

        /**
         * Explicitly opts out of singleflight coalescing (a risk; on by
         * default). Logged as a risk by the engine.
         *
         * @return this builder
         * @see TierCacheFactory.Builder#disableSingleflight()
         * @since 0.4.0
         */
        public Builder disableSingleflight() {
            delegate.disableSingleflight();
            return this;
        }

        /**
         * Explicitly opts out of distributed rebuild coordination (a risk;
         * on by default). Logged as a risk by the engine.
         *
         * @return this builder
         * @see TierCacheFactory.Builder#disableDistributedCoordination()
         * @since 0.4.0
         */
        public Builder disableDistributedCoordination() {
            delegate.disableDistributedCoordination();
            return this;
        }

        /**
         * Explicitly opts out of the L2 circuit breaker (a risk; on by
         * default). Logged as a risk by the engine.
         *
         * @return this builder
         * @see TierCacheFactory.Builder#disableCircuitBreaker()
         * @since 0.4.0
         */
        public Builder disableCircuitBreaker() {
            delegate.disableCircuitBreaker();
            return this;
        }

        /**
         * Maximum threads serving async cache operations (the bounded async
         * executor); defaults to {@code max(4, availableProcessors)}. Under
         * saturation, submissions fail their stage with
         * {@link java.util.concurrent.RejectedExecutionException} rather
         * than growing threads.
         *
         * @param asyncExecutorThreads the thread cap; must be &gt; 0
         * @return this builder
         * @see TierCacheFactory.Builder#asyncExecutorThreads(int)
         * @since 1.2.0
         */
        public Builder asyncExecutorThreads(int asyncExecutorThreads) {
            delegate.asyncExecutorThreads(asyncExecutorThreads);
            return this;
        }

        /**
         * Builds the factory: installs the composed invalidation-event
         * listener (internal flux fan-out first, then the
         * application-facing observer) and builds the underlying
         * {@link TierCacheFactory}.
         *
         * @return the configured {@link ReactorCacheFactory}
         * @throws NullPointerException if the required L2 implementation was
         *     not provided
         * @throws io.tiercache.CacheConfigurationException if the assembled
         *     configuration is invalid; see
         *     {@link TierCacheFactory.Builder#build()}
         * @since 0.4.0
         */
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
 *
 * <p>Internal to this module; not part of the public API.
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
