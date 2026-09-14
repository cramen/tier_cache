/**
 * Lettuce-backed Redis/Valkey transport: the L2 {@link io.tiercache.redis.LettuceRemoteCache}
 * with per-entry TTLs, atomic {@code setIfAbsent}, tag registries, and
 * versioned last-write-wins writes; the {@link io.tiercache.redis.LettuceLockProvider}
 * for cluster-wide rebuild coordination; the bounded
 * {@link io.tiercache.redis.RedisStreamJournal}; and the two invalidation
 * transport profiles — lightweight Pub/Sub
 * ({@link io.tiercache.redis.LettucePubSubInvalidationTransport}) and durable
 * Redis Streams ({@link io.tiercache.redis.LettuceStreamsInvalidationTransport}).
 *
 * <p><b>Internal — not part of the supported API.</b> These classes are the
 * default transport implementation behind the {@code io.tiercache.spi}
 * interfaces. Applications do not use them directly: they are wired by the
 * Spring Boot starter's auto-configuration, or passed programmatically to
 * {@code TierCacheFactory.Builder.remoteCache}/{@code remoteCacheFactory}.
 * They may change in any release without notice.
 *
 * @since 0.1.0
 */
package io.tiercache.redis;
