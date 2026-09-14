/**
 * The invalidation engine: {@link io.tiercache.invalidation.InvalidationService}
 * publishes local writes, applies inbound events to registered caches
 * (last-write-wins), and heals missed events after a transport reconnect by
 * replaying the journal; {@link io.tiercache.invalidation.MessageCodec} is the
 * binary wire codec the invalidation transports use.
 *
 * <p><b>Internal — not part of the supported API.</b> These classes are the
 * default invalidation engine behind the {@code io.tiercache.spi} interfaces;
 * they are wired through {@code TierCacheFactory.Builder.invalidation(...)}
 * and may change in any release without notice.
 *
 * @since 0.1.0
 */
package io.tiercache.invalidation;
