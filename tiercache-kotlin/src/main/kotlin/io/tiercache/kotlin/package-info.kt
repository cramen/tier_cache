/**
 * Kotlin coroutines API for Tiercache: [io.tiercache.kotlin.KTierCache] suspend facade over the
 * blocking `io.tiercache.TierCache`, invalidation events as a cold [kotlinx.coroutines.flow.Flow]
 * via [io.tiercache.kotlin.KTierCacheFactory.invalidationEvents], and the
 * [io.tiercache.kotlin.tierCache] configuration DSL.
 *
 * @since 0.1.0
 */
package io.tiercache.kotlin
