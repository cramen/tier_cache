# Migrating from Spring Cache

Migration from standard Spring Cache is a dependency swap plus one config
line. Your `@Cacheable` / `@CachePut` / `@CacheEvict` code does not change.

## 1. Swap the dependency

Remove your current cache provider starter (for example
`spring-boot-starter-cache` with a Caffeine or Redis provider) and add the
Tiercache starter:

```kotlin
// Gradle (Kotlin DSL)
implementation("io.tiercache:tiercache-spring-boot-starter:<version>")
```

```xml
<!-- Maven -->
<dependency>
    <groupId>io.tiercache</groupId>
    <artifactId>tiercache-spring-boot-starter</artifactId>
    <version>&lt;version&gt;</version>
</dependency>
```

The starter brings Spring's cache abstraction with it, so annotations keep
resolving.

## 2. Add the config line

```yaml
tiercache:
  enabled: true
  redis-uri: redis://localhost:6379
```

`tiercache.enabled=true` is the switch; `redis-uri` points at the
Redis/Valkey instance used as L2 (required with the default transport unless
you provide your own `RemoteCache` bean). A missing URI fails startup with an
error naming the property
(`TiercacheAutoConfigurationTest.missingRedisUriFailsWithActionableError`).
This is exactly the setup in the demo application
(`examples/demo-spring/src/main/resources/application.yml`).

## 3. Keep your annotations unchanged

The starter implements Spring's Cache SPI:

- `io.tiercache.spring.TierCacheManager` extends
  `org.springframework.cache.support.AbstractCacheManager` and is
  registered as the `CacheManager` bean (a user-provided `CacheManager`
  takes precedence).
- `io.tiercache.spring.TierCacheSpringCache` extends
  `AbstractValueAdaptingCache`, so Spring's null-value conventions work as
  with Spring's own cache managers.

Because it is a plain Spring `CacheManager`, `@EnableCaching`, `@Cacheable`,
`@CachePut`, `@CacheEvict`, SpEL keys, and cache names all behave as before.
The migration gate test proves the swap is configuration-only: the **same**
annotation-driven service class is exercised once under Spring's
`ConcurrentMapCacheManager` and once under `TierCacheManager`, with identical
results —
[`SpringCacheMigrationTest`](../tiercache-spring-boot-starter/src/test/java/io/tiercache/spring/SpringCacheMigrationTest.java).

## What improves

| Behavior | What you get | Proof |
|---|---|---|
| L1 warm on L2 hit | A local miss that hits Redis warms the in-process L1, so subsequent reads are served in-process. `CompositeCacheManager` never did this; here `Cache.retrieve(...)` implements the honest L1 → L2 → empty cascade by construction. | Core: `DefaultTierCacheTest.l2HitWarmsL1` (`tiercache-core/src/test/java/io/tiercache/DefaultTierCacheTest.java`). Adapter: `TierCacheSpringCacheTest.retrieveIsMultilevel`. |
| Stampede protection | Concurrent misses of one key coalesce: `get(key, loader)` under annotations delegates to the core's singleflight path, so one loader execution serves all waiters — per instance, and cluster-wide via distributed rebuild coordination when the Redis transport is present. | [`TierCacheSpringCacheTest.concurrentValueLoadersCoalesce`](../tiercache-spring-boot-starter/src/test/java/io/tiercache/spring/TierCacheSpringCacheTest.java) (16 threads, 1 loader call). |
| Null caching | Opt-in `allow` policy caches null results as markers in both levels (penetration defense); Spring's `ConcurrentMapCache` does not cache nulls. | `TierCacheSpringCacheTest.nullResultMapsToMarkerUnderAllow` / `nullResultIsSkippedUnderDeny`. |
| Cross-instance invalidation | Writes and evictions propagate to other instances (Pub/Sub by default, Streams optional); missed events are replayed from a journal on recovery. Wired automatically with the Redis transport. | [`InvalidationAutoConfigurationTest`](../tiercache-spring-boot-starter/src/test/java/io/tiercache/spring/InvalidationAutoConfigurationTest.java). |
| Degradation | An L2 circuit breaker (on by default) drops the cache to L1-only mode when Redis fails — no infrastructure exceptions escape into business code — and recovers automatically. | Core: `DegradationTest` (`tiercache-core/src/test/java/io/tiercache/DegradationTest.java`); see [configuration](configuration.md#circuit-breaker-and-degradation). |
| Fail-fast config validation | Invalid configuration (e.g. L1 TTL larger than L2 TTL) aborts startup with an actionable message instead of silently serving stale data. | `TiercacheAutoConfigurationTest.invalidTtlOrderingAbortsStartup`. |
| Atomic `putIfAbsent` | Cross-instance atomicity via L2 while the circuit breaker is closed. | `TierCacheSpringCacheTest.putIfAbsentReturnsExistingForLoser`. |

Everything above is on by default; protections are disabled only via
explicit programmatic opt-ins, which are logged as risks. See
[configuration](configuration.md) for the full knob reference.

## What changes semantically

- **Eventual consistency.** L1 copies on other instances are refreshed by
  invalidation events, not synchronously. There is a bounded staleness
  window between a write on one instance and visibility on others (bounded
  by propagation latency plus the journal replay path on reconnect). If your
  code relied on read-your-writes across instances with a shared Redis
  cache, that assumption no longer holds — as with any near-cache design.
- **Null results under the `allow` policy.** If you enable `null-policy:
  allow` for a cache, a method returning `null` is cached as a marker for
  `null-marker-ttl`; subsequent calls do not invoke the method until the
  marker expires. Under the default `deny` policy nothing changes versus
  `ConcurrentMapCache`: nulls are not cached.
- **Loader coalescing.** With singleflight, concurrent callers for the same
  missing key share one loader execution. Loader side effects therefore run
  once per rebuild round, not once per caller — which is the point, but
  matters if your loader performed per-call bookkeeping.
- **Cache names not in configuration.** A name absent from
  `tiercache.caches` is created on demand with the global defaults (same as
  Spring's dynamic cache managers). Configure hot caches explicitly to give
  them their own TTLs and policies.

## Verifying the migration

1. Start the app; startup fails fast on missing `redis-uri` or invalid TTL
   ordering, so a clean start already validates the configuration.
2. Hit a `@Cacheable` method twice: the second call is served from L1. The
   demo (`examples/demo-spring`) exposes this via `/greeting/{name}`.
3. Optionally add the metrics module and check `tiercache.requests` — see
   [observability](observability.md).
