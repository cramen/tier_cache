# Tiercache

A two-level cache for the JVM: L1 in-process (Caffeine) + L2 Redis/Valkey, with cross-instance invalidation, built-in protection against stampede/avalanche/penetration/L2 degradation, and first-class observability.

Status: early development (0.x, incubating API). See `AGENTS.md` for the design principles, module structure, and roadmap.

## Quick start

Build:

```bash
./gradlew build
```

Minimal usage (`tiercache-core` + `tiercache-transport-redis`):

```java
import io.tiercache.TierCache;
import io.tiercache.TierCacheFactory;
import io.tiercache.redis.LettuceRemoteCache;

try (LettuceRemoteCache<String, String> l2 = LettuceRemoteCache
        .<String, String>builder("redis://localhost:6379")
        .cacheName("users")
        .build()) {

    TierCache<String, String> cache = TierCacheFactory.builder()
            .remoteCache(l2)
            .build()
            .getCache("users");

    String name = cache.getOrCompute("user:42", key -> loadFromDatabase(key));
}
```

Reads cascade L1 → L2 → loader; an L2 hit always warms L1. Concurrent loads of the same key share one loader execution — per instance (singleflight) and cluster-wide via a distributed rebuild lock with watchdog lease extension and mandatory double-check, on by default. Invalid configuration (e.g. L1 TTL > L2 TTL) fails fast at startup. Atomic `putIfAbsent` is backed by the L2 `SET NX PX` primitive. Null caching is available per cache via `NullPolicy.allow(ttl)` — repeated lookups of nonexistent keys are absorbed by an explicit marker; use `lookup(key)` to distinguish miss / cached-null / hit.

When Redis fails, a built-in circuit breaker switches the cache to L1-only mode: business operations never see infrastructure exceptions. The degraded mode is honest — cross-instance atomicity narrows to per-instance, and every transition is logged and signaled. Recovery replays the missed invalidation journal and never flushes L1 on reconnect (no healing-partition stampede).

Every failure mode has a metric: request outcomes per level, L2 latency, invalidation flow (sent/received/replayed/dropped), journal size, degraded state, breaker state, entry age, null entries — via Micrometer (`tiercache-micrometer` module; the Spring Boot starter binds to your MeterRegistry automatically). OpenTelemetry spans cover L2 operations and invalidation processing. A reference Grafana dashboard and alert rules live in `docs/grafana/`.

The cache is eventually consistent by design; no strong-consistency guarantees are given or implied.

## Spring Boot quick start

```java
// application.yml
tiercache:
  enabled: true
  redis-uri: redis://localhost:6379
```

With `tiercache-spring-boot-starter` on the classpath this replaces the standard cache manager: `@Cacheable/@CachePut/@CacheEvict` code works unchanged, backed by the two-level cache (migration from `RedisCacheManager` = swap the starter + these two lines). Per-cache overrides live under `tiercache.caches.<name>.*`; invalid configuration (e.g. L1 TTL > L2 TTL) aborts startup with an actionable error.

A runnable demo lives in `examples/demo-spring` (docker-compose included).

## License

[Apache License 2.0](LICENSE)
