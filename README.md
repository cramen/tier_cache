# Tiercache

[![CI](https://github.com/cramen/tier_cache/actions/workflows/ci.yml/badge.svg)](https://github.com/cramen/tier_cache/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

**Tiercache** is a family of JVM libraries providing a correct-out-of-the-box **two-level cache**: L1 in-process (Caffeine) + L2 Redis/Valkey, with cross-instance invalidation, built-in protection against the classic high-load failure modes (stampede, avalanche, penetration, L2 degradation), and first-class observability. A framework-independent core with thin adapters — pull in exactly one starter module and keep your existing cache code.

The cache is eventually consistent by design; no strong-consistency guarantees are given or implied.

## Features

- **Two-level read cascade** — L1 (shaded Caffeine, zero-allocation hit path) → L2 (Redis/Valkey) → your loader. An L2 hit always warms L1, so the next read of the same key is served in-process.
- **Correct by default** — singleflight per instance plus cluster-wide rebuild coordination (distributed lock with watchdog lease extension and mandatory double-check), TTL jitter, fail-fast TTL-ordering validation, atomic `putIfAbsent` — all on without configuration; disabling requires an explicit opt-in and is logged as a risk. Null caching is opt-in (`null-policy: allow`) with a tri-state `lookup` to distinguish miss from cached-null.
- **Cross-instance invalidation** — versioned events with last-write-wins ordering, a bounded journal with replay on reconnect, and two transport profiles: lightweight Pub/Sub or durable Redis Streams.
- **Honest degradation** — a circuit breaker switches the cache to L1-only when Redis fails; business code never sees infrastructure exceptions. Recovery replays the missed invalidations from a bounded journal and does not flush L1 within the journal window (no healing-partition stampede). If a disconnect outlives the journal window — or a hand-built engine has no journal — L1 is flushed for the affected caches, signalled via log, the `tiercache.invalidation{direction="dropped"}` metric, and a listener callback.
- **Stale serving** — stale-while-revalidate and XFetch early refresh keep hot keys fast while values refresh in the background.
- **Observability as a feature** — Micrometer metrics for every failure mode, OpenTelemetry tracing, JMX inspection, and a reference Grafana dashboard with alert rules in [`docs/grafana/`](docs/grafana/).
- **Kotlin coroutines** — `suspend` API, invalidation `Flow`, and a `tierCache { }` config DSL in `tiercache-kotlin`. A suspending loader runs on the caller's coroutine dispatcher, so a blocking loader blocks that dispatcher — offload blocking work with `withContext(Dispatchers.IO)`.
- **Bounded async executor** — async/Reactor/coroutines operations run on a bounded, library-managed pool (size via `tiercache.async-executor-threads` or `TierCacheFactory.Builder.asyncExecutorThreads`); under saturation the returned stage fails with `RejectedExecutionException` instead of growing threads without bound.
- **GraalVM Native Image** — reachability metadata ships inside the published jars; the demo application compiles natively in CI.

## Quick start

### Spring Boot

```kotlin
// build.gradle.kts
implementation("io.github.cramen:tiercache-spring-boot-starter:1.1.0")
```

```yaml
# application.yml
tiercache:
  enabled: true
  redis-uri: redis://localhost:6379
```

The starter replaces the standard cache manager: `@Cacheable` / `@CachePut` / `@CacheEvict` code works unchanged, backed by the two-level cache. Load coalescing under `@Cacheable` requires `sync = true` — Spring's abstraction only coordinates concurrent loads in sync mode (see [migration from Spring Cache](docs/migration-from-spring-cache.md)). Null caching is opt-in (`tiercache.caches.<name>.null-policy: allow`). Per-cache overrides live under `tiercache.caches.<name>.*`; invalid configuration (e.g. L1 TTL > L2 TTL) aborts startup with an actionable error. A runnable demo lives in [`examples/demo-spring`](examples/demo-spring/) (docker-compose included).

### Micronaut

```kotlin
// build.gradle.kts
implementation("io.github.cramen:tiercache-micronaut:1.1.0")
```

```yaml
# application.yml
tiercache:
  enabled: true
  redis-uri: redis://localhost:6379
```

The module replaces Micronaut's `DefaultCacheManager`: `@Cacheable` / `@CachePut` / `@CacheInvalidate` code works unchanged, backed by the two-level cache. The `tiercache.*` property keys and defaults are identical to the Spring Boot starter's, and per-cache overrides live under `tiercache.caches.<name>.*` — [`docs/configuration.md`](docs/configuration.md) is the single configuration reference. The Pub/Sub invalidation profile is wired by default; each named cache gets its own L2 namespace (`micronaut:<cache-name>`).

### Programmatic

```kotlin
// build.gradle.kts
implementation("io.github.cramen:tiercache-core:1.1.0")
implementation("io.github.cramen:tiercache-transport-redis:1.1.0")
```

```java
import io.tiercache.TierCache;
import io.tiercache.TierCacheFactory;
import io.tiercache.redis.LettuceRemoteCache;

try (LettuceRemoteCache<String, String> l2 = LettuceRemoteCache
        .<String, String>builder("redis://localhost:6379")
        .cacheName("users")
        .build();
     TierCacheFactory factory = TierCacheFactory.builder()
        .remoteCache(l2)
        .build()) {

    TierCache<String, String> cache = factory.getCache("users");
    String name = cache.getOrCompute("user:42", key -> loadFromDatabase(key));
}
```

Reads cascade L1 → L2 → loader; concurrent loads of the same key share one loader execution, per instance and cluster-wide. All protections (singleflight, rebuild coordination, TTL jitter, circuit breaker) are active in this snippet with no extra configuration.

## Modules

| Module | What it gives you |
|---|---|
| `tiercache-spring-boot-starter` | Spring Boot 3 auto-configuration — the one dependency most Spring apps need |
| `tiercache-micronaut` | Micronaut CacheManager/SyncCache/AsyncCache adapter — the one dependency Micronaut apps need |
| `tiercache-core` | The framework-independent cache engine: cascade, singleflight, rebuild coordination, degradation |
| `tiercache-transport-redis` | Lettuce-backed Redis/Valkey L2 and invalidation transport |
| `tiercache-kotlin` | Coroutines API: suspend facade, invalidation Flow, config DSL |
| `tiercache-micrometer` | Micrometer metrics, OpenTelemetry tracing, JMX inspection |
| `tiercache-invalidation` | Invalidation protocol internals (pulled in transitively by the transport) |
| `tiercache-tck` | Public chaos-test suite and benchmarks |

## Requirements

- Java 17 or newer
- Redis 6.2+ or Valkey (for L2 and cross-instance features)
- Docker, to run the integration tests and TCK chaos suite locally

## Compatibility and versioning

All published Maven artifacts follow **semantic versioning**: patch releases for backwards-compatible fixes, minor releases for backwards-compatible additions, major releases for breaking changes.

**Supported public API** — the documented entry points only:

- `io.tiercache` core API: `TierCache`, `AsyncTierCache`, `TierCacheFactory`, `CacheSettings`, `CacheOverride`
- Spring Boot starter properties (`tiercache.*`) and annotations
- Micronaut `tiercache.*` properties (`tiercache-micronaut`)
- Kotlin `suspend`/`Flow` extensions in `tiercache-kotlin`
- Reactor `Mono`/`Flux` facade in `tiercache-reactor`

Everything else — builders, transport internals, metrics helpers, and any type not listed above — is internal and may change in any release without notice. Minor and patch upgrades never break consumers who use only the supported API.

**Deprecation and removal** — before any public API element is removed, it is marked `@Deprecated` with a documented replacement and stays functional for at least one minor release. Removals are recorded in [CHANGELOG.md](CHANGELOG.md) and `UPGRADING.md`, which describes the migration path per release.

## When Tiercache is not the right tool

- **Strong-consistency requirements.** Tiercache is eventually consistent by design: L1 copies on other instances lag writes by a bounded staleness window. If your use case needs read-your-writes across instances, use the database or a strongly consistent store directly.
- **Single-instance deployments.** With one node there is nothing to invalidate and nothing to coordinate — L2 plus the invalidation protocol is pure overhead. Plain Caffeine is the better fit.
- **Tiny working sets.** If your hot data fits comfortably in an in-process cache, an L2 round trip on every cold read costs more than it saves; L2 pays off when L1 misses are frequent or expensive.
- **Write-mostly workloads.** A cache amortizes reads over writes. If writes dominate, invalidation churn and L2 write traffic add latency without the read hit rate to justify it.

## Documentation

- [Configuration reference](docs/configuration.md)
- [Migration from Spring Cache](docs/migration-from-spring-cache.md)
- [Migration from Redisson](docs/migration-from-redisson.md)
- [Migration from JetCache](docs/migration-from-jetcache.md)
- [Sizing and TTL guidance](docs/sizing-and-ttl.md)
- [Observability: metrics, tracing, dashboards](docs/observability.md)
- [Running the TCK chaos suite](docs/tck.md)
- [Grafana dashboard and alert rules](docs/grafana/)
- [Security policy and supply-chain verification](SECURITY.md)
- [Demo application](examples/demo-spring/)

## License

[Apache License 2.0](LICENSE)
