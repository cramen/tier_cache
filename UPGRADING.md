# Upgrading TierCache

Upgrade notes per release. Each section lists anything a consumer moving to that
version needs to know: new modules, behavior changes, and changed defaults.
Releases are listed newest first; read every section between your current and
target versions.

For the full list of additions and fixes, see [CHANGELOG.md](CHANGELOG.md).

## 1.2.0 (unreleased)

- Cross-instance write ordering fixed: versions are now time-ordered
  (wall-clock hybrid sequence) instead of per-instance counters starting at
  1. Previously, a freshly started instance's writes could lose to an older
  instance's earlier writes on the same key. The wire format and Redis
  scripts are unchanged. Rolling upgrade note: during a mixed rollout,
  writes from not-yet-upgraded instances lose to writes from upgraded ones
  (their old counter sequences always compare as older); the window closes
  once all instances run the new version. Keep instance clocks NTP-synced —
  ordering follows wall-clock time.
- Journal stream identity for framework-wired caches changed: invalidation
  journal rows are now written to the stream of the logical cache name
  (`users`) instead of the namespaced one (`spring:users` / `micronaut:users`).
  This fixes reconnect replay for Spring/Micronaut-wired caches, which
  previously read a stream that was never written (missed invalidations were
  not replayed; stale L1 entries survived until TTL). Streams written by older
  versions under the prefixed names become inert and expire harmlessly. No
  action needed; programmatic (prefix-free) wiring is unchanged.

## 1.1.0

- New module: `tiercache-micronaut` — Micronaut `CacheManager`/
  `SyncCache`/`AsyncCache` adapter, configured via the same `tiercache.*`
  keys as the Spring Boot starter. Micronaut `@Cacheable`/`@CachePut`/
  `@CacheInvalidate` code works unchanged.
- New docs: migration guides from Redisson `RLocalCachedMap` and JetCache,
  a sizing-and-TTL guide, and the "When Tiercache is not the right tool"
  README section.
- The soak gate moved out of CI to local runs
  (`./gradlew :tiercache-tck:soakTest`). No runtime change.
- No breaking changes from 1.0.0 — a drop-in upgrade.

## 1.0.0

First GA release. No breaking changes from 0.4.0 — a drop-in upgrade.

From 1.0.0 on, the project commits to semantic versioning and a formal
compatibility policy: what constitutes the public API, deprecation and removal
rules, and upgrade-note requirements. See the "Compatibility and versioning"
section of [README.md](README.md).

## 0.4.0

- New module: `tiercache-reactor` — Reactor bridge over the async view
  (`ReactorTierCache` Mono facade, `ReactorCacheFactory` with a cold
  invalidation `Flux`).
- The absolute two-level throughput budget (1M ops/s per instance) was dropped
  from the requirements. The throughput benchmarks remain as reproducible
  trend measurements without absolute ops/s budgets. No runtime change.

## 0.3.0

- New API: `AsyncTierCache`, a non-blocking `CompletionStage` view of the
  cache, obtained via `TierCacheFactory.asyncCache(name)`.
- Kotlin module: `KTierCache` suspend operations now run on the core's async
  view instead of `Dispatchers.IO`. A suspending loader therefore runs on the
  caller's coroutine dispatcher — a blocking loader blocks that dispatcher.
  Loader failures reach coalesced callers directly (unwrapped) instead of
  `CompletionException`-wrapped. Public Kotlin API is unchanged.
- Spring Boot starter: factory-level gauges (`tiercache.degraded`,
  `tiercache.breaker.state`, `tiercache.journal.size`,
  `tiercache.last.load.age`) are now registered eagerly; previously they never
  appeared in the Spring path. Dashboards that tolerated their absence now see
  real values.
- The shaded core's Gradle `.module` metadata now matches its POM (shaded jar
  as the default variant). Gradle-metadata consumers previously resolved the
  unshaded jar; after upgrading they get the shaded artifact as documented.

## 0.2.0

Multi-instance hardening release.

- **Behavior change — per-cache L2 namespacing.** Each named cache now gets
  its own Redis key prefix, fixing cross-cache key collisions and making
  per-cache `evictAll` correct. Entries written by older versions become
  unreachable after the upgrade: a one-time cold start per key (each key is
  reloaded once from the source of truth), not data corruption. Plan the
  rollout for a period that tolerates a cold cache.
- Spring Boot starter: the shared Redis client now carries the documented fast
  timeouts (100 ms connect, 250 ms command) instead of the Lettuce defaults
  (60 s command timeout). L2 outages now trip the circuit breaker quickly
  instead of hanging business requests. If your environment needs different
  values, configure them explicitly.
- New API: `TierCacheFactory.Builder.remoteCacheFactory(...)` — provide the L2
  per cache name (memoized) for full key-space isolation between named caches.
- Demo change only: the expensive-computation endpoint uses
  `@Cacheable(sync = true)` so concurrent misses coalesce through the
  value-loader path; the default `sync = false` get/put flow bypasses stampede
  protection (documented Spring Cache behavior). Apply the same pattern in
  your own `@Cacheable` code where stampede protection matters.

## 0.1.0

First public release, published to Maven Central under the `io.github.cramen`
group ID (Java packages: `io.tiercache`).

Modules:

- `tiercache-core` — two-level cascade, singleflight, distributed rebuild
  coordination, TTL jitter, null caching, degradation handling, stale
  serving (SWR/XFetch), shaded Caffeine L1, L1/L2/lock SPI.
- `tiercache-invalidation` — versioned invalidation protocol with journal
  replay, last-write-wins, Pub/Sub and Redis Streams transport profiles,
  UPDATE mode, tag-based and batch eviction.
- `tiercache-transport-redis` — Lettuce-backed Redis/Valkey L2 and lock
  provider.
- `tiercache-spring-boot-starter` — Spring Boot 3.5.x auto-configuration;
  replaces the standard cache manager, existing `@Cacheable` code unchanged.
- `tiercache-kotlin` — `KTierCache` suspend facade, invalidation `Flow`,
  `tierCache { }` configuration DSL.
- `tiercache-micrometer` — Micrometer metrics, OpenTelemetry tracing, JMX.
- `tiercache-tck` — public chaos-test suite (Testcontainers) and benchmarks.

Baseline requirements: JDK 17+, Redis 6.2+ or Valkey.
