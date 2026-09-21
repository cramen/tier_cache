# Upgrading TierCache

Upgrade notes per release. Each section lists anything a consumer moving to that
version needs to know: new modules, behavior changes, and changed defaults.
Releases are listed newest first; read every section between your current and
target versions.

For the full list of additions and fixes, see [CHANGELOG.md](CHANGELOG.md).

## 1.4.0

- New opt-in knob `tiercache.degradation-stale-ttl` (per cache, default `0`
  = unchanged): serves physically retained L1 entries stale while the L2
  breaker rejects calls, instead of every expired key falling to the loader
  during an outage. Off by default — existing behavior is identical unless
  you set it. `CacheSettings` gained a record component with a compatible
  old-arity constructor; note the documented residual for full-outage
  writes (no replay healing; the stale L2 copy can re-warm L1 until its own
  TTL) and the expire-after-access refinement that applies only when the
  knob is on.

## 1.3.0

- Replay cursor protocol replaced (bug fix, no API change): the replay
  position now advances only over confirmed-applied contiguous journal
  rows, every cursor read carries an atomic integrity proof, and trim
  detection for beginning cursors uses a write-time counter. Operational
  constraint to check: the journal capacity must comfortably exceed the
  cursor cadence (64 events) — with a smaller journal, prefix integrity is
  unconfirmable on every cadence tick and the service takes the L1 flush
  path BY DESIGN (previously such journals silently worked most of the
  time but could skip events). Size journals per docs/sizing-and-ttl.md.
  Mixed rollout: not-yet-upgraded writers trim the journal without
  incrementing the trim counter, so the exact beginning-cursor guarantee
  holds once ALL writers run the new version; until then the pre-existing
  uncertainty applies (failure direction never invents a loss for
  non-zero cursors). No state migration needed: the counter key appears
  on the first trim.
- Tag index boundedness scheme changed (bug fix, no API change). Each tag
  set's TTL is now extend-only (a shorter-lived entry never shrinks the
  index) and member cleanup is atomic. Transition: sets written by older
  versions keep their old TTL until the next write lifts it; sets that
  never get another write expire on the old TTL. Already-shrunken set TTLs
  from the old scheme do not self-repair — that state must either expire
  on its own or be corrected deliberately. Safe corrections: (a) re-`put`
  the live tagged entries under the new version to rebuild membership —
  deleting the `tiercache:tags:*` / `tiercache:tagkeys:*` index keys alone
  is NOT safe (live data would lose its tag membership and `evictByTag`
  would stop finding it); or (b) coherently clear the affected cache
  entirely (data entries, the tag indexes, and a local-cache flush on all
  instances). During a mixed rollout, not-yet-upgraded instances can
  transiently shrink set TTLs or skip member cleanup; boundedness is exact
  once all writers run the new version AND the pre-existing shrunken state
  has expired or been corrected.

## 1.2.0

- Async executor is now bounded: `AsyncTierCache` operations previously ran
  on an unbounded cached thread pool (thread-per-task growth under
  concurrency). The pool is now fixed-size (default
  `max(4, availableProcessors)`, knob `tiercache.async-executor-threads` /
  `TierCacheFactory.Builder.asyncExecutorThreads`) with a bounded queue.
  Behavior change under saturation: submissions that exceed pool + queue
  capacity now fail their `CompletionStage` with
  `RejectedExecutionException` instead of queueing onto fresh threads.
  Tune the knob if you run high-concurrency IO-bound async loaders.
  Background SWR/XFetch revalidation moved to its own small bounded pool.
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
