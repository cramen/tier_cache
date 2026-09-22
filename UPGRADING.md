# Upgrading TierCache

Upgrade notes per release. Each section lists anything a consumer moving to that
version needs to know: new modules, behavior changes, and changed defaults.
Releases are listed newest first; read every section between your current and
target versions.

For the full list of additions and fixes, see [CHANGELOG.md](CHANGELOG.md).

## Unreleased: invalidation journal capacity floor

Enabled built-in journals now reject `tiercache.invalidation.journal-capacity`
values <=64, including the exact boundary 64. Configure at least 65; the confirmed
cursor row must survive alongside the next 64 delivered events. The default remains
10000. Direct RedisStreamJournal construction and Spring/Micronaut wiring validate
before journal commands or owned connection creation; disabled invalidation leaves
its unused capacity setting alone. Values are rejected, never silently clamped.

65 is only the protocol floor. Choose a larger window for bursts, disconnected
receivers and scheduled recovery delays. Redis trimming remains approximate, and
read failures or real history loss can still require a conservative L1 reset.
See [journal sizing](docs/sizing-and-ttl.md#journal-capacity).

## Unreleased: complete Spring async retrieval

Managed Spring caches now use the factory's bounded async view for both retrieve
methods introduced in Spring 6.1. Single-argument retrieval no longer blocks its
caller on L2. Supplier retrieval supports synchronized CompletableFuture and Mono
caching through existing engine coalescing, with unchanged null policy and async
shutdown/rejection semantics. Ordinary sync=false annotations gain no coalescing
promise; synchronous writes/evictions remain synchronous.

The internal two-argument TierCacheSpringCache constructor remains usable for sync
operations, but both retrieve overloads return failed futures without an async view.
Direct async users must use TierCacheManager or the new constructor accepting both
views. This changes its former blocking single-argument retrieval behavior.
See [Spring async retrieval](docs/migration-from-spring-cache.md#asynchronous-retrieval)
for wrappers, cached nulls, bounded execution and cancellation.

## Unreleased: value-bound local freshness

Degradation freshness now lives with the retained L1 entry instead of an
independently expiring metadata map. Freshness and allowed stale serving no longer
end early when a version fence expires or is evicted. Access refresh preserves
the original retention floor and cannot reinsert an entry removed by Caffeine.

Existing LocalCache methods and defaults remain compatible. Custom providers
must preserve opaque StoredEntry holders. Only providers combining a positive
`degradationStaleTtl` with `l1ExpireAfterAccess` need to implement the new default
capability and atomic identity-replacement methods; otherwise engine-cache
creation rejects that combination. Built-in Caffeine already supports it.
Redis frames and remote timestamps are unchanged. The window remains off by
default and does not heal writes performed while Redis was unavailable.

## Unreleased: lock-provider and factory shutdown

Client-backed lock providers now close the dedicated connections they create;
caller-supplied clients and connections remain caller-owned. Derived providers
are lazy and owned by their factory. Direct acquisition after provider close
fails immediately instead of accidentally reopening coordination resources.

Already-obtained synchronous caches remain usable with a usable supplied L2,
but factory close disables coordination, refresh, invalidation publication and
recovery. Continued cluster coherence is not promised. Async shutdown semantics,
constructor signatures, stored data and lease/compensation settings are unchanged.
See [resource ownership and shutdown](docs/resource-lifecycle.md).

## Unreleased: Redis keyspace v2 (breaking; major release required)

The built-in Redis/Valkey transport now uses separate v2 data, tag, journal,
channel, group and lock addresses. This fixes cross-cache deletion by clear
for hierarchical names (`user` / `user:roles`) and glob-containing names
(`a?` / `a1`). Java cache APIs and value frames are unchanged, but active
old/new instances are **not rolling-compatible**.

The published compatibility policy requires a major release for this
operational break. Do not publish it as a compatible 1.x patch/minor upgrade;
the current development snapshot is not a release-version decision.

Follow the [v2 migration guide](docs/redis-keyspace-v2.md): quiesce traffic or
source mutations, drain and stop all old requests/loaders/publishers, start
with empty v2 namespaces, then resume with capacity for cold-cache loads.
V2 never reads, copies, subscribes to or deletes legacy state. Rollback also
requires a drained cutover and clean isolated cache storage. Whole-cache
clear remains a non-transactional scan followed by a separate journal append.

## Unreleased: asynchronous invalidation recovery

Replay no longer runs under state monitors or inline in the successful probe
or reconnect callback. The breaker stays HALF_OPEN until its recovery epoch
has verified replay or a safe baseline-and-clear result; CLOSED/recovered
notifications can therefore arrive later. A failed baseline still clears L1
but keeps the confirmed cursor and recovery pending. Repeated failures retry
with bounded backoff instead of waiting for another live event.

Factories own two recovery workers and unregister pending gauges on close.
Closed coherence hooks cannot be restarted by surviving synchronous caches;
real probes may still establish caller-owned L2 availability. Existing SPI
methods remain, with additive asynchronous completion and local-clear epoch
hooks. Custom callers must await the completion stage when they require
settled recovery; returning from the old void callback is no longer that
boundary. See [the recovery contract](docs/recovery.md).

## Unreleased: Streams pending recovery

Streams now drains its own pending work before new rows and resumes only
inside the receiver's own group. Poison/missing rows require a committed
baseline-before-clear result before covered ACKs. Failed application and ACK
attempts retain the delivered batch and use bounded retries. A reader without
a capable gap handler remains pending rather than silently skipping the gap.

Default random-identity groups are retired best-effort on graceful close;
explicit stable-UUID groups persist for an **exclusive** restart. Registration
clears the fresh/resumed target against a captured baseline, so covered old
UPDATE payloads cannot warm a new L1. Do not run two live owners of one UUID.

`CheckedRange` keeps its record signature but may omit a raw-validated cursor
anchor. Consumers must use its integrity flag rather than requiring the first
event to be the cursor row. Existing custom journals may retain a valid typed
anchor. Corrupt unconsumed rows now raise sanitized typed failures. See
[Streams recovery and operator procedures](docs/streams-recovery.md).

## 1.5.0 (unreleased)

### Custom transports: versioned tagged writes

The public `TierCache.put(key, value, tags)` signature is unchanged. Custom
`RemoteCache` providers and decorators must implement both
`supportsTaggedWriteOutcomes()` (a side-effect-free, no-I/O capability query)
and `putTaggedIfNewer(...)` before accepting **versioned tagged writes**.
The existing void `putTagged(...)` method alone is no longer sufficient.
Old providers still compile and link, but core now throws an actionable
`CacheConfigurationException` before any mutation for this unsupported
operation, including when the breaker is OPEN. Unsupported capability is
not recorded as an infrastructure failure.

Return `WON` only after accepting the candidate, and `LOST` when a newer
stored value or tombstone rejects it. Couple the acceptance decision with
data, replacement tag memberships, reverse index and any configured journal
append. A losing candidate must leave all of them unchanged. Advertise the
capability only when this contract is implemented; decorators must forward
both methods. The default extension returns `UNSUPPORTED` without I/O for
versioned entries. Unversioned entries still delegate to the legacy void
method with its existing unconditional semantics.

The built-in Lettuce transport implements this in one Lua operation on
Redis/Valkey. This tagged-write correction alone does not change value frames,
key names or journal formats; the separate v2 keyspace change above does
change addresses and requires its coordinated migration.
Its existing limitation remains: without a journal, or for unversioned
entries, tagged writes are unconditional. Retagging replaces old memberships;
it does not accumulate every tag ever assigned to the key. During a mixed
rollout, old writers can still create incorrect memberships or publish a
losing candidate. Upgrade all writers; already-corrupt indexes are not
repaired automatically by the new protocol.

A confirmed loss performs at most one convergence read and never publishes
the losing value. A refused breaker probe or an admitted infrastructure
failure uses local-only fallback without publishing or persisting tags.
This does not make an unacknowledged timeout a confirmed loss: Redis may
have accepted the write before the client timed out. There is no guaranteed
rollback, exactly-once retry, or reconciliation after such an uncertain
outcome. Lua excludes interleaving commands, but does not roll back commands
already executed if a later Redis runtime error occurs.

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
