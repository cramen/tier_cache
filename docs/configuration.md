# Configuration reference

This is the complete configuration surface of Tiercache. It has two layers
that map 1:1 onto each other:

- **Core** — `io.tiercache.CacheSettings` (fully resolved settings for one
  cache; every field concrete) and `io.tiercache.CacheOverride` (nullable
  per-cache overrides resolved against the global defaults).
- **Spring Boot starter** — `io.tiercache.spring.TiercacheProperties`, bound
  under the `tiercache.*` prefix with relaxed binding (kebab-case names,
  duration styles like `5m`, `PT5M`, `300s` all accepted).

Validation is fail-fast: an invalid configuration aborts startup (or
`TierCacheFactory.build()`) with an actionable error message before any cache
serves traffic.

## Inheritance model

`tiercache.defaults.*` defines the global defaults applied to every cache.
`tiercache.caches.<name>.*` overrides individual knobs for one named cache;
any field left unset inherits the defaults level. A cache that is requested
but not listed under `tiercache.caches` (for example via an annotation on a
cache name that appears nowhere in the configuration) is created on demand
with the global defaults.

Programmatic configuration mirrors this exactly: `CacheOverride` fields left
`null` inherit from the `CacheSettings` passed as factory defaults, and
`resolve()` produces the concrete per-cache settings.

## Top-level starter properties

| Property | Default | Meaning |
|---|---|---|
| `tiercache.enabled` | `false` | Master switch. The starter contributes its beans (cache manager, factory, transport) only when `true`. With `false` or absent, the starter stays out of the way. |
| `tiercache.redis-uri` | none | Redis/Valkey URI for the L2 transport, e.g. `redis://localhost:6379`. Required when `tiercache.enabled=true` unless the application provides its own `io.tiercache.spi.RemoteCache` bean (which takes precedence; cross-instance invalidation is then skipped unless the app wires it itself). |
| `tiercache.metrics.enabled` | `true` | Binds the metrics listener when a Micrometer `MeterRegistry` bean exists. `false` opts out. Note: this is a conditional property of the metrics auto-configuration, not a bound field of `TiercacheProperties`. See [observability](observability.md). |
| `tiercache.invalidation.enabled` | `true` | Wires the cross-instance invalidation engine (journal + transport) when the default Redis transport is used. `false` opts out: caches become single-node, nothing is published or subscribed. |
| `tiercache.invalidation.profile` | `pubsub` | Invalidation transport profile: `pubsub` (default) or `streams`. See [Invalidation profiles](#invalidation-profiles). |
| `tiercache.invalidation.journal-capacity` | `10000` | Approximate retained-row target per cache stream. Enabled built-in journals require at least `65`; `<=64` fails initialization before connection use. Size for event rate and recovery lag, not merely this protocol floor. |
| `tiercache.async-executor-threads` | `max(4, availableProcessors)` | Maximum threads serving `AsyncTierCache` operations (the bounded async executor). When the pool and its bounded queue (10,000) are saturated, submissions fail their `CompletionStage` with `RejectedExecutionException` rather than growing threads without bound. Raise when IO-bound async loaders starve throughput; background SWR/XFetch revalidation runs on its own small bounded pool and never competes for these threads. |

## Per-cache knobs

The table lists every `CacheSettings` record component with its starter
property name. Properties are shown relative to a level prefix — use
`tiercache.defaults.<property>` for global defaults or
`tiercache.caches.<name>.<property>` for a per-cache override.

| Starter property | Core field (`CacheSettings`) | Default | Unit | Meaning and validation |
|---|---|---|---|---|
| `l1-max-size` | `l1MaxSize` | `10000` | entries | Maximum number of entries in the in-process L1 (bounded Caffeine cache). Must be positive. |
| `l1-expire-after-write` | `l1ExpireAfterWrite` | `5m` | duration | L1 TTL since write, before jitter. Must be positive. Invariant: `<= l2-ttl` (fail-fast). |
| `l1-expire-after-access` | `l1ExpireAfterAccess` | unset (disabled) | duration | L1 TTL since last access. Unset means no access-based expiry. When set: must be positive and `<= l2-ttl`. |
| `l2-ttl` | `l2Ttl` | `1h` | duration | Logical TTL of L2 (Redis) entries; see [end-to-end staleness budgeting](sizing-and-ttl.md#l1-ttl-vs-l2-ttl). Must be positive. With a stale window configured, entries are physically stored for `l2-ttl + stale-ttl`; see [Stale window semantics](#stale-window-semantics). |
| `jitter-amplitude` | `jitterAmplitude` | `0.10` | fraction in `[0, 1)` | TTL jitter amplitude. `0.1` shortens TTLs by up to 10%. See [TTL jitter](#ttl-jitter). |
| `null-policy` | `nullPolicy` | `deny` | `deny` or `allow` | Null-caching policy. See [Null-caching policy](#null-caching-policy). |
| `null-marker-ttl` | `nullPolicy.markerTtl()` | `1m` (when policy is `allow`) | duration | TTL of cached null-markers (jittered). Must be positive and `<= l2-ttl`. Ignored under `deny`. |
| `invalidation-mode` | `invalidationMode` | `invalidate` | `invalidate` or `update` | Invalidation event mode. See [Invalidation modes](#invalidation-modes). |
| `payload-cap-bytes` | `payloadCapBytes` | `65536` (64 KiB) | bytes | Maximum payload size for `update` invalidation events; larger writes fall back to plain `invalidate` events. Minimum `1024`. |
| `stale-ttl` | `staleTtl` | `0` (disabled) | duration | Stale-while-revalidate window served from L2 past the entry TTL. Must be `>= 0`. See [Stale window semantics](#stale-window-semantics). |
| `xfetch-enabled` | `xfetchEnabled` | `false` | boolean | Probabilistic early refresh (XFetch) of fresh L2 entries. See [XFetch](#xfetch). |
| `xfetch-beta` | `xfetchBeta` | `1s` | duration | XFetch tuning factor. Must be positive when XFetch is enabled. Smaller values refresh earlier/more aggressively. |
| `degradation-stale-ttl` | `degradationStaleTtl` | `0` (disabled) | duration | Extra L1 retention window served stale while the L2 circuit breaker rejects calls (OPEN, or HALF_OPEN with no probe permit). Must be `>= 0`. See [Degradation stale window](#degradation-stale-window). |

Per-cache programmatic equivalents: `CacheOverride` exposes the settings from the table above
through nullable builder-style setters (`l1MaxSize(long)`,
`l1ExpireAfterWrite(Duration)`, …). `null` means "inherit the defaults".

## TTL ordering invariant

The configured fresh L1 duration must not exceed the configured L2 duration.
Startup validation checks this relationship for every cache (`CacheConfigValidator`):

- `l1-expire-after-write <= l2-ttl`
- `l1-expire-after-access <= l2-ttl` (when set)
- `null-marker-ttl <= l2-ttl` (under the `allow` null policy)

A violation throws `CacheConfigurationException` naming the cache, both
values, and the fix. Jitter never breaks this invariant because it only
shortens TTLs (see below), so the effective L1 TTL equals the configured
value in the worst case. This compares configured durations, not the remaining
TTL of a particular Redis entry. Warming L1 from L2 starts a new local lifetime;
it can outlast that Redis entry. Access expiry and opt-in stale retention need
separate budgeting. See [end-to-end staleness](sizing-and-ttl.md#l1-ttl-vs-l2-ttl).

## TTL jitter

`jitter-amplitude` spreads expiry times so that many entries written together
(for example at deploy time or after a mass invalidation) do not expire
together and stampede the loader (avalanche protection). Jitter only ever
shortens TTLs: the effective TTL of each write is drawn uniformly from
`[base * (1 - amplitude), base]`. It applies to L1 entry TTLs and to
null-marker TTLs. The default is `0.10`; `0` disables jitter. Range
validation: `[0, 1)`.

Jitter covers L1 only — L2 entries are shared by all instances and are
stored with the unjittered `l2-ttl` by design. Keys written together
(deploy-time warm-up, mass invalidation, a fleet restart) therefore expire
from L2 together: an L2-expiry cliff. The synchronized reload wave shows up
as tail latency on the loader path, not as errors. The mitigation is XFetch
(`xfetch-enabled` / `xfetch-beta`, off by default), which refreshes hot
entries probabilistically **before** they expire — see [XFetch](#xfetch).

## Null-caching policy

Controls what happens when the loader returns `null`:

- `deny` (default) — a loader `null` result is an uncached miss: every lookup
  of a nonexistent key reaches the loader. Use this when misses are cheap or
  keys are trustworthy.
- `allow` — a loader `null` result stores an explicit null-marker in **both**
  levels with the `null-marker-ttl` (jittered), so repeated lookups of
  nonexistent keys are absorbed by the cache instead of penetrating to the
  backing system. Markers are distinguishable from real misses at the core
  API (`LookupResult.CachedNull`) and counted by the `tiercache.null.entries`
  metric.

Spring properties: `null-policy=allow` plus `null-marker-ttl` (default `1m`
when omitted). The marker TTL obeys the TTL ordering invariant
(`null-marker-ttl <= l2-ttl`). Programmatic:
`NullPolicy.allow(Duration.ofMinutes(1))`.

## Invalidation profiles

Cross-instance invalidation is wired automatically when the default Redis
transport is used; `tiercache.invalidation.enabled=false` opts out.

- `pubsub` (default) — events are published on a Redis Pub/Sub channel.
  Lowest propagation latency; a disconnected instance misses events and
  catches up via the journal on recovery.
- `streams` — events go through Redis Streams. Select with
  `tiercache.invalidation.profile=streams`. Own pending rows are drained before
  new rows; corrupt/missing history requires safe L1 reset before ACK. Its
  retention is the same shared journal retention. See [Streams recovery](streams-recovery.md).

Both profiles share the journal (`tiercache.invalidation.journal-capacity`,
default 10000 entries per cache stream), which records recent invalidations
so a recovering instance can replay verified retained history. Unverifiable
history may require a per-cache L1 clear; see [recovery behavior](recovery.md).

## Invalidation modes

Per cache, `invalidation-mode` selects what an invalidation event carries:

- `invalidate` (default) — the event names the key; receiving instances drop
  it from L1 and re-read L2 on next access.
- `update` — the event carries the new value payload, so receivers warm L1
  directly without an L2 read. Intended for read-heavy caches. Payloads
  larger than `payload-cap-bytes` (default 64 KiB, minimum 1024) fall back to
  plain `invalidate` events automatically.

Both standard starters honor the **resolved per-cache** mode and payload cap.
An omitted `tiercache.caches.<name>.payload-cap-bytes` inherits
`tiercache.defaults.payload-cap-bytes`; an explicit override replaces it.
For example, defaults of 65536 and a `catalog` override of 8192 allow larger
UPDATE payloads in other caches while `catalog` falls back to INVALIDATE above
8192 serialized payload bytes. The cap is not a Java object-size estimate.

## Stale window semantics

`stale-ttl` (default `0` = disabled) enables stale-while-revalidate for L2
entries. It separates two TTL notions:

- **Logical TTL** = `l2-ttl`. Past it, the entry is no longer fresh.
- **Physical TTL** = `l2-ttl + stale-ttl`. The entry is stored in L2 for this
  long so it remains readable during the stale window.

An L2 hit is classified by write age:

- **fresh** (age < `l2-ttl`) — served and warmed into L1 as usual.
- **stale** (`l2-ttl` <= age < `l2-ttl + stale-ttl`) — served immediately,
  **without** warming L1 (so an in-flight refresh is never overwritten by
  the stale copy), and one asynchronous revalidation per key per instance is
  triggered through the same coordinated load path as a miss. Revalidation
  failures do not replace an already served stale result; the stale entry
  keeps serving until its window ends. A foreground hard miss joining that
  refresh receives its actual result or failure. If the refresh skips a
  busy distributed lock, foreground demand continues through ordinary
  bounded coordination/loading instead of treating the skip as a missing
  value. This transition preserves the original coordination deadline and
  existing loader-retry budget; a genuine loader null still follows the
  configured null policy.
- **past the window** — treated as a hard miss.

`stale-ttl` must be `>= 0` (fail-fast validation). Stale hits and
revalidation activity are visible via `tiercache.l2.stale.hits` and the
`tiercache.l2.revalidation.*` metrics; see
[observability](observability.md).

## XFetch

XFetch (opt-in via `xfetch-enabled`) refreshes hot entries **before** they
expire: on each fresh L2 hit it draws a refresh with probability

```
p = 1 - exp(-(age / l2-ttl) * delta / xfetch-beta)
```

where `delta` is a per-cache exponential moving average of measured loader
durations, maintained internally. Intuition: the closer the entry is to its
logical TTL and the more expensive the loader, the more likely an early
refresh; a **smaller** `xfetch-beta` makes refresh more aggressive. Before
the first measured load the probability is zero, and a lost draw costs
nothing (the hit proceeds normally). Refreshes are fire-and-forget on the
same singleflight map, so concurrent draws coalesce.

Invariants (fail-fast): `xfetch-beta` must be positive when enabled, and
XFetch requires an `l2-ttl` to measure entry age against (always present in
the core configuration model).

## Circuit breaker and degradation

The L2 circuit breaker is **on by default** and is not property-configurable;
it is a builder-level concern (`TierCacheFactory.Builder.disableCircuitBreaker()`
/ `circuitBreakerConfig(...)`, both explicit opt-ins logged as risks).

Behavior with the safe defaults: failures are counted over a sliding window
of the last 20 L2 calls; the breaker opens when failures reach 50% of the
window (after at least 5 calls). While open, the cache runs **L1-only**: no
infrastructure exceptions escape into business code, and cross-instance
atomicity (`putIfAbsent`, rebuild coordination) degrades to per-instance —
surfaced via the `tiercache.degraded=1` metric and a log line. After 5
seconds the breaker half-opens and admits up to 3 probe calls. With a recovery
handler it remains HALF_OPEN after successful probes until asynchronous
replay or a safe reset completes. Probes return their results without waiting
for replay; additional L2 calls are rejected while recovery is pending. Any
probe failure reopens the breaker. This protection concerns runtime L2 calls
with the breaker enabled. It does not suppress application loader failures,
invalid configuration, startup connection failures or async executor rejection.

On recovery, the engine attempts journal replay **before** reporting
recovery. Successful replay retains entries it does not invalidate. If the
required history cannot be verified — including trimmed rows or a failed
replay read — the affected cache's L1 is flushed. This can happen even
within the journal's capacity window and can cause a source-load burst.
The fallback emits a log, `tiercache.invalidation{direction="dropped"}` and
the `onJournalOverflow` callback; despite its name, that callback also
reports failed replay verification. A configured invalidation handler without a journal
instead logs and flushes its registered L1 caches; that path has no journal
overflow metric or callback. A factory without an invalidation handler has no
coherence-recovery hook and cannot claim journal-backed recovery.

Recovery uses two owned workers per factory, coalesced per cache, with bounded
passes and 1–30 second exponential failure retries. A failed baseline read
still clears L1 but retains the confirmed cursor and leaves recovery pending;
it cannot close the breaker. See [completion, shutdown, metrics and limits](recovery.md).

## Degradation stale window

`degradation-stale-ttl` (per cache, default `0` = disabled) is the outage
counterpart of the stale window, but for L1. When configured, L1 entries
are physically retained for `L1 TTL + degradation-stale-ttl` (the engine
tracks freshness with its own per-entry deadlines stamped at every L1
store — never the L2 write timestamp), and while the breaker **rejects** L2
calls — OPEN, or HALF_OPEN with no probe permit left — a logically expired
but retained entry is served stale **without a loader call** and counted as
`tiercache.requests{result="stale_degraded"}`. Reads that get a permit
(CLOSED, or a HALF_OPEN probe) always follow the normal path, so probes can
close the breaker. Fresh accesses with `l1-expire-after-access` configured
slide freshness and the stale horizon without ever shortening the
store-time retention floor; stale accesses never extend anything.

The authoritative deadlines live in an immutable local copy of the value or
null marker, using the same monotonic clock as Caffeine expiry. They are removed
with that entry, not by a separate ten-minute metadata expiry or fencing-map
size cap. Size eviction can still remove the entire entry. A fresh access uses
`logical = now + access TTL`, `stale until = logical + window`, and
`retention until = max(store-time retention floor, stale until)`. Retention
beyond the stale cutoff does not authorize serving the value after that cutoff.

For a custom `LocalCache`, preserve each opaque `StoredEntry` unchanged. Combining
a positive degradation window with access expiry additionally requires
`supportsAtomicReplace()` and `replaceIfSame(...)`: identity-checked replacement
of value and TTL without reinserting a removed/expired entry. Caffeine implements
this operation. An unsupported provider fails at cache creation with the cache
name and required capability; other configurations remain compatible. These
local deadlines are never serialized into Redis frames.


Trade-offs to weigh before enabling: entries live longer in L1 (memory
bounded by `window / L1 TTL x working set`, still capped by `l1-max-size`),
and the knob changes nothing in normal mode — it only serves staleness
during outages. Writes made while Redis is fully down are L1-only and are
NOT healed by journal replay: a stale L2 copy can re-warm L1 after recovery.
With write-based expiry, no access sliding or SWR, and no later stale
writes, budget its remaining L2 TTL plus one final L1 warm. During an
ongoing outage, the configured degradation window additionally permits
local stale serving. Access expiry and SWR require separate budgeting;
see [TTL sizing](sizing-and-ttl.md#l1-ttl-vs-l2-ttl). The default (off)
keeps the existing loader fallback during outages.

## Example

```yaml
tiercache:
  enabled: true
  redis-uri: redis://localhost:6379
  defaults:
    l1-max-size: 10000
    l1-expire-after-write: 5m
    l2-ttl: 1h
    jitter-amplitude: 0.10
    null-policy: deny
  caches:
    catalog:
      l2-ttl: 30m
      null-policy: allow
      null-marker-ttl: 45s
    hot:
      stale-ttl: 2m
      xfetch-enabled: true
      xfetch-beta: 500ms
  invalidation:
    enabled: true
    profile: pubsub
    journal-capacity: 10000
```

The programmatic equivalent:

```java
TierCacheFactory factory = TierCacheFactory.builder()
        .defaults(CacheSettings.defaults())
        .remoteCache(lettuceRemoteCache)
        .cache("catalog", new CacheOverride()
                .l2Ttl(Duration.ofMinutes(30))
                .nullPolicy(NullPolicy.allow(Duration.ofSeconds(45))))
        .cache("hot", new CacheOverride()
                .staleTtl(Duration.ofMinutes(2))
                .xfetchEnabled(true)
                .xfetchBeta(Duration.ofMillis(500)))
        .build();
```

`build()` runs the fail-fast validation for every configured cache.

## Compatibility

Every property and core field documented here is part of the supported
configuration surface and follows the project's semantic-versioning
commitment. See the "Compatibility and versioning" section of the
[README](../README.md) for the policy itself, and
[UPGRADING.md](../UPGRADING.md) for per-release upgrade notes.

## Micronaut binding notes

The `tiercache-micronaut` module binds the **same** `tiercache.*` keys with
the **same** defaults documented in the tables above — nothing is renamed
and nothing defaults differently. The differences are binding mechanics
only:

- `io.tiercache.micronaut.TiercacheProperties` is a Micronaut
  `@ConfigurationProperties("tiercache")` bean with immutable constructor
  binding (`@ConfigurationInject`), where the Spring starter uses setter
  binding. Kebab-case names (`l1-max-size`, ...) bind the same way.
- Per-cache overrides under `tiercache.caches.<name>.*` bind as one
  `TiercacheCacheProperties` bean per cache name (Micronaut's
  `@EachProperty` idiom) instead of a `Map` field on the root properties
  bean. The YAML/properties layout is identical to the Spring starter's —
  the [example above](#example) works unchanged in `application.yml` of a
  Micronaut application.
- `tiercache.metrics.enabled` is a conditional of the metrics factory
  (`@Requires(property = "tiercache.metrics.enabled", notEquals = "false")`),
  not a bound field — same semantics as the Spring starter.
- The defaults and invalidation levels are nullable constructor parameters
  substituted with the built-in defaults when the section is absent, and
  every per-knob field is nullable with the usual inherit-from-the-level-below
  semantics — matching the Spring binding's effective behavior.

Fail-fast validation is core's and applies identically: an invalid
combination (for example an L1 TTL above the L2 TTL) aborts application
startup with an actionable error.

## Built-in Redis namespaces (v2)

Each complete physical cache namespace is encoded as a delimiter-safe
Base64URL token: Spring `users` uses the token for `spring:users`, Micronaut
uses `micronaut:users`, and programmatic wiring uses the configured cache
name. Colons, glob syntax, empty names where supported, and valid Unicode
retain their literal identities. Malformed Unicode fails before mutation.
Data and control keys occupy separate `tiercache:v2:*` families. The logical
journal identity remains `users` in both framework examples.

There is no legacy/v2 compatibility switch. Version 2.0.0 requires
a coordinated cold cutover; an ordinary mixed-version
rolling upgrade is unsafe. See [exact layouts, clear limits, migration and
rollback](redis-keyspace-v2.md). Clear removes only data in its own namespace,
using a non-transactional scan; its journal append is a separate operation.

## Resource lifecycle

Factories own auxiliary workers and derived lock providers, while supplied L2,
clients, connections and explicit providers retain their caller ownership.
Close factories when their owning component stops. See [resource ownership and
shutdown](resource-lifecycle.md) for lazy connection cleanup, surviving synchronous
views, async cancellation and coordination limits during shutdown.


## Tags, batches and whole-cache clear

```java
TierCache<String, String> products = factory.getCache("products");
products.put("p1", "first", "category:books", "campaign:summer");
products.put("p2", "second", "category:books");
products.evictByTag("category:books");
products.evictAll(java.util.List.of("p3", "p4"));
products.evictAll();
```

With the built-in transport, an accepted versioned tagged write changes data,
tag membership and its journal record in one Redis operation. A losing write
changes none of them. Tag eviction enumerates matching keys and evicts them
individually; batch eviction also operates per key. Neither operation is a
transaction over the whole set or a fence against concurrent writes.

Whole-cache `evictAll()` clears its own data namespace using bounded SCAN work,
then separately appends EVICT_ALL to the journal. Concurrent writes/loads and
partial failures can interleave with these steps. Receivers converge through
live delivery or triggered replay of retained events, not instant synchronous
acknowledgement from every instance. During degradation, tag membership stored
only in Redis cannot provide a complete offline local tag index. See
[namespace boundaries](redis-keyspace-v2.md) and [recovery](recovery.md).

## Custom Spring Redis client and timeouts

The default client uses a 100 ms connect timeout and a 250 ms command timeout.
There are no `tiercache.*` timeout properties. A primary application-owned
`RedisClient` can customize both without replacing the `RemoteCache` wiring:

```java
@Configuration(proxyBeanMethods = false)
class CacheClientConfiguration {
    @Bean(destroyMethod = "shutdown")
    @Primary
    RedisClient applicationRedisClient(@Value("${tiercache.redis-uri}") String uri) {
        RedisClient client = RedisClient.create(uri);
        client.setOptions(ClientOptions.builder()
                .socketOptions(SocketOptions.builder()
                        .connectTimeout(Duration.ofMillis(150)).build())
                .timeoutOptions(TimeoutOptions.enabled(Duration.ofMillis(350)))
                .build());
        return client;
    }
}
```

Imports are `io.lettuce.core.{RedisClient,ClientOptions,SocketOptions,TimeoutOptions}`,
`java.time.Duration`, Spring's `context.annotation.{Configuration,Bean,Primary}`
and `beans.factory.annotation.Value`. Keep `tiercache.enabled=true` and
`tiercache.redis-uri` configured. Choose timeouts below the application's request
budget; the numbers above demonstrate customization, not universal sizing.

The primary client is used for data, lock, journal and invalidation connections.
The current auto-configuration also creates its extra default client bean: its
condition checks for a custom RemoteCache, not another RedisClient. Both clients
are closed by their owning Spring context. This wiring is exercised by
[CustomRedisClientDocumentationTest](../tiercache-spring-boot-starter/src/test/java/io/tiercache/spring/CustomRedisClientDocumentationTest.java).
It is not a claim that an equivalent custom Micronaut-client example was tested.

## Serialization and Native Image

The standard transport uses JDK serialization: keys and values must be
serializable, class evolution must remain compatible, and untrusted serialized
data must not be accepted. Redis access and payload provenance are therefore
part of the application's trust boundary.

There is no starter property that selects a serializer. Programmatic transport
builders expose key/value serializer extension points, but these transport
classes and SPIs are internal, outside the stable API listed in the README.
Manual wiring must keep L2, journal and UPDATE transport serializers compatible.
Providing only a custom `RemoteCache` skips the starter's automatic invalidation
wiring; it does not magically create a compatible journal or transport.

For GraalVM Native Image, applications using JDK serialization must register
**their own key/value DTOs** for serialization. The library's String registration
does not cover application classes. The native demo verifies its own payloads,
not every application's serializer or DTO graph.
