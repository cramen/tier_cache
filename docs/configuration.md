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
| `tiercache.invalidation.journal-capacity` | `10000` | Maximum journal entries kept per cache stream. The journal backs replay of missed invalidations after reconnects. |

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
| `l2-ttl` | `l2Ttl` | `1h` | duration | Logical TTL of L2 (Redis) entries — the freshness bound. Must be positive. With a stale window configured, entries are physically stored for `l2-ttl + stale-ttl`; see [Stale window semantics](#stale-window-semantics). |
| `jitter-amplitude` | `jitterAmplitude` | `0.10` | fraction in `[0, 1)` | TTL jitter amplitude. `0.1` shortens TTLs by up to 10%. See [TTL jitter](#ttl-jitter). |
| `null-policy` | `nullPolicy` | `deny` | `deny` or `allow` | Null-caching policy. See [Null-caching policy](#null-caching-policy). |
| `null-marker-ttl` | `nullPolicy.markerTtl()` | `1m` (when policy is `allow`) | duration | TTL of cached null-markers (jittered). Must be positive and `<= l2-ttl`. Ignored under `deny`. |
| `invalidation-mode` | `invalidationMode` | `invalidate` | `invalidate` or `update` | Invalidation event mode. See [Invalidation modes](#invalidation-modes). |
| `payload-cap-bytes` | `payloadCapBytes` | `65536` (64 KiB) | bytes | Maximum payload size for `update` invalidation events; larger writes fall back to plain `invalidate` events. Minimum `1024`. |
| `stale-ttl` | `staleTtl` | `0` (disabled) | duration | Stale-while-revalidate window served from L2 past the entry TTL. Must be `>= 0`. See [Stale window semantics](#stale-window-semantics). |
| `xfetch-enabled` | `xfetchEnabled` | `false` | boolean | Probabilistic early refresh (XFetch) of fresh L2 entries. See [XFetch](#xfetch). |
| `xfetch-beta` | `xfetchBeta` | `1s` | duration | XFetch tuning factor. Must be positive when XFetch is enabled. Smaller values refresh earlier/more aggressively. |

Per-cache programmatic equivalents: `CacheOverride` exposes the same eleven
knobs as nullable builder-style setters (`l1MaxSize(long)`,
`l1ExpireAfterWrite(Duration)`, …). `null` means "inherit the defaults".

## TTL ordering invariant

An L1 entry must never outlive its L2 counterpart, otherwise L1 could serve
data after the L2 entry expired. Enforced at startup for every cache
(`CacheConfigValidator`):

- `l1-expire-after-write <= l2-ttl`
- `l1-expire-after-access <= l2-ttl` (when set)
- `null-marker-ttl <= l2-ttl` (under the `allow` null policy)

A violation throws `CacheConfigurationException` naming the cache, both
values, and the fix. Jitter never breaks this invariant because it only
shortens TTLs (see below), so the effective L1 TTL equals the configured
value in the worst case.

## TTL jitter

`jitter-amplitude` spreads expiry times so that many entries written together
(for example at deploy time or after a mass invalidation) do not expire
together and stampede the loader (avalanche protection). Jitter only ever
shortens TTLs: the effective TTL of each write is drawn uniformly from
`[base * (1 - amplitude), base]`. It applies to L1 entry TTLs and to
null-marker TTLs. The default is `0.10`; `0` disables jitter. Range
validation: `[0, 1)`.

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
  `tiercache.invalidation.profile=streams`.

Both profiles share the journal (`tiercache.invalidation.journal-capacity`,
default 10000 entries per cache stream), which records recent invalidations
so a recovering instance replays what it missed instead of flushing L1.

## Invalidation modes

Per cache, `invalidation-mode` selects what an invalidation event carries:

- `invalidate` (default) — the event names the key; receiving instances drop
  it from L1 and re-read L2 on next access.
- `update` — the event carries the new value payload, so receivers warm L1
  directly without an L2 read. Intended for read-heavy caches. Payloads
  larger than `payload-cap-bytes` (default 64 KiB, minimum 1024) fall back to
  plain `invalidate` events automatically.

Note: with the starter, the Redis transport's UPDATE-mode payload cap is
taken from the **defaults level** (`tiercache.defaults.payload-cap-bytes`),
even when the mode is overridden per cache.

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
  failures never reach readers; the stale entry keeps serving until its
  window ends.
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
seconds the breaker half-opens and admits up to 3 probe calls; it closes
when all probes succeed and reopens on any probe failure.

On recovery, missed invalidations are replayed from the journal **before**
recovery is reported; L1 is never flushed on reconnect. (The starter always
wires the journal; only a hand-built invalidation engine without a journal
falls back to a full flush.)

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
