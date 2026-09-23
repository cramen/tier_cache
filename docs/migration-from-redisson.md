# Migrating from Redisson `RLocalCachedMap`

This guide covers moving a Redisson near-cache (`RLocalCachedMap` with
`LocalCachedMapOptions`) to Tiercache. Scope note up front: Redisson is a
distributed-objects toolkit; Tiercache is a cache library. Tiercache replaces
the **local-cached map** use case only. If you also use Redisson for locks,
queues, topics, or other distributed objects, keep Redisson for those — see
[What Tiercache does not replace](#honest-gaps).

Redisson API names below follow the public Redisson documentation; check the
javadoc of your exact Redisson version for details.

## API mapping

| Redisson `RLocalCachedMap` | Tiercache core (`TierCache<K, V>`) | Spring path (starter) |
|---|---|---|
| `get(key)` | `get(key)` — cascades L1 → L2, warms L1 on an L2 hit | `@Cacheable` |
| `get(key)` with read-through loader | `getOrCompute(key, loader)` — same cascade plus loader, with singleflight coalescing | `@Cacheable(sync = true)` engages the coalescing loader path |
| `put(key, value)` | `put(key, value)` — writes L2 first, then L1 | `@CachePut` |
| `put(key, value, ttl, unit)` (per-entry TTL) | No per-entry TTL. TTLs are per-cache (`l2-ttl`, `l1-expire-after-write`); see [gaps](#honest-gaps) | per-cache properties under `tiercache.caches.<name>` |
| `putIfAbsent(key, value)` → previous value | `putIfAbsent(key, value)` → `boolean` (true = stored). Return type differs: adjust call sites that read the previous value | `Cache.putIfAbsent` (Spring's SPI, returns existing wrapper) |
| `fastPut` / `fastPutIfAbsent` (async) | `AsyncTierCache` via `factory.asyncCache(name)` — `CompletionStage`-based | — |
| `remove(key)` | `evict(key)` — removes from both levels, all instances | `@CacheEvict` |
| `clear()` / `delete()` | `evictAll()` | `@CacheEvict(allEntries = true)` |
| `containsKey(key)` | `lookup(key)` — tri-state `Hit` / `CachedNull` / `Miss`; `instanceof LookupResult.Hit` ≈ `containsKey` | — |
| `getAll(keys)` / `putAll(map)` | No bulk API; issue per-key operations | — |
| Entry listeners (`EntryCreatedListener`, …) | `InvalidationEventListener` (factory builder) observes inbound invalidation events per cache; it is observational, not a mutation hook | — |

## `LocalCachedMapOptions` mapping

| `LocalCachedMapOptions` | Tiercache equivalent |
|---|---|
| `cacheSize(int)` | `l1-max-size` (property) / `CacheSettings.l1MaxSize` |
| `timeToLiveInMillis(long)` | `l2-ttl` (per cache; the freshness bound) plus `l1-expire-after-write` |
| `maxIdleInMillis(long)` | `l1-expire-after-access` (L1 only; unset = disabled) |
| `evictionPolicy(...)` | Not configurable. L1 is a bounded Caffeine cache; L2 expiry is TTL-based |
| `syncStrategy(UPDATE / INVALIDATE)` | `invalidation-mode` (`update` / `invalidate`); `payload-cap-bytes` bounds UPDATE payloads |
| `reconnectionStrategy(CLEAR / LOAD / NONE)` | No knob. Journal replay is the default — see [Reconnection semantics](#reconnection-semantics) |
| `storeMode(LOCALCACHE_REDIS_ONLY)` | No equivalent; Tiercache always stores values in both levels |
| `cacheProvider(REDISSON / CAFFEINE)` | L1 is the built-in shaded Caffeine; no selection needed |
| MapLoader / MapWriter (read-through, write-behind) | Read-through only, via `getOrCompute` / `@Cacheable`. Write-behind and write-through are explicit non-goals |

## Reconnection semantics

What Redisson does (per its public docs for `LocalCachedMapOptions.ReconnectionStrategy`):

- `CLEAR` — flushes the whole local cache on every reconnect. Safe against
  staleness, but every blip in Redis connectivity drops your L1: the next
  reads all miss and hit the loader together (stampede-prone).
- `LOAD` — replays missed invalidation events from a bounded buffer, falling
  back to a full flush when the buffer was exceeded.
- `NONE` — does nothing; local entries may go stale.

What Tiercache does: one strategy, no knob. The invalidation engine keeps a
bounded journal (`RedisStreamJournal`, capacity 10,000 entries per cache by
default, `tiercache.invalidation.journal-capacity`). On reconnect — including
circuit-breaker recovery after an L2 outage — recorded events are replayed
when the required history is readable and intact. Unverifiable history or
a replay-read failure triggers an L1 flush for the affected cache, with a
log, `onJournalOverflow` callback and
`tiercache.invalidation{direction=dropped}` metric. An engine without a
journal instead logs and flushes every registered L1. See
[`InvalidationService.onReconnect`](../tiercache-invalidation/src/main/java/io/tiercache/invalidation/InvalidationService.java).

### What changes for you operationally

- If you ran `CLEAR`: successful replay avoids an unconditional L1 flush.
  Recovery can still flush L1, including after a replay-read failure within
  the journal window. Retain capacity for recovery-time loader bursts and
  validate it under failures before reducing source headroom.
- If you ran `LOAD`: behavior is equivalent in shape — replay first, flush as
  the bounded fallback. The journal capacity is the knob you already know
  (`journal-capacity`); watch the overflow signal above.
- If you ran `NONE`: replay applies missed events that survived in the
  journal. It cannot reconstruct writes that never reached Redis or rows
  lost during Redis failover. Overall source convergence is not bounded by
  replay latency; see the [outage residual and TTL budgeting](configuration.md#degradation-stale-window).

## Feature parity

| Capability | Redisson `RLocalCachedMap` | Tiercache |
|---|---|---|
| Local (L1) + Redis (L2) read cascade | Yes | Yes; an L2 hit always warms L1 |
| Cross-instance invalidation (Pub/Sub) | Yes (`syncStrategy`) | Yes (`invalidate` / `update` modes; Pub/Sub default, Streams optional) |
| Bounded local cache | `cacheSize` | `l1-max-size` |
| TTL | Per-entry | Per-cache only |
| Reconnect healing | `reconnectionStrategy` | Journal replay by default, bounded flush fallback |
| Stampede protection (concurrent miss coalescing) | No (per-instance at best, version-dependent) | Singleflight per instance + distributed rebuild coordination cluster-wide, on by default |
| TTL jitter (avalanche protection) | No | On by default (`jitter-amplitude` 0.10) |
| Null caching (penetration protection) | `storeMode`-dependent | Opt-in `null-policy=allow` with marker TTL |
| L2 outage behavior | Exceptions propagate | Circuit breaker → L1-only mode, no infrastructure exceptions in business code; replay on recovery |
| Async API | `RLocalCachedMapAsync` (reactive variants exist) | `AsyncTierCache` (`CompletionStage`); Reactor and Kotlin coroutines modules |
| Spring Cache integration | Redisson Spring CacheManager | Starter's `CacheManager`; `@Cacheable` code unchanged |

## Honest gaps

Things Redisson gives you that Tiercache does not:

- **Per-entry TTL.** Redisson accepts a TTL per `put`; Tiercache TTLs are
  per-cache. Workaround: split entries with different lifetimes into separate
  named caches.
- **Bulk operations** (`getAll`, `putAll`). Per-key operations only.
- **Distributed objects.** `RMap`, `RSet`, `RQueue`, `RTopic`, `RBucket`, …
  are out of scope. Tiercache is a cache library only.
- **Locks as general-purpose primitives.** `RLock`, `RSemaphore`, etc.:
  Tiercache uses distributed locking internally for rebuild coordination and
  exposes no lock API. Keep Redisson for application-level locking.
- **Write-behind / write-through to a database** (MapWriter). Explicit
  non-goal.
- **Entry listeners as mutation hooks.** Tiercache's
  `InvalidationEventListener` observes inbound invalidation events; it is not
  a lifecycle callback API for entries.
- **`storeMode=LOCALCACHE_REDIS_ONLY`** (keys local, values only in Redis).
  No equivalent; values always live in both levels.
- **Local-cache-only mode.** `RLocalCachedMap` cannot degrade to serving
  without Redis in the same sense; conversely Tiercache has no "Redis down =
  hard error" mode — the breaker is on by default and can be disabled only by
  explicit programmatic opt-in.

## Before / after: programmatic

Before — Redisson:

```java
Config config = new Config();
config.useSingleServer().setAddress("redis://localhost:6379");
RedissonClient redisson = Redisson.create(config);

LocalCachedMapOptions<String, User> options = LocalCachedMapOptions.<String, User>defaults()
        .cacheSize(10_000)
        .timeToLive(Duration.ofHours(1).toMillis())
        .syncStrategy(LocalCachedMapOptions.SyncStrategy.INVALIDATE)
        .reconnectionStrategy(LocalCachedMapOptions.ReconnectionStrategy.CLEAR);

RLocalCachedMap<String, User> users = redisson.getLocalCachedMap("users", options);

User u = users.get("user:42");
if (u == null) {
    u = loadFromDatabase("user:42");   // every concurrent caller loads
    users.put("user:42", u);
}
```

After — Tiercache:

```java
try (LettuceRemoteCache<String, User> l2 = LettuceRemoteCache
        .<String, User>builder("redis://localhost:6379")
        .cacheName("users")
        .build();
     TierCacheFactory factory = TierCacheFactory.builder()
        .remoteCache(l2)
        .defaults(new CacheSettings(
                10_000, Duration.ofMinutes(5), null, Duration.ofHours(1),
                0.10, NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024))
        .build()) {

    TierCache<String, User> users = factory.getCache("users");
    User u = users.getOrCompute("user:42", this::loadFromDatabase);
}
```

Notes on the translation:

- The cache is write-through to L2 by construction; the get-then-put race
  from the Redisson snippet is replaced by `getOrCompute`, which coalesces
  concurrent misses onto one loader execution (per instance; cluster-wide
  once the invalidation engine is wired via the factory's
  `.invalidation(...)` builder hook).
- `reconnectionStrategy` disappears: journal replay is the default when the
  invalidation engine is wired with a journal.
- Default TTLs differ from Redisson's; set `l1-expire-after-write` and
  `l2-ttl` explicitly to match your current options. Startup validation fails
  fast if `l1-expire-after-write > l2-ttl`.

## Before / after: Spring Boot

Before — Redisson injected as a map:

```java
@Service
class UserService {
    private final RLocalCachedMap<String, User> users;

    UserService(RedissonClient redisson) {
        this.users = redisson.getLocalCachedMap("users",
                LocalCachedMapOptions.defaults());
    }

    User find(String id) {
        User u = users.get(id);
        if (u == null) {
            u = repository.load(id);
            users.put(id, u);
        }
        return u;
    }
}
```

After — starter plus annotation:

```yaml
tiercache:
  enabled: true
  redis-uri: redis://localhost:6379
  caches:
    users:
      l1-max-size: 10000
      l1-expire-after-write: 5m
      l2-ttl: 1h
```

```java
@Service
class UserService {
    @Cacheable(cacheNames = "users", sync = true)
    public User find(String id) {
        return repository.load(id);
    }
}
```

The manual get-then-put, the Redisson client wiring, and the options object
are gone. `sync = true` routes misses through the coalescing loader path —
without it, concurrent misses each invoke the method (standard Spring Cache
semantics; see
[migration from Spring Cache](migration-from-spring-cache.md#stampede-protection-and-sync--true)).

## Verifying the migration

1. Startup is fail-fast: a missing `redis-uri` or an L1 TTL above the L2 TTL
   aborts boot with an actionable error. A clean start validates the config.
2. Exercise a hot read twice; the second call is served from L1
   (`tiercache.requests{result="l1_hit"}` if you bind the metrics module —
   see [observability](observability.md)).
3. Restart Redis briefly: expect `tiercache.degraded=1` while it is down.
   On recovery, missed journal rows produce `direction="replayed"`; a
   journal-backed flush produces `direction="dropped"`. Inspect logs for
   unavailable history or replay-read failures, and measure the resulting
   source load rather than assuming recovery never flushes L1.
