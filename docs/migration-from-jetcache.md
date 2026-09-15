# Migrating from JetCache

Migration from JetCache (`com.alicp.jetcache`) to Tiercache is a dependency
swap, an annotation swap, and a move of cache parameters from annotation
attributes into configuration. Cache method bodies do not change.

## Why migrate

JetCache is effectively frozen: the upstream project shows no active
maintenance (the last releases date to the 2.7.x line), and known gaps —
limited observability, no first-class degradation handling — will not be
addressed. Tiercache covers the same ground (in-process + Redis two-level
cascade behind annotations) and adds the failure-mode protections,
fail-fast config validation, and metrics a two-level cache needs in
production. See [README](../README.md) for the feature list.

## Annotation mapping

JetCache annotations come from `com.alicp.jetcache.anno.*`. Tiercache
exposes no annotations of its own — it implements Spring's Cache SPI, so
you use standard Spring Cache annotations
(`org.springframework.cache.annotation.*`) with
`tiercache-spring-boot-starter`. If your project is not Spring Boot, use
the programmatic API instead (see [README](../README.md)).

| JetCache | Spring Cache + Tiercache | Notes |
|---|---|---|
| `@Cached(name, key, expire, timeUnit, cacheType=BOTH)` | `@Cacheable(cacheNames, key, sync = true)` | `expire`/`timeUnit` move out of the annotation into configuration (`l1-expire-after-write` / `l2-ttl` per cache). `cacheType=BOTH` is the only model Tiercache implements. `sync = true` routes misses through the coalescing loader path — see [Stampede protection and `sync = true`](migration-from-spring-cache.md#stampede-protection-and-sync--true). |
| `@CacheInvalidate(name, key)` | `@CacheEvict(cacheNames, key)` | Direct equivalent. |
| `@CacheUpdate(name, key, value)` | `@CachePut(cacheNames, key)` | Return the value from the method instead of pointing the annotation at an argument. |
| `@CachePenetrationProtect` | Nothing to write | Stampede protection is on by default and not annotation-gated: concurrent misses of one key coalesce per instance (singleflight) and cluster-wide (distributed rebuild coordination with the Redis transport). Cache-penetration defense against *null* results is the `null-policy: allow` knob (JetCache's `cacheNullValue` equivalent), configured per cache. |
| `@CacheRefresh(refresh, ...)` | `stale-ttl` + `xfetch-enabled` per cache | Periodic reload has no annotation equivalent; use the stale-while-revalidate window and XFetch early refresh — see [configuration](configuration.md#stale-window-semantics). |
| `@CacheInvalidate(name)` (whole cache) | `@CacheEvict(cacheNames, allEntries = true)` | Direct equivalent. |

JetCache's `name` used a trailing-separator convention plus `keyPrefix`
to scope keys in Redis. Tiercache uses the Spring cache name directly as
the cache identity; configure it under `tiercache.caches.<name>`.

## Configuration mapping

JetCache's `area` concept (`default` area + named areas, each with
`jetcache.local.<area>.*` and `jetcache.remote.<area>.*` blocks) maps onto
Tiercache's two-level inheritance model: `tiercache.defaults.*` is the
area applied everywhere, `tiercache.caches.<name>.*` is a named area with
only the fields it overrides.

| JetCache property | Tiercache property | Notes |
|---|---|---|
| `jetcache.local.<area>.limit` | `tiercache.caches.<name>.l1-max-size` | L1 entry bound (default `10000`). |
| `jetcache.local.<area>.expireAfterWriteInMillis` | `tiercache.caches.<name>.l1-expire-after-write` | Duration styles `5m`, `PT5M`, `300s` all accepted. Default `5m`. |
| `jetcache.local.<area>.expireAfterAccessInMillis` | `tiercache.caches.<name>.l1-expire-after-access` | Disabled by default. |
| `jetcache.remote.<area>.expireAfterWriteInMillis` | `tiercache.caches.<name>.l2-ttl` | Logical L2 TTL (default `1h`). Invariant: every L1 TTL must be `<= l2-ttl`; violations abort startup. |
| `jetcache.remote.<area>.uri` (`redis.lettuce` type) | `tiercache.redis-uri` | One URI for the whole application; required when `tiercache.enabled=true`. |
| `jetcache.<local\|remote>.default.*` | `tiercache.defaults.*` | Global defaults level. |
| `jetcache.<local\|remote>.<area>.*` | `tiercache.caches.<name>.*` | Named per-cache override level. |
| `cacheNullValue=true` on `@Cached` | `tiercache.caches.<name>.null-policy: allow` | Nulls are cached as markers with `null-marker-ttl` (default `1m`). Default is `deny`. |
| `jetcache.remote.<area>.broadcastChannel` | `tiercache.invalidation.*` | Cross-instance invalidation is wired automatically with the Redis transport (`pubsub` profile default, `streams` optional); missed events are replayed from a journal on recovery. |

JetCache parameters with no Tiercache equivalent — encoders, key
converters, `areaInCacheName`, `keyPrefix`, `statIntervalMinutes` — are
simply dropped. Serialization is a serializer SPI, not per-cache
configuration (see [configuration](configuration.md)); statistics are
always-on metrics when a `MeterRegistry` exists, not a polled interval —
see [observability](observability.md).

The full property reference is [configuration](configuration.md).

## Example: configuration

Before (`application.properties`):

```properties
jetcache.statIntervalMinutes=15
jetcache.areaInCacheName=false
jetcache.local.default.type=caffeine
jetcache.local.default.limit=10000
jetcache.local.order.type=caffeine
jetcache.local.order.limit=1000
jetcache.local.order.expireAfterWriteInMillis=300000
jetcache.remote.default.type=redis.lettuce
jetcache.remote.default.uri=redis://localhost:6379/
jetcache.remote.default.expireAfterWriteInMillis=3600000
jetcache.remote.default.keyConvertor=fastjson2
jetcache.remote.default.valueEncoder=java
jetcache.remote.default.valueDecoder=java
```

After (`application.yml`):

```yaml
tiercache:
  enabled: true
  redis-uri: redis://localhost:6379
  defaults:
    l1-max-size: 10000
    l1-expire-after-write: 5m
    l2-ttl: 1h
  caches:
    order:
      l1-max-size: 1000
```

`tiercache.enabled=true` is the master switch; a missing `redis-uri`
fails startup with an error naming the property. L1 and L2 TTLs here are
the built-in defaults, shown explicitly for clarity.

## Example: annotated service

Before:

```java
import com.alicp.jetcache.anno.CacheInvalidate;
import com.alicp.jetcache.anno.CachePenetrationProtect;
import com.alicp.jetcache.anno.CacheType;
import com.alicp.jetcache.anno.CacheUpdate;
import com.alicp.jetcache.anno.Cached;

import java.util.concurrent.TimeUnit;

@Service
public class OrderService {

    @Cached(name = "order:", key = "#id",
            expire = 3600, timeUnit = TimeUnit.SECONDS,
            cacheType = CacheType.BOTH, cacheNullValue = true)
    @CachePenetrationProtect
    public Order getOrder(long id) {
        return repository.findById(id).orElse(null);
    }

    @CacheUpdate(name = "order:", key = "#order.id", value = "#order")
    public void updateOrder(Order order) {
        repository.save(order);
    }

    @CacheInvalidate(name = "order:", key = "#id")
    public void deleteOrder(long id) {
        repository.deleteById(id);
    }
}
```

After:

```java
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    @Cacheable(cacheNames = "order", key = "#id", sync = true)
    public Order getOrder(long id) {
        return repository.findById(id).orElse(null);
    }

    @CachePut(cacheNames = "order", key = "#order.id")
    public Order updateOrder(Order order) {
        return repository.save(order);
    }

    @CacheEvict(cacheNames = "order", key = "#id")
    public void deleteOrder(long id) {
        repository.deleteById(id);
    }
}
```

With the null-caching policy moved to configuration:

```yaml
tiercache:
  caches:
    order:
      null-policy: allow
```

Three mechanical rewrites:

1. `expire`/`timeUnit`/`cacheType`/`cacheNullValue` leave the annotation
   and become `tiercache.caches.order.*` properties (shown above).
2. `@CachePenetrationProtect` is deleted — miss coalescing is always on;
   `sync = true` on `@Cacheable` selects the coalescing loader path.
3. `@CacheUpdate(..., value = ...)` becomes `@CachePut` returning the
   value, matching Spring's contract.

`@EnableCaching` is required, as with any Spring Cache setup.

## What changes semantically

- **Eventual consistency.** Cross-instance invalidation events refresh L1
  copies asynchronously; there is a bounded staleness window between a
  write on one instance and visibility on others. JetCache's broadcast
  channel had the same property, but Tiercache surfaces it explicitly
  (metrics, journal replay, degradation behavior).
- **TTLs live in configuration, not annotations.** Recompiling to change
  an expiry is no longer needed; but a TTL ordering violation (L1 longer
  than L2) now aborts startup instead of silently serving stale data.
- **Loader coalescing.** Concurrent callers for the same missing key share
  one method execution. JetCache's `@CachePenetrationProtect` did this
  per annotation; here it applies everywhere by default, and `@Cacheable`
  methods without `sync = true` follow standard Spring get-then-put
  semantics (no coalescing on that path).
- **Degradation.** When Redis fails, a circuit breaker (on by default)
  drops the cache to L1-only mode with no infrastructure exceptions
  escaping into business code, then recovers automatically. See
  [configuration](configuration.md#circuit-breaker-and-degradation).

## Verifying the migration

1. Start the app — startup fails fast on a missing `redis-uri` or an
   invalid TTL ordering, so a clean start already validates the config.
2. Call a `@Cacheable` method twice: the second call is served from L1.
3. Add the metrics module and check `tiercache.requests` — see
   [observability](observability.md).
