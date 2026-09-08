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

Reads cascade L1 → L2 → loader; an L2 hit always warms L1 (F-01). Concurrent loads of the same key share one loader execution (singleflight, F-20) — on by default. Invalid configuration (e.g. L1 TTL > L2 TTL) fails fast at startup (F-04/F-05). Atomic `putIfAbsent` is backed by the L2 `SET NX PX` primitive (F-03). Null caching (F-25) is available per cache via `NullPolicy.allow(ttl)` — repeated lookups of nonexistent keys are absorbed by an explicit marker; use `lookup(key)` to distinguish miss / cached-null / hit.

The cache is eventually consistent by design; no strong-consistency guarantees are given or implied (F-15).

## License

[Apache License 2.0](LICENSE)
