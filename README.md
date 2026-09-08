# Tiercache

A two-level cache for the JVM: L1 in-process (Caffeine) + L2 Redis/Valkey, with cross-instance invalidation, built-in protection against stampede/avalanche/penetration/L2 degradation, and first-class observability.

Status: early development (0.x, incubating API). See `AGENTS.md` for the design principles, module structure, and roadmap.

## Quick start

Build:

```bash
./gradlew build
```

Minimal usage (current milestone: `tiercache-core` with an in-memory L2 test double; the Redis transport lands in a later change):

```java
import io.tiercache.TierCache;
import io.tiercache.TierCacheFactory;
import io.tiercache.testkit.InMemoryRemoteCache;

TierCache<String, String> cache = TierCacheFactory.builder()
        .remoteCache(new InMemoryRemoteCache<String, String>()) // L2 SPI implementation
        .build()
        .getCache("users");

String name = cache.getOrCompute("user:42", key -> loadFromDatabase(key));
```

Reads cascade L1 → L2 → loader; an L2 hit always warms L1 (F-01). Concurrent loads of the same key share one loader execution (singleflight, F-20) — on by default. Invalid configuration (e.g. L1 TTL > L2 TTL) fails fast at startup (F-04/F-05).

The cache is eventually consistent by design; no strong-consistency guarantees are given or implied (F-15).

## License

[Apache License 2.0](LICENSE)
