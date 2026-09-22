# Tested platforms

The library baseline is Java 17. Redis 6.2 remains the minimum server line; newer
server profiles are additional compatibility evidence, not a reason to raise that
floor. These tests do not establish support for every patch, OS or deployment.

## Reproducible profiles

Exact versions live in [`compatibility/platforms.properties`](../compatibility/platforms.properties).
The transport contracts select the server through a Gradle task input and retain
JUnit XML, the command, selected image and Docker image identity. All profiles run
the same storage, conditional/versioned writes, expiration, tags, namespace,
journal, Pub/Sub and Streams tests.

| Profile | Image | Scope |
|---|---|---|
| Redis compatibility floor | `redis:6.2.24-alpine` | Standalone transport contracts |
| Redis 7.4 | `redis:7.4.11-alpine` | Standalone contracts and Sentinel |
| Redis 8 | `redis:8.10.2-alpine` | Standalone transport contracts |
| Valkey | `valkey/valkey:9.1.2-alpine` | Standalone transport contracts |

**Redis Cluster is unsupported by the stock transport.** Multiple application
instances and Sentinel failover do not imply Redis Cluster slot routing, key
placement or multi-key script compatibility. Sentinel evidence here uses Redis
7.4; it does not establish Valkey Sentinel compatibility.

## Spring consumers

Independent builds in `compatibility/spring-consumer` consume locally published
TierCache artifacts. They do not use Gradle project substitution or reuse the
library's compilation graph. Each run imports its own enforced Boot BOM and saves
the resulting runtime graph, including the actual Lettuce version.

| Consumer BOM | Spring Framework | Resolved Lettuce | Test JVM |
|---|---|---|---|
| Boot 3.5.16 | 6.2.19 | 6.6.0.RELEASE | Java 17 |
| Boot 4.1.1 | 7.0.9 | 7.5.2.RELEASE | Java 17 |

The library itself compiles against Lettuce 7.7.0.RELEASE. A consumer BOM can select
a different client: the table records what was actually tested, not what the
library's own dependency catalog suggests. The consumer checks cover automatic
starter discovery, custom CacheManager backoff, disabled activation, synchronous
annotations, allowed null caching, metrics and `CompletableFuture` caching with
`sync=true` and a coalesced loader.

Boot 3.5 is retained as a compatibility baseline; its OSS maintenance has ended.
Compatibility is not a security-maintenance promise. Boot 4.1 is the modern
consumer profile. Check upstream [supported versions](https://github.com/spring-projects/spring-boot/wiki/Supported-Versions)
when choosing a production release.

## Sentinel scenarios and limits

The fixture creates a unique Docker network with one primary, two replicas,
three Sentinel processes and a JVM containing two independent Spring contexts.
Both contexts use the actual starter-managed data, lock, journal and Pub/Sub
connections. Running the JVM inside that network makes the advertised addresses
reachable without a test-only address translator or replacement RemoteCache.
Readiness requires agreed primary addresses, a reachable primary and Sentinel
quorum. Retained-history assertions wait for both replicas to catch up first.

Three bounded scenarios run separately:

1. Planned promotion through `SENTINEL FAILOVER`.
2. Abrupt primary loss through Docker `SIGKILL`.
3. Complete Redis outage, a source-version change and an eviction that cannot
   reach Redis, followed by restart from the retained AOF history.

The first two deliberately omit one publication while keeping the versioned
update and journal row. The second context must replay that history, observe
`v2` and retain an unaffected L1 entry. Both breakers must finish recovery before
new distributed writes are asserted. Successful Sentinel election alone is not
application recovery; writes made while degraded can remain local.

The outage scenario records the observed value independently of call success
and checks that the missed eviction did not appear in the restored journal.
Replay cannot reconstruct an event never stored in Redis. There is no promised
one-second freshness bound, zero loader burst, or lossless asynchronous Redis
replication. The prepared replication barrier narrows the first two scenarios
to retained-history recovery; it does not test loss of unreplicated writes.

Protected cache calls are checked against a five-second scenario bound, workers
and topology waits have finite deadlines, and only fixture-owned resources are
removed in cleanup. Full logs and topology events remain on disk on failure.
These finite regressions do not cover every partition or failover interleaving.

## Running the checks

Run from the repository root with Docker, Python 3 and a Java 17 toolchain:

```bash
# All standalone profiles, or pass redis62 / redis74 / redis8 / valkey.
python3 compatibility/run-server-matrix.py

# Independent consumer builds against the locally built artifacts.
./gradlew stageCompatibilityArtifacts
python3 compatibility/run-consumers.py

# Planned promotion, primary kill, full outage; individual names are accepted.
./gradlew :tiercache-spring-boot-starter:sentinelRuntime
docker pull eclipse-temurin:17-jre
python3 compatibility/run-sentinel.py
```

Artifacts go to `build/compatibility-evidence/`: per-profile JUnit results and
image identity, consumer dependency graphs, and Sentinel topology events and
container logs. `sentinelRuntime` includes the test module's runtime; it is not
used as a substitute for the separately published-artifact consumer tests.
The fixture JVM image is a Java 17 runtime; its exact runtime version is logged.

PR CI runs the compact standalone matrix and both Spring consumers on Java 17.
Nightly runs the three Sentinel scenarios with a bounded job timeout. Existing
JDK 21/25 build checks remain separate; this is deliberately not a Cartesian
product of all JDKs, servers and topology failures.
