# Repository quality gates

Commands in this guide run from the repository root. For consuming the published
TCK artifact, see the [TCK user guide](../docs/tck.md).

## Running the suite from the repository

```bash
./gradlew :tiercache-tck:test
```

This is part of the aggregate `./gradlew build` and requires Docker.

## Optional gates

All of the following are excluded from `check`; run them explicitly.

| Command | What it does | Budget |
|---|---|---|
| `./gradlew :tiercache-tck:soakTest` | Churn soak against a real L2 container. Default duration PT10M; override with `-Dtiercache.soak.duration=PT24H` for the full profile. Not run in CI (hosted runners kill long jobs) — run it locally or on your own hardware before releases. | Each memory series ≤ 5%, complete workload, bounded journal |
| `./gradlew :tiercache-tck:vtStressTest -PtiercacheVtJdk=21` | Separate fresh-JVM cold diagnostic and strict steady-state read gate, plus recovery/lock checks. Use 25 to select Java 25 explicitly. | Zero steady-state product pinning; complete workload and valid evidence in both phases |
| `./gradlew :tiercache-tck:jmhBenchmark` | Throughput benchmark against a Redis container: `mixedWorkload` (95% hot L1 hits / 5% cold cascade reads, reference profile), `cascadeRead` (pure cascade, worst-case reference), `l1Hit` (attribution control). Results in `tiercache-tck/build/results/jmh-benchmark/results.txt`. | No absolute budget — trend/regression measurement (throughput is environment-dependent) |
| `./gradlew :tiercache-tck:propagationBenchmark` | Invalidation propagation latency harness (two Pub/Sub instances, 10k events by default; override with `-Dtiercache.propagation.events`). Results in `tiercache-tck/build/results/propagation/results.txt`. | p99 ≤ 5 ms publish-to-applied (single AZ) |

The propagation harness (`io.tiercache.tck.PropagationBenchmark`) is a plain
`main` class inside the `tests` jar, so consumers can also run it from the
artifact on a classpath assembled as shown above.

See [VT validation](virtual-thread-validation.md) for accepted startup policy, fixed warmup on separate state, failure checks, evidence paths and the repeated 21/25 runtime command.

### Real-journal recovery and virtual threads

`RecoveryJfrTest` supplements the in-memory 100k-thread read gate with a real
Redis journal, a virtual-thread HTTP probe and the registered reconnect
callback. Deterministic gates verify callbacks return before replay; JFR
checks library-attributed monitor pinning on JDK 21. Run it with
`./gradlew :tiercache-tck:vtStressTest --tests '*RecoveryJfrTest'`; use
`-PtiercacheVtJdk=25` for newer-JDK functional coverage. Recordings remain in
`tiercache-tck/build/reports/vt/jdk-*/independent/*/`. See [recovery](../docs/recovery.md).

### Streams pending and corruption

`RedisStreamsRecoveryTest` and `ValkeyStreamsRecoveryTest` cover pending batch
remainder, missing payloads on Redis 6.2/newer reply behavior, safe baseline
and clear gates, ACK reply loss, same-group stable resume, other-group
isolation, closed/superseded callbacks and corrupt replay anchors. Shared
decoder tests verify sanitized failures and typed payload compatibility;
metric tests verify fixed labels under non-English JVM locales.

## Strict soak measurements and evidence

`./gradlew :tiercache-tck:soakTest` always performs a new run; an earlier Gradle
up-to-date result cannot satisfy this gate. The default remains PT10M with eight
workers and a 30-second sample interval. Use
`-Dtiercache.soak.duration=PT24H` for the full profile. The strict minimum is PT1M30S:
at least four total samples are needed to retain three after warm-up. Shorter or
incomplete diagnostic runs cannot pass the release gate.

Two independent memory series are measured in bytes:

- Post-GC heap comes from heap-pool usage in a completed explicit `System.gc()`
  notification. Completion must be observed within five seconds. A fixed sleep,
  an unrelated young GC, or ignored explicit GC is not evidence of a collected
  live set. Remove `-XX:+DisableExplicitGC` and use a collector exposing the required
  completion notifications if preflight reports the measurement inconclusive.
- Process RSS is read from Linux `/proc/self/status` (`VmRSS`) or macOS
  `/bin/ps -o rss= -p <JVM pid>` with a fixed locale and two-second command timeout.
  Both sources report KiB, converted to bytes. Missing, zero, negative, malformed,
  timed-out or unsupported measurements fail the strict gate. Virtual/committed
  memory is never substituted for RSS or added to live heap.

Preflight validates measurement availability before starting Redis/workload traffic.
The first `ceil(sample count × 0.20)` samples are discarded. Each remaining series
uses its own fixed first steady-state baseline; its peak must be no more than 5%
higher. Adjacent increments below 5% do not hide cumulative growth above the budget.
The journal checks remain: peak ≤ twice configured capacity (4000 rows here), and
second-half mean ≤ first-half mean + 25% of capacity (500 rows here). Approximate
Redis trimming is unchanged.

Every submitted worker Future is inspected, including failures captured by
FutureTask. Workers must start, perform successful operations and reach the intended
deadline. Errors, exceptions, early completion, cancellation or failure to terminate
within 60 seconds fail the workload. Cleanup always cancels/interrupts remaining work
and waits at most one additional second; uninterruptible work is reported, never
converted into a successful run.

The report is `tiercache-tck/build/reports/soak/report.json` by default; override with
`-Dtiercache.soak.report=/absolute/path/report.json`. It is updated after every
sample and finalized for PASS or FAIL, including acquisition/workload failures.
It contains duration, JDK/collector, heap configuration, sources/units, independent
memory/journal series, explicit-GC completion counts, per-worker operation counts,
Future outcomes and fixed-baseline assessments. Keep the JSON alongside Gradle logs,
not just the BUILD SUCCESSFUL line. A ten-minute pass is evidence for that observed
run, not proof against every leak or a substitute for a day-long soak.


## Platform compatibility and Sentinel

The [platform matrix](../docs/compatibility.md) describes the pinned standalone server
contracts, isolated Boot 3.5/4.1 consumer builds and real starter-managed Sentinel
failover regressions. Run commands and evidence locations are documented there.
These checks complement the churn/chaos suite; they do not imply Redis Cluster
support, instantaneous failover freshness or recovery of unstored invalidations.

## Shared local-cache contention

For a CPU-only shared-instance L1 diagnostic, including stale retention, access expiry, collision controls and strict cascade attribution, see [the shared L1 benchmark](l1-contention-benchmark.md). It runs separately from the existing thread-local nightly baseline and the real-Redis TCK benchmarks.
