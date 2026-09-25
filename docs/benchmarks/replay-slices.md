# Bounded replay application: experiment, not an accepted speedup

The 32-row candidate preserves deterministic replay/lifecycle guarantees but **does not meet the frozen latency acceptance criterion**. Do not describe it as a proven performance improvement, close its performance task, or use these measurements as a release latency promise.

## What changed

The baseline is commit `9af4aee`. Both sides include the same bounded Pub/Sub dispatcher from that commit. Only the candidate changes replay application: a checked Redis response still contains at most 256 rows, but each cache-state monitor acquisition applies at most 32 rows. One recovery pass still allows 16 actual checked reads. A retained response is not fetched again at each slice boundary.

Each slice validates registration, target identity, token, service generation, committed cursor and target recovery generation. Notifications for its committed prefix run outside the monitor, including when a later target operation throws or becomes obsolete. An empty, validated checked read remains necessary for caught-up proof; a slice boundary cannot complete recovery or authorize Streams ACKs. Existing capacity, corruption, retry and full-reset protocols remain in place.

A monitor release permits interleaving; it does not guarantee fair scheduling. One full clear or slow custom target call can still hold the monitor for an arbitrarily long time. The implementation adds no sleep, yield, timed handoff or per-row executor task. It keeps one response, at most 32 pending notifications, and the existing bounded tracking structures.

## Experiment

Measured on macOS 14.5, OpenJDK 21+35, `-Xms512m -Xmx512m`, dedicated Docker Redis 6.2 Alpine (image `sha256:9d6d416217a50326c190acf2797ee26ca354b6bd055e4399bc97427ff7af369f`). This is a focused two-cache protocol experiment, not a six-instance capacity test.

Each fork has one warmup round and four measured rounds. Each round uses fresh cache names and stock Caffeine/DefaultTierCache targets. The ordinary profile starts with 32,768 missed UPDATE rows. One ordered producer sends 1,000 live updates at a total target rate of 500/s, alternating the recovering cache and a second healthy cache. The healthy control has no initial backlog. The clear profile replaces the middle backlog row with EVICT_ALL and is reported separately.

A live operation atomically appends its journal row and publishes through real Lettuce Pub/Sub. Latency runs from just before that command to the first corresponding application notification (live or replay); it includes Redis command/transport latency. Replay and live duplicates are counted once. Ordinary/clear latency samples include only messages submitted while the first recovery was pending. The healthy profile includes all live messages. Final validation checks latest values, the history tail, absence of the cleared prefix, cursor equality and pending=false. A final replay settles the live schedule's tail.

Three forks per version/profile ran sequentially, alternating baseline/candidate order, with no concurrent builds or traffic generators. Baseline classes and jars were frozen before production edits. The same benchmark class was used for both versions. The only harness adjustment after the initial reference run moved the final Redis cursor read outside the verification monitor; the workload and thresholds did not change. Exploratory reference runs are excluded from the scored results. Copy-on-write latency collectors introduce equal harness overhead on both sides.

Before the candidate ran, the acceptance plan froze a minimum 10% reduction in median ordinary same-cache p99, an improvement in at least two of three paired forks, at most 10% regression in healthy completion throughput or ordinary catch-up time, and no more than 10% read-count inflation. Exact absence of per-slice reads is tested deterministically because live tick timing changes benchmark read totals.

## Results

Latency columns are medians of fork percentiles, not percentiles of pooled data. Catch-up is the mean of 12 rounds; CPU is the mean measured process CPU per fork. Throughput is completed events divided by measured duration, including final convergence; it is **not maximum capacity**.

| Profile | Version | Same-cache p50 / p95 / p99, ms | Other-cache p99, ms | Catch-up, ms | Events/s | CPU, s |
|---|---|---:|---:|---:|---:|---:|
| Ordinary | Baseline | 1.219 / 4.825 / 7.523 | 8.255 | 423.004 | 496.198 | 4.519 |
| Ordinary | 32-row candidate | 1.220 / 4.779 / 8.156 | 7.543 | 418.433 | 497.882 | 3.872 |
| Healthy | Baseline | 1.105 / 3.420 / 7.369 | 6.841 | 2.602 | 496.065 | 3.108 |
| Healthy | 32-row candidate | 1.151 / 3.118 / 5.992 | 5.541 | 3.084 | 473.429 | 3.296 |
| Full clear | Baseline | 7.113 / 205.279 / 240.714 | 7.688 | 424.707 | 497.149 | 4.285 |
| Full clear | 32-row candidate | 5.402 / 185.199 / 225.222 | 6.986 | 428.636 | 497.855 | 4.125 |

Ordinary paired p99 values were 7.523 → 11.801, 9.817 → 8.156, and 6.717 → 7.172 ms. Only one pair improved. The median increased 8.4%, so the primary criterion fails. Ordinary catch-up time did not regress; healthy completion throughput fell 4.6%, within the guardrail. These noisy results do not establish a universal regression either, but they cannot justify the intended speedup.

All 18 scored forks passed coherence checks. Ordinary checked-read counts remained 138–140 per round on both versions. Healthy counts were 10–11. Clear counts were 135–141 vs 137–142; timing of live ticks and fence resumption accounts for differing totals, without multiplying reads by eight. The deterministic 256-row test verifies the stronger invariant directly.

Full-clear latency is dominated by the delivery fence that protects EVICT_ALL replay. That fence deliberately holds same-cache delivery until recovery finishes. The slice refactor does not remove it. Mixed UPDATE/INVALIDATE/duplicate semantics have deterministic coverage, but this experiment does not establish performance for mixed invalidations, sustained saturation, many writers, remote Redis, or arbitrary custom targets.

Raw measurements and the frozen acceptance plan: [scored results](replay-slices-results.json).

## Separate JFR attribution

One additional fork per version recorded `jdk.JavaMonitorEnter` with threshold zero and stack traces. These profiled latency numbers are excluded from acceptance. Recordings cover startup/warmup as well as measured rounds.

| CacheState monitor | Baseline | Candidate |
|---|---:|---:|
| Contended entries | 95 | 101 |
| Aggregate wait, ms | 133.702 | 95.644 |
| Maximum wait, ms | 32.762 | 9.939 |

This supports reduced monitor waiting in that recording, not a proven end-to-end speedup. It is one attribution sample, not a statistically repeated JFR result. See [JFR summary](replay-slices-jfr.json).

## Reproduction

Use a dedicated Redis endpoint. Export and **copy** all baseline runtime entries before compiling the candidate; saving only a path string into mutable build directories is insufficient. The benchmark source must exist on both classpaths; compile the same harness against each runtime or prepend a directory containing only the identical benchmark classes.

```sh
./gradlew -I scripts/pubsub-runtime.gradle \
  -PpubsubClasspathFile=/absolute/path/runtime.txt \
  :tiercache-transport-redis:exportPubSubRuntime --offline
python3 scripts/benchmark-replay-slices.py \
  --baseline-classpath /absolute/path/frozen-baseline.txt \
  --candidate-classpath /absolute/path/candidate.txt \
  --java /path/to/jdk-21/bin/java \
  --redis redis://127.0.0.1:16390 \
  --output build/replay-comparison
```

For attribution, repeat with `--jfr --forks 1` and a different output directory. Inspect recordings with `jfr print --events jdk.JavaMonitorEnter`. Do not mix those results into the unprofiled comparison.

## Correctness checks and an existing TCK failure

The first 32-row boundary test was run before the production edit: it failed on the baseline with 256 applied rows instead of 32. The new deterministic suite has 11 passing cases. It proves that live delivery can enter after 32 committed rows without another Redis read; clear, replacement, recovery restart and close invalidate the retained tail; notification exceptions/reentrancy do not lose committed prefix events; tick stops at its first unknown row; own full clears update epochs; mixed UPDATE/INVALIDATE and duplicate rows retain version semantics; and trimming a fetched response does not force per-slice reads. A subsequent read failure still invokes the existing baseline-before-clear RESET_SAFE path. Existing large-pass tests retain the 16-read budget and bounded continuation queue.

Java 17 final module results: core 452, invalidation 80, transport 195, Micrometer 26, Micronaut 47, Spring 57, Kotlin 25 and Reactor 13 tests pass (unchanged Kotlin/Reactor tasks were up to date). The full transport suite includes Redis/Valkey, corruption, Streams pending/reclaim/ACK and baseline/reset contracts. Core dependency audit passes; branch coverage is 808/896 (90.18%).

Java 21: 452 core, 80 invalidation, 195 transport and all three VT/JFR gates pass. Java 25: the same 727 module tests pass; the recovery and lock-lifecycle JFR gates pass, but the general 100,000-VT stress gate fails with 18 library-stack pinning events. Its isolated rerun also fails, with 72 events; that retry's first captured stack includes BuiltinClassLoader.loadClass and DefaultTierCache.freshnessOf. The recovery-specific JFR gate reports zero library pinning on both JVMs. The general VT test does not configure invalidation at all and executes unchanged core code, so its failure is outside the replay slice path. The stack samples alone do not establish the cause of every pinning event; do not weaken the gate or label this harmless without separate investigation. Java 25 runtime acceptance therefore remains open.

**The full build is not green.** The 56-test candidate TCK run failed `RedisInvalidationRaceTest.raceConvergesToL2Truth`: L1 did not converge to Redis truth within 30 seconds. Its isolated rerun passed. Repeating the full TCK with the frozen baseline invalidation jar, keeping every other runtime dependency unchanged, reproduced the same failure for both Redis and Valkey. This establishes an existing intermittent problem; it does not identify its root cause or prove it harmless. The race scenario sends 50 remote events to each side, below the 64-event tick cadence, and does not explicitly request replay. Investigate this separately before release; do not hide it with retries or increase the timeout as a substitute for understanding the mismatch.

An earlier core run also failed `degradedCoordinationFallsBackToSingleflight` (two loader calls rather than one); its full-suite retry passed. Its countdown occurs before callers actually join getOrCompute, so the test's barrier does not establish that all callers share one active flight. Core production code is unchanged by this candidate. This observation is recorded without modifying the unrelated test or claiming that the entire failure is diagnosed.

## Decision

Performance acceptance, the clean regression-suite gate and the Java 25 VT gate remain open. On 2026-09-25 the user requested committing and archiving this implementation with those results disclosed. Archival records that decision; it does not turn the three incomplete acceptance tasks into passed gates or establish a proven optimization. A reasonable next decision is either to isolate the independently valuable committed-notification correctness fix, or explicitly revise the experiment to study repeated, longer replay overlap at several producer rates. Neither reducing the acceptance bar nor changing scheduling with fair locks/owner queues is justified by this run without a separate design decision.
