# CLOSED breaker admission: state and measurement inventory

Baseline: `09d32b5`. This change keeps one factory-wide breaker and exact synchronized completion accounting. It does not partition, approximate or batch the last-N outcome window. Performance acceptance depends on repeated measurements, not merely on fewer monitor entries.

## State and epoch publication sites

| Site | Authoritative mutation | Admission-visible publication / validation |
|---|---|---|
| Construction | CLOSED, epoch 0, empty window | Initial immutable admission snapshot before publication of the breaker |
| `openLocked` | Increment epoch; clear recovery-pending; set OPEN; read opening time | Publish matching OPEN/epoch after all transition fields are ready, before returning the deferred observer |
| `advance` | OPEN -> HALF_OPEN after the clock/backoff; reset probe counters; same epoch | Publish HALF_OPEN/same epoch under the monitor |
| `closeLocked` | Increment epoch; CLOSED; clear pending and ring counters | Publish CLOSED/new epoch only after the same-epoch recovery decision commits |
| `detachRecovery` | Retire hook/executor; increment epoch; clear pending/probes/backoff; preserve state and OPEN deadline | Publish even when state stays CLOSED or OPEN |
| `configureRecovery` | Install hook/executor before use | No state/epoch transition; stays synchronized |
| `successLocked` / `failureLocked` / `record` | Exact outcome ring, probe counters, pending flag; delegate actual transitions above | No new snapshot per ordinary outcome; pending is only consulted on the locked non-CLOSED path |
| Queued `startRecovery` | No state transition | Validate epoch, pending flag and hook identity before invoking external code outside the monitor |
| `finishRecovery` | Validate epoch/hook/pending, then reopen/close | Obsolete completions cannot publish a successor state or notify recovery |

CLOSED `isOpen`, `state`, `tryAcquire` and `tryAcquirePermit` read one volatile immutable state/epoch pair. Non-CLOSED observations acquire the monitor and recheck authoritative state. HALF_OPEN budgets and recovery-pending rejection remain serialized. A permit retains its captured epoch; it never rereads the successor epoch to assign its outcome. Compatibility `onSuccess`/`onFailure` continue to use existing locked outcome methods and deferred observer execution.

A fast admission overlapping an unpublished OPEN transition belongs to the old CLOSED epoch. Its remote call may begin after publication, as with any operation descheduled after admission. Its completion cannot affect the successor epoch. Calls beginning after transition return see the published state.

## Healthy callers

`DefaultTierCache` uses `isOpen` for cascade and degraded-path selection. `CircuitBreakerRemoteCache` and `BreakerLockProvider` acquire epoch-bound permits around remote attempts; Redis/lock I/O already runs outside the breaker monitor. Factory degraded/state inspection also uses the state methods. Actual successes still enter the ring and age out failures; neutral cancellation and duplicate completion remain once-only and epoch-bound.

## Verification and experiment boundaries

`BreakerAdmissionContractTest` models 4,000 ordered outcomes, ring eviction/open thresholds, neutral and duplicate completions, CLOSED detachment, and concurrent HALF_OPEN reservations. Existing recovery tests cover queued/in-flight obsolete hooks and observer isolation. `ClosedAdmissionProgressTest` pauses a real opening completion in the controllable clock before transition publication: the baseline blocks admission and times out; the candidate must expose the still-published CLOSED epoch and discard its late result after OPEN publication.

The shared-breaker benchmark has healthy/mixed full admission-plus-completion paths, OPEN rejection and a fully reserved HALF_OPEN control at 1/8/32/64 threads. Its 128-outcome window exceeds the maximum simultaneous failure streak possible with 64 sequential callers failing once per eight calls, so mixed accounting cannot silently become OPEN throughput; teardown checks CLOSED. The separate admission-only diagnostic uses compatibility CLOSED admission, which creates no probe reservation and requires no leaked-permit cleanup. It is not full remote-path throughput.

JMH uses 3 x 2-second warmups and 3 x 2-second measurements in each fresh fork, allocation profiling and whole-process CPU/op. Full Redis measurements use stock Caffeine, one factory and a shared breaker: a one-entry L1 yields an asserted L2-heavy path, six named caches share the factory in the multi-cache profile, mixed alternates 95% hot with 5% cold requests, and the hot control asserts zero Redis calls. Values, breaker state and real remote-call fractions are validated. No artificial Redis latency is added.

Full-path latency samples one request in 31 (coprime with the 20-request mixed cycle) into bounded per-worker rings; throughput includes final worker collection. Whole-process CPU/allocation includes client and harness work. Worker allocation is separately scoped to calling threads. The Java 17 management interface lacks a process allocation counter, so process allocation is explicitly unavailable there; worker allocation remains measured. Supported newer JVMs provide the process counter, including the virtual-thread profile where per-worker counters can be unavailable. Summed monitor waits are not request latency.


The Redis fixture is read-only after setup, uses unversioned values and does not run Pub/Sub or a journal. It measures the actual stock L1/guarded L2 read path, not every invalidation/write profile or six-node HTTP capacity. Recovery and rejected-admission correctness are covered separately by guarded-path and existing integration tests.

## Results and acceptance (2026-09-26)

The candidate passed the targeted CLOSED throughput criterion. The primary Java 21 / 32-caller median was 9.34 -> 26.94 million operations/s (2.88x); all three paired forks improved, with a descriptive paired bootstrap interval of 2.72–3.01x. This exceeds the predeclared 10% target and two-of-three repeatability criterion. CPU per operation fell from approximately 137 to 47 ns. The exact completion window remains synchronized.

Healthy full admission-plus-completion results (median fork throughput, million operations/s):

| Runtime | Callers | Baseline | Candidate | Ratio |
|---|---:|---:|---:|---:|
| Corretto 17.0.5 | 1 | 34.61 | 98.77 | 2.85x |
| Corretto 17.0.5 | 32 | 2.45 | 6.69 | 2.73x |
| OpenJDK 21+35 | 1 | 31.40 | 90.27 | 2.87x |
| OpenJDK 21+35 | 8 | 8.75 | 26.69 | 3.05x |
| OpenJDK 21+35 | 32 | 9.34 | 26.94 | 2.88x |
| OpenJDK 21+35 | 64 | 8.95 | 28.36 | 3.17x |
| GraalVM JDK 25 | 1 | 23.33 | 65.83 | 2.82x |
| GraalVM JDK 25 | 32 | 11.53 | 32.58 | 2.83x |

On Java 21, mixed success/failure accounting improved 2.64–3.18x across the four caller counts. OPEN throughput ratios were 0.94–1.04 and saturated HALF_OPEN 0.99–1.03, with substantial bidirectional fork variation in some contended controls. No repeated material throughput regression was observed. Their CPU estimates are noisier than CLOSED: for example, OPEN/32 paired CPU ratios were 1.35, 1.20 and 0.71, so its +15% ratio of medians must not be presented as a stable measured penalty or silently omitted.

Microbenchmark allocation estimates were near the measurement floor in both versions (healthy <=0.002 B/op). Ratios between such tiny numbers mostly normalize fixed harness allocation by different operation counts and are not proof of a new allocation optimization. The admission-only diagnostic has much larger gains, but omits completion and is not the acceptance target or remote request throughput.

### Actual Redis path

Complete-path throughput did not reproduce the multiple-fold microbenchmark gain. Across the primary and representative profiles, ratios of median fork throughput were 0.97–1.03. That is consistent with the guarded step becoming cheaper while other read-path and transport work dominates this fixture; it is not evidence of increased six-instance HTTP capacity.

| JVM / model | Profile / callers | Throughput ratio | Ratio of fork-median p99 |
|---|---|---:|---:|
| 21 / platform | L2 / 16 | 0.99 | 1.06 |
| 21 / platform | Six caches / 16 | 0.99 | 1.03 |
| 21 / platform | 95% hot / 16 | 0.97 | 1.03 |
| 21 / platform | Pure hot / 16 | 1.03 | 0.96 |
| 17 / platform | L2 / 16 | 0.97 | 1.13 |
| 25 / platform | L2 / 16 | 0.98 | 0.86 |
| 21 / virtual | L2 / 16 | 0.98 | 1.00 |
| 25 / virtual | L2 / 16 | 1.03 | 1.08 |

Latency remains noisy. The Java 17 point estimate above is +13.5%, but its paired ratios were 1.17, 0.94 and 1.07 (paired geometric mean 1.05, bootstrap interval 0.94–1.17): a repeated >10% penalty was not established. Several other Redis p99 intervals also span both improvement and deterioration. These measurements do not establish a universal p99 improvement or statistically exclude every sub-resolution regression.

**The original one-caller Redis p99 guardrail produced a signal that required investigation.** With only 91–144 sampled observations in each three-second round, empirical p99 was effectively one of the largest one or two samples. The candidate's median-of-fork p99 was 1.83x baseline. That result is retained; it was not deleted or labelled a pass.

Before continuing the remaining matrix, a separately declared matched diagnostic used the identical library binaries, two 10-second warmups, three 10-second measurements and every-request latency collection. Each round then had 8–12 thousand samples. Paired median p99 ratios were 0.978, 0.904 and 1.002. The large short-window regression did not reproduce with adequate sampling and longer warmup. This resolves the initial low-concurrency signal as insufficient evidence of a repeatable library regression; it does not prove which individual pause caused each original outlier. The 10% guardrail was not relaxed. Both protocols and all results remain available.

One in-progress JMH pair was interrupted by the investigator during that diagnosis. Its unmatched baseline and interrupted candidate were retained outside scoring, then both halves of that pair were repeated together. Completed pairs were reused without filtering by outcome; no failed library assertions were retried to green.

All Redis runs verified values, remote-call fractions and CLOSED state. Actual Redis allocation per operation stayed close between variants; the hot control's process-allocation number is mostly harness sampling/collection overhead. Unavailable allocation counters are explicit nulls, never zero. Results are descriptive same-host comparisons with three paired forks; bootstrap intervals over three forks are not a general production guarantee.

### Separate JFR attribution

Profiled runs were excluded from the scored throughput/latency comparison. In the saturated shared-breaker microbenchmark, total monitor wait was about 372 seconds across threads in both versions, despite much higher candidate throughput. Contended-entry counts were 381,498 vs 447,891. Removing admission serialization therefore does not imply the exact completion monitor disappears or its aggregate waits become request latency.

In the separate six-cache Redis recording, breaker monitor entries were 1,607 -> 248; aggregate wait across threads 132.5 -> 20.6 ms; maximum recorded wait 2.70 -> 0.81 ms. These are attribution observations from one profiling fork, not repeated p99 evidence.

Execution sampling was sparse: the Redis recordings had 70/73 Java execution samples, with breaker frames in 6/10 and explicit accounting frames in 0/0. The micro recordings had 252/454 samples and explicit accounting frames in 4/0. Inlining, native monitor work and the small sample count prevent a reliable accounting-time fraction; zero observed accounting frames is not zero accounting cost. The measured process CPU/op and throughput carry the performance conclusion, not those frame percentages.

### Correctness gates

The full Java 17 build passed, including 479 core, 80 invalidation, 195 transport, 71 TCK tests, adapters and demo. Core branch coverage was 833/922 (90.35%). Dependency audit passed. Core PIT generated 870 mutations: 707 killed, 25 timed out, 105 survived and 33 had no coverage; the configured >=75% gate passed (732 detected, 84.14%, including timeouts).

Java 21 and 25 each passed all 479 core and 80 invalidation tests plus the complete isolated VT gate (cold, steady-state, real-journal recovery, lock lifecycle and failure-evidence checks). No public configuration, data format, per-factory failure scope, exact accounting, permit ownership or recovery policy changed.

The runtime candidate is accepted for the validated CLOSED admission/observation improvement and coherent epoch behavior. Full Redis throughput is treated as broadly unchanged within the observed dispersion, not advertised as accelerated. The latency limitations and original low-concurrency signal remain part of the evidence.

## Reproducing the comparison

Use a dedicated Redis server and otherwise idle host. Export JMH using `scripts/closed-breaker-runtime.gradle` (`:tiercache-core:exportClosedBreakerJmh`, `-PbreakerClasspathFile=/path/runtime.txt`) and the Redis harness using `scripts/pubsub-runtime.gradle` (`:tiercache-transport-redis:exportPubSubRuntime`, `-PpubsubClasspathFile=/path/runtime.txt`). Both baseline and candidate need identical benchmark sources; copy the harness into the baseline checkout before compiling it. Freeze **all** exported files/directories before changing or rebuilding a version; saving a path string into mutable build outputs is insufficient.

```sh
python3 scripts/benchmark-closed-breaker.py \
  --baseline-jmh-classpath /path/frozen-baseline-jmh.txt \
  --candidate-jmh-classpath /path/frozen-candidate-jmh.txt \
  --baseline-redis-classpath /path/frozen-baseline-redis.txt \
  --candidate-redis-classpath /path/frozen-candidate-redis.txt \
  --java17 /path/jdk17/bin/java --java21 /path/jdk21/bin/java \
  --java25 /path/jdk25/bin/java --redis redis://127.0.0.1:16391 \
  --section target --output build/breaker-comparison/target
```

Run the remaining sections `redis`, `micro-rest`, `representative` and `jfr` sequentially into separate directories. `--resume` only reuses completed indexed measurements; unaccounted files are not overwritten. The full `micro` section includes `target`; do not score both as independent copies of the same paired forks.

For the supplemental one-caller check, invoke `io.tiercache.redis.ClosedBreakerRedisBenchmark` with the same immutable library classpath and arguments `redis-uri l2 1 platform`, adding `-Dbreaker.warmupSeconds=10 -Dbreaker.measureSeconds=10 -Dbreaker.sampleInterval=1` to both JVM commands. Repeat three pairs in alternating order, preserving the short-window reference separately.

Summarize completed sections with `scripts/summarize-closed-breaker.py <directories...> --output <summary.json>`. Stream each separate recording with `java scripts/BreakerJfrSummary.java <recording.jfr>`. Never mix profiled throughput into scored results.

[Structured results and provenance](closed-breaker-results.json) include every comparison interval, the supplemental diagnostic, runtime/PIT checks and attribution. [Compressed raw scored measurements](closed-breaker-raw.json.gz) retain JMH iteration data and Redis round data; decompress with standard gzip tooling. Host-specific Java executable paths in raw JMH output are reduced to the executable name; runtime/version and JVM arguments remain. Raw local JFR recordings are retained by the runner. The dedicated benchmark Redis container was removed after completion.
