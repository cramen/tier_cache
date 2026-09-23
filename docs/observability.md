# Observability

Tiercache exposes metrics for cache outcomes, degradation and invalidation.
Some cleanup failures are log-only, so use metrics together with logs when
diagnosing failures. The `tiercache-micrometer` module binds core events to Micrometer, adds
OpenTelemetry tracing, and exposes a JMX inspection view. A ready-made
Grafana dashboard and Prometheus alert rules ship in `docs/grafana/`.

## Enabling metrics

Core emits semantic events through the `io.tiercache.spi.CacheMetricsListener`
SPI (`NOOP` by default, zero cost). To bind them to a registry:

- **Spring Boot:** add `io.github.cramen:tiercache-micrometer` to your
  dependencies. When a Micrometer `MeterRegistry` bean exists (for example
  via Spring Boot Actuator), the starter wires `MicrometerCacheMetrics`
  automatically; `tiercache.metrics.enabled=false` opts out, and a custom
  `CacheMetricsListener` bean takes precedence.
- **Programmatic:** `TierCacheFactory.builder().metricsListener(new
  MicrometerCacheMetrics(registry))`, then call
  `registerGauges(factory, journal, cacheNames)` once per factory to add the
  gauges.

Prometheus naming note: Micrometer meters named `tiercache.requests` surface
in Prometheus as `tiercache_requests_total` (counters gain `_total`, dots
become underscores); the latency timer surfaces as
`tiercache_latency_seconds` with `_bucket/_count/_sum` series.

## Metrics catalog

All meters are created by
`io.tiercache.micrometer.MicrometerCacheMetrics`.

| Metric | Type | Tags | Meaning |
|---|---|---|---|
| `tiercache.requests` | Counter | `cache`, `result` | Cache lookups by outcome. `result` is one of `l1_hit`, `l2_hit`, `miss`, `load`, `coalesced` (waited on another caller's in-flight load), or `stale_degraded` (retained L1 served while L2 admission is rejected). |
| `tiercache.latency` | Timer | `cache`, `level` | Latency of cache operations by level. The level enum defines `l1`/`l2`; core times only L2-touching operations, so `level="l2"` is what you will see in practice (L1 hits are deliberately not timed — zero clock reads on the hot path). |
| `tiercache.invalidation` | Counter | `cache`, `direction` | Invalidation events by direction: `sent` (admitted publication attempts, not confirmed delivery), `received`, `replayed` (from the journal on recovery), `dropped` (a journal-backed L1 flush because replay history could not be verified, for example after trimming or a read failure). The latter records fallback events, not a count of individually lost messages. |
| `tiercache.degraded` | Gauge | — | `1` while the L2 circuit breaker is open (L1-only mode), else `0`. |
| `tiercache.breaker.state` | Gauge | — | Breaker machine state: `0` = closed, `1` = half-open (probing or awaiting coherence recovery), `2` = open. During half-open `tiercache.degraded` is already back at `0`. |
| `tiercache.invalidation.recovery.pending` | Gauge | `cache` | `1` while triggered recovery or its follow-up/retry is pending, `0` when settled. Unregistered on close. Read alongside breaker state and logs; a failed-baseline clear does not reset it to success. |
| `tiercache.invalidation.stream` | Counter | `cache`, `result` | Stream/journal failures: `decode_failed`, `apply_failed`, `ack_failed`, `resync_failed`. Counts failed attempts, not individual lost invalidations. |
| `tiercache.invalidation.publish` | Counter | `cache`, `outcome` | Terminal publication results: `acknowledged`, `failed`, `unconfirmed`, `not_required`. Counts complete asynchronously in batches; acknowledgement means Redis accepted PUBLISH, not that receivers applied it. |
| `tiercache.journal.size` | Gauge | `cache` | Invalidation journal entries currently held for the cache. |
| `tiercache.last.load.age` | Gauge | `cache` | Milliseconds since the last load event for the cache, tracked from `LOAD` outcomes, not every put or per-entry metadata. |
| `tiercache.null.entries` | Counter | `cache` | Null-markers stored under the `allow` null policy. |
| `tiercache.l2.stale.hits` | Counter | `cache` | L2 entries served stale (past logical TTL, inside the stale window). |
| `tiercache.l2.revalidation.triggers` | Counter | `cache` | Refresh claims before executor submission (stale-while-revalidate and XFetch); rejected submission also increments failures. |
| `tiercache.l2.revalidation.completions` | Counter | `cache` | Revalidations that finished without error. |
| `tiercache.l2.revalidation.failures` | Counter | `cache` | Revalidations that failed; the stale entry keeps serving until its window ends. |

Rebuild-lock release failures are currently logged at DEBUG by
`io.tiercache.internal.DefaultTierCache`; they have no dedicated counter
and are not recorded as breaker failures. Cleanup preserves the loaded
value or original loader exception, and an unreleased Redis lock relies on
lease expiry. Enable that logger when diagnosing cleanup or lease delays;
metrics alone cannot identify these failures.

Health reading: a healthy cache shows a high `l1_hit` share, `dropped`
invalidations at zero, `tiercache.degraded` at `0`, and revalidation
completions tracking triggers.

## Tracing

`io.tiercache.micrometer.TiercacheTracing` implements the same listener SPI
and produces OpenTelemetry spans (tracer name `io.tiercache`):

- `tiercache.l2.<operation>` — one span per L2 operation, with attributes
  `cache.name`, `db.system=redis`, and `cache.hit` set at span end.
- `tiercache.invalidation.apply` — observes an already-committed inbound invalidation.
  Observer dispatch runs outside state monitors; this span does not measure
  time spent inside the L1 state commit.

There are deliberately **no spans on the L1-hit path** — core never calls
the tracing hooks there, so hot reads pay nothing. Wire it alongside the
metrics listener with your `OpenTelemetry` instance; with the Spring
starter, register it as a `CacheMetricsListener` bean (it takes precedence
over the auto-configured one, so combine both listeners yourself if you want
metrics and tracing together).

## JMX

`io.tiercache.micrometer.TiercacheInspection` exposes a JMX view at object
name `io.tiercache:type=Inspection` implementing
`TiercacheInspectionMXBean`:

| Attribute / operation | Type | Meaning |
|---|---|---|
| `CacheNames` | `String[]` | Configured cache names. |
| `getL1HitRatio(cache)` | `double` | L1 hits over all requests for the cache (misses and loads count as the non-L1 bucket). |
| `getL2HitRatio(cache)` | `double` | L2 hits over all requests for the cache. |
| `BreakerState` | `String` | `"closed"`, `"half_open"`, or `"open"` (open = L1-only degraded mode). |
| `getJournalSize(cache)` | `long` | Journal entries held for the cache; `-1` when no journal is wired. |

With Spring Boot, the starter auto-registers one `TiercacheInspection` bean
at `io.tiercache:type=Inspection` when the `tiercache-micrometer` module is
on the classpath (and unregisters it on shutdown) — nothing to wire. Outside
Spring, registration is programmatic: create one
`TiercacheInspection(registry, factory, journal, cacheNames)` per factory,
call `register()`, and `close()` to unregister. There is deliberately no
top-N-keys operation: per-key counting would tax the hot path, so key-level
inspection is refused by design.

### Ownership with multiple factories

The fixed name `io.tiercache:type=Inspection` exposes at most one inspection per
JVM; it does not aggregate multiple factories. The first successful registration
owns that name. Another inspection that finds it occupied is a no-op and cannot
unregister the owner when it closes. Repeating register on the owner preserves
ownership; repeating close cannot remove a later factory's registration.

Register and close are ordered per inspection. An explicit register after close
can establish a new ownership period if the name is free; a skipped factory does
not automatically take over when the name becomes vacant. An absent name during
cleanup is benign. Other unregister failures are logged and ownership is consumed,
so cleanup is not retried against a potential replacement. Such failures may
require operator cleanup of the fixed name.

This protects registrations managed through TiercacheInspection and pre-existing
foreign MBeans. JMX offers no atomic compare-and-unregister here: arbitrary external
replacement of an owned MBean by management tools is outside this ownership guarantee.
The ObjectName, attributes, operations and hit-ratio denominators are unchanged.

### Locale-independent metric identifiers

Enum-derived result, level and direction tags use Locale.ROOT, independently of
the JVM default locale. For example, `l1_hit`, `l2_hit`, `miss` and `received` keep
the same spelling under Turkish locale and after a locale change. Application cache
names retain their original spelling and case. Dashboards and JMX continue to use
the existing lowercase ASCII identifiers.

Restart affected processes with the fix if they previously emitted locale-specific
spellings. Already-created malformed meters are not renamed or deleted in place;
old time series can remain in the monitoring backend until normal retention removes
them. This does not add new meters or change their meaning.

## Grafana dashboard

`docs/grafana/tiercache-dashboard.json` is a self-contained dashboard
("Tiercache Overview") for a Prometheus datasource. Import it via Grafana →
Dashboards → Import → Upload JSON, then select your Prometheus datasource.
Panels:

| Panel | Query (Prometheus) |
|---|---|
| Request outcomes | `sum by (result) (rate(tiercache_requests_total[5m]))` |
| L2 latency (p99) | `histogram_quantile(0.99, sum by (le, cache) (rate(tiercache_latency_seconds_bucket{level="l2"}[5m])))` |
| Invalidation flow | `sum by (direction) (rate(tiercache_invalidation_total[5m]))` |
| Degraded | `max(tiercache_degraded)` |
| Breaker state | `max(tiercache_breaker_state)` |
| Journal size | `tiercache_journal_size` |
| Last load age (ms) | `tiercache_last_load_age` |
| Null entries | `sum by (cache) (rate(tiercache_null_entries_total[5m]))` |
| L2 stale hits | `sum by (cache) (rate(tiercache_l2_stale_hits_total[5m]))` |
| Revalidation triggers / completions / failures | Three separate queries: `rate(tiercache_l2_revalidation_triggers_total[5m])`, `rate(tiercache_l2_revalidation_completions_total[5m])`, `rate(tiercache_l2_revalidation_failures_total[5m])` |

Every query maps onto a meter in the [catalog above](#metrics-catalog).

## Alerts

`docs/grafana/alerts.yml` contains a Prometheus rule group (`tiercache`)
with five alerts:

| Alert | Severity | Fires when |
|---|---|---|
| `TiercacheMissGrowth` | warning | The 10-minute miss rate exceeds twice its value 30 minutes earlier, while the total request rate is below 1.2 times its earlier value; this is a signal to investigate, not a diagnosis. |
| `TiercacheDroppedInvalidations` | critical | A journal-backed L1 flush occurred in the last 5 minutes (`direction="dropped"`). Inspect logs to distinguish trimmed/unavailable history from a replay-read failure; the signal does not by itself prove messages were lost. |
| `TiercacheDegraded` | critical | `tiercache_degraded` has been above 0 for 5 minutes: the cache is running L1-only and cross-instance guarantees are degraded. |
| `TiercacheRevalidationFailures` | warning | Stale-while-revalidate the failure rate stays positive for 10 minutes; successful refreshes may coexist with failures. |
| `TiercachePublicationFailures` | warning | Publication failures persist for one minute. Inspect Redis connectivity and recovery; this is not proof that every failed command was undelivered. |

Load the file into your Prometheus `rule_files` or a compatible Prometheus-rule
provisioning system. Alertmanager routes notifications; it does not evaluate
these expressions.

See [triggered recovery](recovery.md) for HALF_OPEN admission, baseline failure, bounded retries and shutdown semantics.

Streams settlement and gap waits also contribute to the pending gauge. See [Streams diagnostics and PEL inspection](streams-recovery.md#diagnostics-and-operator-checks); Redis backlog is not bounded by the local batch size.

## Publication outcomes

`sent` keeps its legacy meaning as an attempted submission. Use
`tiercache.invalidation.publish` to distinguish what happened afterward:

- `acknowledged`: the actual Redis PUBLISH command completed successfully, even
  with zero subscribers. It is not acknowledgement by receiving cache instances.
- `failed`: submission/serialization threw, or the command completed exceptionally
  or was cancelled. Ambiguous failure does not prove Redis never executed it.
- `unconfirmed`: a legacy transport returned from void publish without an outcome
  API. The library cannot infer Redis acknowledgement from that return.
- `not_required`: Streams uses the durable journal row and issues no separate
  PUBLISH. This outcome does not certify an arbitrary caller's journal write.

Writes do not await publication completion or retry ambiguous messages. A publish
failure cannot replace an already successful data-write result. Oversized UPDATEs
still fall back to INVALIDATE and are not failures by themselves. Metrics and logs
do not repair a receiver that missed the last message: existing recovery triggers,
retention limits and conservative reset rules still apply.

Completion callbacks only update bounded internal accounting. One owned observer
worker exports batched terminal counts outside transport I/O threads and library
monitors. There is at most one pending token per registered cache, plus the fixed
`__tiercache_unregistered__` metrics bucket for direct, unregistered publications.
Reserve that label when naming caches for metrics. The observer keeps four outcome
accumulators and bounded failure-class summaries, not a list of messages or futures.
The existing SENT callback retains its submission-path timing; terminal counters
arrive asynchronously and need not match SENT in an instantaneous scrape.

Failure diagnostics contain cache and failure class, never serialized keys/payloads
or exception text, and are limited to one warning per bucket per 30 seconds.
Observer exceptions do not change the recorded publication outcome; a failing custom
metrics backend is not guaranteed complete exported counts and is not blindly retried.

Close stops admission and allows at most one second for queued observer work, then
stops notification. Already-admitted outcomes still settle internally; late completion
cannot restart workers or notify closed observers. An already-running arbitrary
listener may outlive the drain timeout. Export after backend shutdown is best effort.
A post-close write through a surviving synchronous view creates no publish attempt,
so it increments neither SENT nor a terminal publication counter.

For Pub/Sub, compare the failed rate to the acknowledged-plus-failed rate. Keep
unconfirmed and not-required series visible separately; they are capability/profile
outcomes, not network errors. The dashboard includes these series and a failure
fraction panel. Investigate failures alongside pending recovery and connection logs.


## Coverage and diagnosis boundaries

Request and activity counters are created as events occur, including for caches
created on demand. The standard starters register `journal.size`,
`last.load.age` and the JMX cache-name list for **configured** cache names; a
runtime-created name need not appear there. Programmatic wiring supplies that
list explicitly. Recovery-pending gauges follow active recovery registrations,
including runtime-created caches, and are removed when those registrations close.
Factory-level degraded/breaker gauges have no cache tag and do not aggregate
multiple independent factories with the same meter identity.

`last.load.age` is zero before a LOAD outcome has been observed; it is not an
entry freshness gauge. `null.entries` is cumulative stores, not current occupancy.
A revalidation completion does not by itself prove a new value was stored (a
coordinated refresh may skip). Revalidation failures do not cover all foreground
loader failures: inspect the application's error/latency signals as well.
Neither zero publication failures nor Redis acknowledgement proves every
subscriber applied every event. L1 hits have neither a timer nor a tracing span.

Use the [troubleshooting runbook](troubleshooting.md) with these counters and
logs. The sample dashboard's histogram query requires your registry to publish
histogram buckets; the binder does not enable them itself. The sample miss alert
uses its written rate/traffic expression, not a statistical proof of constant
traffic or a diagnosis of the underlying cause.
