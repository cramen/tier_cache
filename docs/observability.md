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
| `tiercache.invalidation` | Counter | `cache`, `direction` | Invalidation events by direction: `sent`, `received`, `replayed` (from the journal on recovery), `dropped` (a journal-backed L1 flush because replay history could not be verified, for example after trimming or a read failure). The latter records fallback events, not a count of individually lost messages. |
| `tiercache.degraded` | Gauge | — | `1` while the L2 circuit breaker is open (L1-only mode), else `0`. |
| `tiercache.breaker.state` | Gauge | — | Breaker machine state: `0` = closed, `1` = half-open (recovery probing), `2` = open. During half-open `tiercache.degraded` is already back at `0`. |
| `tiercache.journal.size` | Gauge | `cache` | Invalidation journal entries currently held for the cache. |
| `tiercache.last.load.age` | Gauge | `cache` | Milliseconds since the last load event for the cache, tracked from store events, not per-entry metadata. |
| `tiercache.null.entries` | Counter | `cache` | Null-markers stored under the `allow` null policy. |
| `tiercache.l2.stale.hits` | Counter | `cache` | L2 entries served stale (past logical TTL, inside the stale window). |
| `tiercache.l2.revalidation.triggers` | Counter | `cache` | Asynchronous revalidations claimed and submitted (stale-while-revalidate and XFetch). |
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
- `tiercache.invalidation.apply` — wraps inbound invalidation processing.

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

## Grafana dashboard

`docs/grafana/tiercache-dashboard.json` is a self-contained dashboard
("Tiercache Overview") for a Prometheus datasource. Import it via Grafana →
Dashboards → Import → Upload JSON, then select your Prometheus datasource.
Panels:

| Panel | Query (Prometheus) |
|---|---|
| Request outcomes | `sum by (result) (rate(tiercache_requests_total[$5m]))` |
| L2 latency (p99) | `histogram_quantile(0.99, sum by (le, cache) (rate(tiercache_latency_seconds_bucket{level="l2"}[$5m])))` |
| Invalidation flow | `sum by (direction) (rate(tiercache_invalidation_total[$5m]))` |
| Degraded | `max(tiercache_degraded)` |
| Breaker state | `max(tiercache_breaker_state)` |
| Journal size | `tiercache_journal_size` |
| Last load age (ms) | `tiercache_last_load_age` |
| Null entries | `sum by (cache) (rate(tiercache_null_entries_total[$5m]))` |
| L2 stale hits | `sum by (cache) (rate(tiercache_l2_stale_hits_total[$5m]))` |
| Revalidation triggers / completions / failures | `sum by (cache) (rate(tiercache_l2_revalidation_{triggers,completions,failures}_total[$5m]))` |

Every query maps onto a meter in the [catalog above](#metrics-catalog).

## Alerts

`docs/grafana/alerts.yml` contains a Prometheus rule group (`tiercache`)
with four alerts:

| Alert | Severity | Fires when |
|---|---|---|
| `TiercacheMissGrowth` | warning | The miss rate more than doubled over 30 minutes while total traffic stayed flat (a hot-key or eviction problem, not a traffic spike). |
| `TiercacheDroppedInvalidations` | critical | A journal-backed L1 flush occurred in the last 5 minutes (`direction="dropped"`). Inspect logs to distinguish trimmed/unavailable history from a replay-read failure; the signal does not by itself prove messages were lost. |
| `TiercacheDegraded` | critical | `tiercache_degraded` has been above 0 for 5 minutes: the cache is running L1-only and cross-instance guarantees are degraded. |
| `TiercacheRevalidationFailures` | warning | Stale-while-revalidate revalidations have been failing for over 10 minutes: stale entries are served but never refreshed. |

Load the file into your Prometheus `rule_files` (or drop it into an
Alertmanager/Grafana-managed rule provisioning directory).
