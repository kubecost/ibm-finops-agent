# Kubecost export semantics

How the agent's Kubecost exports behave, what can go wrong with them, and what the agent reports.
Traced against OpenCost `1aaf85ffd875`, the pinned version. Finding numbers (F-xx) and upstream
items (U-x) refer to `docs/reliability/FINDINGS.md`. Re-check this page when the OpenCost module is
bumped.

## Data path

1. The exporter snapshots the cluster every emission interval (1 min by default) and calls the
   Kubecost emitter's `Emit`, which publishes the snapshot to the OpenCost adapters
   (`kubecost/adapters`).
2. The adapters keep the current and previous window for each metrics resolution
   (`MaxBackfillSnapshots = 2`).
3. OpenCost's export controllers compute each pipeline (allocation, assets, network insights,
   KubeModel) from the adapters and write one object per window to the bucket. Heartbeat and
   diagnostics controllers write events to the same bucket.

## When a window is exported

- The pipeline controllers tick every **10 min** (`ALLOCATION_EXPORT_INTERVAL` and friends,
  `kubecost/config.go`). The first tick comes a full interval after `Init`.
- Resolutions are hourly and daily. 10m is added per pipeline when `MINUTE_METRICS_ENABLED` is set
  (`kubecost/emitter.go`).
- Every tick exports the in-progress window. The previous window is exported as well until one
  export succeeds after the rollover, which moves `lastExport` past it (`controller.go`
  `exportWindowsFor`).
  - If the previous window fails and the current one succeeds in the same tick, the previous
    window is never retried (F-29). A 10m window can be skipped outright (F-42). U-1 fixes both
    upstream. It isn't pinned yet.
  - The first tick after rollover lands 0 to 10 min after the window closes. The tail is missing
    only if the tick runs before the next 1-min snapshot (about 10% of rollovers, up to about
    1 min of tail). Nothing re-exports it later (F-30; needs U-4).
  - After a restart, `lastExport` is zero, so the window that was open at the crash is never
    exported again. Its object keeps the last export from before the crash: up to 10 min of tail
    plus the downtime is lost (F-30, restart case).

## What an export writes

- The exporter checks `Exists`, then overwrites the object with any non-empty set
  (`exporter.go`, `validator.go`). An empty set never overwrites.
- A query error aborts the window and nothing is written. `NoDataError` proceeds with an empty
  set, which then doesn't overwrite (`controller.go`, `allocation.go`).
- Delivery is effectively **last writer wins per window**: the latest successful export of a
  window is what the backend sees.

## Consistency of a computation (F-19)

- `computeAllocation` runs two query groups (`buildPodMap`'s, then the main group). With
  `BatchDuration()` = 730h there is a single batch.
- The agent previously updated its four adapters under four locks, so a computation could mix
  snapshot N and N+1 across adapters or across the two query groups. That self-heals for the
  in-progress window. At rollover it is permanent for the closed window.
- **Now:** the four adapters share one immutable state, and `Update` replaces it as a unit.
  Every computation runs pinned (`OpenCostDataSourceAdapter.Pin`): a snapshot published during a
  computation is held back until the last pinned computation finishes. `Update` never blocks. A
  pin held for more than 5 min is treated as hung and the snapshot is published anyway, counted
  in `ForcedSnapshotSwapsTotal`. This also closes the query-group tear for this agent. U-2 is
  still worth doing for other OpenCost consumers.

## The collector's WAL and restarts (F-20, F-34, F-41)

- In collector mode with a bucket configured, OpenCost wraps the metric repository in a
  write-ahead log (the Walinator). Every scrape (every 30 s) is written to the bucket. At startup
  the WAL replays synchronously, so the rebuilt windows are complete. **This is the only thing
  that stops a restart from overwriting the day's in-progress windows with post-restart data.**
- Without a working WAL, the first export after a restart overwrites the current hourly and daily
  objects with post-restart data only.
- **Now:**
  - The WAL's bucket store is retried with capped backoff for up to 2 min before the collector
    starts. A config file that can't be read, or that doesn't parse as a supported bucket
    config, fails at once instead of delaying every emitter by the whole budget. If it still can't be built, the collector starts without a WAL and `wal_unavailable`
    is raised. The Kubecost emitter's `Init` then refuses to start the export controllers
    (the exporter retries it every cycle). The WAL can't be attached to a running collector, so
    background retries only switch the condition's reason to `restart_required` once the bucket
    works again.
  - If the store was built but the collector didn't start the WAL (`NewWalinator` failed, which
    OpenCost only logs), `wal_unavailable` is raised with reason `not_started`.
  - A bucket List or Read error while the WAL replays raises `wal_restore_failed`. Exports still
    run: holding them back would lose every later window too.
  - A failed WAL write raises `wal_write_failing` until the next write succeeds. Scrapes in that
    gap aren't in the WAL, so they are lost at the next restart.
  - Not detected: a WAL object that reads but fails to deserialise. The Walinator only logs it.
    U-3 would report it.
- WAL writes are synchronous in the scrape goroutine, so a slow bucket skips scrapes (F-51, U-7).

## Export health

- **Bucket canary.** Every `BUCKET_CANARY_INTERVAL` (default 10m; 0 disables) the emitter writes,
  reads back and deletes `<cluster name>/write-test/canary-<pod name>.txt`. That is one object
  per pod, under the `write-test/` prefix `ValidateConfig` already uses, so pods that overlap in a
  rolling update don't interfere. A failure or timeout raises `bucket_unavailable`, and the next success clears
  it. That is 3 small requests per cluster per interval (432 a day at the default). Until OpenCost
  reports export failures itself (U-3), this is the signal that exports are failing.
- **Write counters.** Every export write (pipelines, heartbeat, diagnostics) is counted, including
  failures (`KubecostEmitter.Status()`).
- **Conditions.** `KubecostEmitter.Conditions()` returns `bucket_unavailable` plus the collector
  WAL's conditions. Chunk 08 registers this one method; there's no need to register the WAL
  separately.

## Shutdown

`KubecostEmitter.Stop(ctx)`:

1. Stops every controller and the canary.
2. Refuses new computations.
3. Waits until no computation, existence check or write has been in flight for 250 ms. Writes
   stay allowed while it waits, so a window whose computation just finished still gets written.
   OpenCost exports a closed window only once.
4. If `ctx` ends first, refuses writes from then on, counting each one in
   `WritesRejectedAfterStopTotal` with an Error log.

Encoding a set between the existence check and the write isn't tracked. If it runs longer than
the quiet period, that write can still happen after `Stop` returns.

`main` calls it after the exporter stops, with a 10 s bound. The OpenCost controllers have no way
to wait for their loops (U-6), which is why the emitter tracks activity itself.

## Retention and retries

The adapters hold only the current and previous window per resolution. When U-1 is pinned,
retries of older closed windows will fail in `QueryPods` until they're dropped. Either keep U-1's
retries within the two held windows or raise `MaxBackfillSnapshots` to U-1's pending bound (chunk
06, design 3).

## WAL restore cost (F-33): how to measure it

The numbers aren't measured yet. They need a representative bucket. Method:

1. Run the agent in collector mode against a real bucket (S3, and ideally Azure and GCS) with the
   default retention (36 × 10m, 49 × 1h, 15 × 1d) for the full 15 days. At a 30 s scrape interval
   the WAL then holds about 43,200 objects.
2. Restart the pod with `LOG_LEVEL=debug`, `pprof` enabled and `GODEBUG=gctrace=1`.
3. Time the restore by wrapping the `construct` call in `openWAL` (`pkg/core/opencost/wal.go`)
   with a timer and logging the result. The call replays the WAL synchronously.
4. Take peak heap from `/debug/pprof/heap` and the gctrace output during restore, and peak RSS
   from the container metrics.
5. Take the bucket request count and cost from the provider's request metrics: one List plus one
   GET per object.
6. Repeat at 100, 500 and 2,000 nodes. Feed the p99 restore time into chunk 08's startup-probe
   budget and the peak heap into chunk 11's memory baseline.

## Upstream follow-ups

| ID | Item |
|---|---|
| U-1 | Retry failed closed windows (F-29, F-42). In progress; bump the agent once it merges. |
| U-2 | Put `computeAllocation`'s queries in one query group (F-19). The agent's pinning covers this agent only. |
| U-3 | Status for `ExportController` and the Walinator: last success, last error, failures, dropped windows, restore errors (F-34, F-35). |
| U-4 | A finality signal so a closed window is re-exported once its data is final (F-30). |
| U-6 | Stop APIs that wait for the controller and collector loops (F-16). |
| U-7 | Asynchronous WAL writes with a bounded queue (F-51). |
| — | F-30 restart case: seed the previous window on the first tick after a restart, gated on WAL health. |
