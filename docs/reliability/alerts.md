# IBM FinOps Agent: metrics, alerts and runbooks

This is the reference for the agent's reliability metrics, the alert rules built on them, and
what to do when each alert fires. It implements chunk 09 of `docs/reliability/FINDINGS.md`.

The agent has two audiences with different views:

- **The cluster operator** sees `/metrics`, `/readyz`, `/status`, the pod's readiness and restarts,
  and the logs. The alerts below are for them.
- **IBM** sees only what the agent uploads: `agent-measurement.json` in every Cloudability payload
  and the Kubecost heartbeat. Both carry the same `agent_health` summary (see
  [What IBM receives](#what-ibm-receives)). A customer's Prometheus is invisible to IBM.

## How the health signals fit together

| Signal | Fails for | Effect |
|---|---|---|
| Liveness (`/healthz`) | A local wedge a restart can fix: a stalled exporter or upload loop, a stuck call, a hung health check | kubelet restarts the pod |
| Readiness (`/readyz`) | Any active condition: remote, credential, disk, RBAC or config faults, repeated snapshot or emitter failures | Pod is NotReady. The agent serves no traffic, so this costs nothing and is purely a signal |
| `/status` | Nothing: it is JSON of every component's conditions and progress | For humans and support bundles |
| Metrics | Nothing: they count and measure | The alerts below |

A remote outage (Cloudability, the bucket, the API server) never fails liveness: a restart would
not fix it. It shows up as NotReady, as `finops_agent_health_condition`, and as the alerts below.

## Metrics

Every metric is on the agent's `/metrics` (port 9003), next to OpenCost's. Every name starts with
`finops_agent_`. Every label takes its values from a closed set defined in `pkg/telemetry`; no
label ever holds a file name, node name, error string or URL. A value outside its set is recorded
as `unknown` and logged once at Error.

Counters restart from zero when the agent restarts; use `increase()` or `rate()`. Series with
closed label sets are exported at 0 from the start, so `increase()` sees their first increment.

### Collection and the exporter

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `exporter_cycle_last_end_timestamp_seconds` | gauge | | When the last snapshot-and-emit cycle ended; the agent's start until one has |
| `exporter_cycle_overruns_total` | counter | | Ticks skipped because a cycle took longer than the interval |
| `exporter_snapshot_timeouts_total` | counter | | Cycles with no snapshot within the snapshot deadline |
| `exporter_emit_timeouts_total` | counter | | Init or Emit calls that outlived the emit deadline |
| `snapshot_duration_seconds` | histogram | | Duration of each snapshot, successful or not |
| `snapshot_component_failures_total` | counter | `component` (`cluster_info`, `kubernetes`, `node_stats`, `metrics`) | Failed snapshot components (from chunk 06) |
| `snapshot_object_conversion_failures_total` | counter | `resource` (Kubernetes resource kinds) | Objects left out of a snapshot because they failed typed conversion (from chunk 06) |
| `node_stats_duration_seconds` | histogram | | Duration of each node-stats collection across all nodes |
| `window_gaps_total` | counter | `resolution` (`10m`, `1h`, `1d`), `reason` (`backfill_limit`) | Closed metrics windows never queried (from chunk 06) |
| `emit_total` | counter | `emitter` (`cloudability`, `kubecost`, `turbonomic`), `result` (`ok`, `error`, `skipped`) | Init and Emit calls per cycle. `skipped`: the previous call was still running. A call past its deadline counts once, as `error` |
| `emitter_ready` | gauge | `emitter` | 1 when the emitter has initialised and its last 3 cycles haven't all failed |
| `emission_slots_skipped_total` | counter | | Cloudability emission slots skipped after a stall. Not a loss: the next sample covers their usage |

### Cloudability upload

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `cldy_upload_attempts_total` | counter | `service` (`frontdoor`, `metrics_collector`, `custom_s3`, `custom_azure_blob`), `result` (`ok`, `retryable`, `timeout`, `auth`, `rejected`) | Payload upload attempts |
| `cldy_upload_duration_seconds` | histogram | `service` | Duration of each upload attempt |
| `cldy_upload_last_success_timestamp_seconds` | gauge | | When a payload was last delivered; the agent's start until one has been |
| `cldy_upload_backlog_files` | gauge | | Payloads and finalised samples queued, at the end of the last upload cycle |
| `cldy_upload_backlog_bytes` | gauge | | Their size |
| `cldy_upload_head_deferred_total` | counter | | Payloads moved behind the rest of the queue after failing at its head |
| `cldy_recovery_items_total` | counter | `kind` (`sample`, `payload`), `outcome` (`recovered`, `quarantined`, `dropped`, `discarded_unfinalized`) | What startup recovery found |
| `cldy_unfinalized_discarded_total` | counter | | Unfinished samples or payloads removed. Not a loss: their data is in a finalised sample or still in scratch |
| `cldy_quarantine_evicted_total` | counter | | Quarantined items removed to bound the quarantine. See [below](#quarantine-evictions-are-not-counted-twice) |

### Loss and health

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `data_dropped_total` | counter | `emitter` (`cloudability`, `exporter`, `kubecost`), `reason` | Collected data lost. Summed over every series it is the total loss, each item counted once |
| `health_condition` | gauge | `component`, `type` | The number of active conditions of that type; 0 once cleared |

### Kubecost export (from chunk 07)

Exported once chunk 07's counters are wired in (see `telemetry.Metrics.SetKubecostExport` and
`SetWAL`).

Totals and failures are separate counters (a failure rate is
`rate(..._failures_total) / rate(..._total)`).

| Metric | Type | Labels |
|---|---|---|
| `kubecost_export_writes_total` | counter | |
| `kubecost_export_write_failures_total` | counter | |
| `kubecost_export_rejected_after_stop_total` | counter | |
| `kubecost_bucket_canary_runs_total` | counter | |
| `kubecost_bucket_canary_failures_total` | counter | |
| `kubecost_bucket_canary_last_success_timestamp_seconds` | gauge | |
| `kubecost_forced_snapshot_swaps_total` | counter | |
| `collector_wal_writes_total` | counter | |
| `collector_wal_write_failures_total` | counter | |
| `collector_wal_store_attempts_total` | counter | |
| `collector_wal_store_failures_total` | counter | |
| `collector_wal_restore_errors_total` | counter | |

`kubecost_export_last_success_timestamp_seconds{pipeline,resolution}` waits for OpenCost's
`ExportController` status API (U-3).

### Drop reasons

`reason` on `finops_agent_data_dropped_total` is one closed enum, defined in
`pkg/telemetry/reasons.go`. Every drop is also logged once at Error in one structured form:

```
event=data_dropped emitter=cloudability reason=backlog_age count=3 bytes=524288 window_start=2026-09-22T10:00:00Z window_end=2026-09-22T10:20:00Z: <detail>
```

`bytes` and the window are left out when unknown. When one cycle evicts many items, they are
logged once per reason with the total count and bytes and the time range they covered; each item
is also logged at Info.

| Reason | Emitter | What was lost | Usual cause |
|---|---|---|---|
| `disk_pressure` | cloudability | A queued sample or payload evicted to make room on the scratch volume | Scratch volume too small for the backlog, usually during an upload outage |
| `disk_pressure_skipped` | cloudability | A sample not written, because eviction couldn't free enough space | As above, with nothing left to evict |
| `short_lived_pod_overflow` | cloudability, exporter | Short-lived pods past a buffer's cap (the emitter's pending pods, or the cluster cache's buffer) | Snapshots or samples failing for a long time on a busy cluster |
| `recovery_expired` | cloudability | A sample or payload found at startup older than `CLOUDABILITY_RECOVERY_PERIOD` (72h) | The agent was down, or uploads failed, for longer than the period |
| `cluster_id_mismatch` | cloudability | A queued payload of another cluster ID (quarantined) | The scratch volume moved between clusters, or the `default` namespace was recreated |
| `corrupt_payload` | cloudability | A payload that failed its end-to-end read (quarantined) | Disk corruption |
| `invalid_sample` | cloudability | A sample directory that isn't a valid finalised sample (quarantined) | A torn write, or files left by an agent from before the manifest |
| `invalid_payload` | cloudability | A file in `upload/` that isn't a payload (quarantined) | Something else wrote to the volume |
| `rejected_by_backend` | cloudability | A payload the backend refused for good, 400 or 413 (quarantined) | A payload too large, or malformed |
| `undeliverable` | cloudability | A payload that kept failing at the head of the queue while others were delivered (quarantined) | A payload-specific fault the backend reports as retryable |
| `no_uploader` | cloudability | Queued data evicted while no storage service was configured | Upload settings missing or wrong |
| `backlog_bytes` | cloudability | The oldest queued data, past `CLOUDABILITY_BACKLOG_MAX_MB` | A long upload outage |
| `backlog_age` | cloudability | Queued data older than the recovery period | A long upload outage |
| `snapshot_failed` | exporter | Short-lived pods drained by a snapshot that then failed | Snapshot failures |
| `snapshot_abandoned` | exporter | Short-lived pods in a snapshot completed but never emitted | A snapshot finished after a newer one, or after shutdown began |
| `export_rejected_after_stop` | kubecost | A Kubecost export refused because the emitter had stopped | Shutdown didn't drain in time |

#### Quarantine evictions are not counted twice

A quarantined item is counted in `data_dropped_total` when it is quarantined (under its reason:
`invalid_sample`, `rejected_by_backend` and so on). When the quarantine later exceeds its bounds
(100 MiB or 7 days) and the item is removed, that is not a second loss, so it is counted in
`finops_agent_cldy_quarantine_evicted_total` and logged at Warn with `event=quarantine_evicted`,
not in `data_dropped_total`. Summing `data_dropped_total` over its reasons counts every lost item
once.

## What IBM receives

Both upload channels carry the same summary, `agent_health` (`telemetry.Summary`):

- **Cloudability**: a new top-level `agent_health` object in `agent-measurement.json`, in every
  sample. Every existing field is unchanged (`cldy/agent_measurement_test.go` checks this against
  a golden file of the old output).
- **Kubecost**: an `agent_health` key in the heartbeat's `metadata`, next to OpenCost's cluster
  info and log level.

```json
"agent_health": {
  "schema_version": 1,
  "ready": false,
  "phase": "running",
  "active_conditions": [{"component": "cldy-emitter", "type": "uploads_failing", "since_ts": 1790334000}],
  "data_dropped_total": 5,
  "data_dropped": {"cloudability": {"backlog_age": 3}, "exporter": {"snapshot_failed": 2}},
  "quarantine_evicted_total": 1,
  "window_gaps_total": 4,
  "emission_slots_skipped_total": 6,
  "backlog_files": 7,
  "backlog_bytes": 8192,
  "last_upload_success_ts": 1790334900,
  "counters_since_ts": 1790272800
}
```

Timestamps are Unix seconds; `last_upload_success_ts` is 0 until a payload has been delivered
since the start. Counters count since `counters_since_ts`, the agent's start, and reset when it
restarts. Condition messages are never sent: only component, type and reason.

**The summary only reaches IBM while uploads work.** An agent that can't upload, has stopped, or
has been uninstalled sends nothing, so the backend must alert on absence: no Cloudability sample
for a cluster within a few upload intervals (uploads run every 10 minutes), and no Kubecost
heartbeat within a few heartbeat intervals (5 minutes by default). Absence is the only signal for
a total outage.

Before relying on the new field, confirm with Cloudability ingestion and the Kubecost heartbeat
consumer that they ignore unknown fields (see the PR's open questions).

## Alert rules

A Prometheus Operator `PrometheusRule`. Adjust the `namespace` and `pod` selectors to your
install: the chart's default release in the e2e tests is namespace `ibm-finops-agent`, pods
labelled `app.kubernetes.io/name=finops-agent`. The kube-state-metrics names are those of KSM v2
(`kube_pod_status_ready`, `kube_pod_container_status_restarts_total`,
`kube_pod_container_status_last_terminated_reason`); check them against the KSM version your
cluster runs.

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: finops-agent
spec:
  groups:
    - name: finops-agent
      rules:
        - alert: FinOpsAgentUploadStale
          expr: |
            (time() - finops_agent_cldy_upload_last_success_timestamp_seconds > 1800)
            and on (namespace, pod) (finops_agent_cldy_upload_backlog_files > 0)
          for: 15m
          labels: {severity: warning}
          annotations:
            summary: "FinOps agent {{ $labels.pod }} has not delivered a Cloudability payload for 30 minutes with a backlog"
            runbook: "docs/reliability/alerts.md#finopsagentuploadstale"
        - alert: FinOpsAgentDataDropped
          expr: sum by (namespace, pod, emitter, reason) (increase(finops_agent_data_dropped_total[15m])) > 0
          labels: {severity: warning}
          annotations:
            summary: "FinOps agent {{ $labels.pod }} lost {{ $value }} items ({{ $labels.emitter }}, {{ $labels.reason }})"
            runbook: "docs/reliability/alerts.md#finopsagentdatadropped"
        - alert: FinOpsAgentExporterStalled
          expr: time() - finops_agent_exporter_cycle_last_end_timestamp_seconds > 300
          for: 5m
          labels: {severity: critical}
          annotations:
            summary: "FinOps agent {{ $labels.pod }} has not finished a collection cycle for over 5 minutes"
            runbook: "docs/reliability/alerts.md#finopsagentexporterstalled"
        - alert: FinOpsAgentNotReady
          expr: kube_pod_status_ready{namespace="ibm-finops-agent", pod=~".*finops-agent.*", condition="true"} == 0
          for: 15m
          labels: {severity: warning}
          annotations:
            summary: "FinOps agent {{ $labels.pod }} has been NotReady for 15 minutes"
            runbook: "docs/reliability/alerts.md#finopsagentnotready"
        - alert: FinOpsAgentRestarting
          expr: increase(kube_pod_container_status_restarts_total{namespace="ibm-finops-agent", pod=~".*finops-agent.*"}[1h]) > 2
          labels: {severity: warning}
          annotations:
            summary: "FinOps agent {{ $labels.pod }} restarted {{ $value }} times in an hour"
            runbook: "docs/reliability/alerts.md#finopsagentrestarting"
        - alert: FinOpsAgentOOMKilled
          expr: |
            kube_pod_container_status_last_terminated_reason{namespace="ibm-finops-agent", pod=~".*finops-agent.*", reason="OOMKilled"} == 1
            and on (namespace, pod, container)
            increase(kube_pod_container_status_restarts_total{namespace="ibm-finops-agent", pod=~".*finops-agent.*"}[30m]) > 0
          labels: {severity: warning}
          annotations:
            summary: "FinOps agent {{ $labels.pod }} was OOM-killed"
            runbook: "docs/reliability/alerts.md#finopsagentoomkilled"
```

The chart doesn't ship these rules yet; adding them as an optional template, off by default, is a
chart follow-up.

## Runbooks

Start every investigation with the pod's `/status` (JSON of every component's conditions and
progress) and `/readyz` (one line per reason the agent isn't ready):

```sh
kubectl -n ibm-finops-agent port-forward deploy/<agent> 9003 &
curl -s localhost:9003/status | jq
curl -s localhost:9003/readyz
```

### FinOpsAgentUploadStale

**Means**: no Cloudability payload has been delivered for 30 minutes while data is queued. Data
is safe on the scratch volume until the backlog bounds (`CLOUDABILITY_BACKLOG_MAX_MB`, 2 GiB) or
the recovery period (72h) evict it, and each eviction is counted as a drop.

**Check**:
1. `/status`, component `cldy-emitter`: its conditions say why.
   - `upload_auth_failed`: the backend refused the agent's credentials (401/403). Check the API
     key or the Frontdoor key pair and environment ID.
   - `upload_rejected`: the backend rejected several payloads in a row (400/413). Probably a
     backend fault; nothing is quarantined while it lasts. Contact IBM support.
   - `uploader_unconfigured` / `uploader_misconfigured`: no usable upload settings. Fix the
     `CLOUDABILITY_*` settings.
   - `upload_connectivity_failed`, `uploads_failing`: network. Check egress to the upload
     endpoints, proxy settings (`CLOUDABILITY_OUTBOUND_PROXY`) and DNS.
2. `rate(finops_agent_cldy_upload_attempts_total[15m])` by `result`: `timeout` and `retryable`
   point at the network, `auth` at credentials, `rejected` at the payloads or the backend.
3. The logs, filtered on `Cloudability`.

**Don't** restart the pod for this: a restart doesn't fix a remote fault. Restarts no longer lose
the queue, but they don't help either.

### FinOpsAgentDataDropped

**Means**: collected data was lost and will never reach IBM. The `reason` label says why; see the
[drop reasons](#drop-reasons) table for what each means.

**Check**:
1. The Error log line `event=data_dropped ... reason=<reason>` gives the count, bytes, the time
   range lost and the detail.
2. By reason:
   - `disk_pressure`, `disk_pressure_skipped`: the scratch volume is full. Usually a symptom of an
     upload outage (see FinOpsAgentUploadStale); otherwise give the volume more space.
   - `backlog_bytes`, `backlog_age`, `recovery_expired`: uploads failed, or the agent was down,
     for too long. Fix the upload fault; consider a larger `CLOUDABILITY_BACKLOG_MAX_MB`.
   - `no_uploader`: fix the upload configuration.
   - `rejected_by_backend`, `undeliverable`, `corrupt_payload`, `invalid_sample`,
     `invalid_payload`, `cluster_id_mismatch`: the item is in `<scratch>/scratch/_quarantine/` for
     7 days. Keep it for IBM support, who can tell whether it can be re-sent.
   - `short_lived_pod_overflow`, `snapshot_failed`, `snapshot_abandoned`: snapshots are failing
     or slow. See FinOpsAgentExporterStalled and the `snapshot_failing` condition.
   - `export_rejected_after_stop`: the agent was stopped before Kubecost exports drained. Check the
     pod's termination grace period.
3. Tell IBM support the time range lost, so they know the gap in the customer's data is real.

### FinOpsAgentExporterStalled

**Means**: no collection cycle has finished for over 5 minutes, so nothing new is collected for
either product. If the loop is truly wedged, liveness fails and the kubelet restarts the pod on
its own; this alert catches the time before that, and a loop that is slow rather than stuck.

**Check**:
1. `/status`, component `exporter`: `lastCycleStart`, `lastCycleEnd`, `lastSnapshotError`, and
   which emitters are `busy`.
2. `finops_agent_snapshot_duration_seconds` and `finops_agent_node_stats_duration_seconds`: a slow
   snapshot is usually node stats (unreachable kubelets) or the metrics source.
3. `finops_agent_exporter_snapshot_timeouts_total`, `finops_agent_exporter_emit_timeouts_total`:
   which deadline is being hit.
4. If the pod was restarted by liveness, the previous container's log
   (`kubectl logs --previous`) says which loop stalled (`health: component ... is not live`).

### FinOpsAgentNotReady

**Means**: the agent has an active degraded condition, or is still starting up. NotReady has no
functional cost (the agent serves no traffic); it is the cluster-side signal that something
needs attention.

**Check**: `curl localhost:9003/readyz` lists every reason, and `finops_agent_health_condition > 0`
shows them in Prometheus. By condition:

| Condition | Component | Action |
|---|---|---|
| `startup: phase data_source` | | Informers syncing or the collector WAL replaying. Wait; if it persists, see `informers_unsynced` |
| `informers_unsynced` | informers | The agent's service account lacks list/watch on the resources named. Fix the RBAC |
| `node_stats_stale` | node_stats | Kubelets unreachable. Check network policies and the node-stats settings |
| `snapshot_failing` | exporter | Three snapshots in a row failed; the message has the error |
| `emitter_uninitialised`, `emitter_failing` | exporter | The emitter named in the reason can't start or keeps failing; the message has the error |
| `uploads_failing`, `upload_*`, `uploader_*` | cldy-emitter | See FinOpsAgentUploadStale |
| `disk_pressure`, `disk_space_unknown` | cldy-emitter | The scratch volume is full, or its free space can't be read |
| `cldy_emitter_uninitialised` | cldy-emitter | No cluster ID yet: the `default` namespace isn't visible to the agent |
| `region_fallback` | cldy-emitter | `CLOUDABILITY_UPLOAD_REGION` isn't a known region; data is going to the US endpoints |
| `bucket_unavailable`, `wal_*` | kubecost-emitter | The Kubecost export bucket can't be written or read. Check the bucket config and credentials |

### FinOpsAgentRestarting

**Means**: the pod restarted more than twice in an hour. Restarts come from liveness (a local
wedge), OOM, or crashes.

**Check**:
1. `kubectl describe pod`: the last state's reason (`OOMKilled`, `Error`) and the liveness probe
   events.
2. `kubectl logs --previous`: `health: component <name> is not live: <reason>` names the stalled
   loop; a panic or `Fatal` line names a crash.
3. A restart no longer loses queued Cloudability data: startup recovery re-queues it
   (`finops_agent_cldy_recovery_items_total`). A restart loop still means no new data is
   collected.

### FinOpsAgentOOMKilled

**Means**: the agent exceeded its memory limit and was killed.

**Check**: memory grows with cluster size (objects in the snapshot) and with degraded paths
(retries, large backlogs). Compare `container_memory_working_set_bytes` with the limit over the
day before the kill, and raise the limit. If memory grows without bound, collect a heap profile
(with `PPROF_ENABLED`) and send it to IBM support.
