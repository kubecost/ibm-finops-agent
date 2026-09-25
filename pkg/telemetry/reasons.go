// Package telemetry is the agent's reliability metrics, structured drop events and uploaded
// status summary (docs/reliability/FINDINGS.md chunk 09, docs/reliability/alerts.md).
//
// Every metric is named finops_agent_*, and every label takes its values from a closed set
// defined here, so no label ever holds a file name, node name, error string or URL. A value
// outside its set is recorded as "unknown" and logged once.
package telemetry

import (
	"slices"
	"sync"

	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/ibm/finops-agent/pkg/telemetry/dropevent"
	"github.com/opencost/opencost/core/pkg/log"
)

// Pipeline stages, the values of the emitter label on finops_agent_data_dropped_total and the
// per-emitter metrics.
const (
	EmitterCloudability = "cloudability"
	EmitterKubecost     = "kubecost"
	EmitterTurbonomic   = "turbonomic"
	// EmitterExporter is collection before any emitter: the cluster cache and the snapshot.
	EmitterExporter = "exporter"
)

// unknown replaces a label value outside its closed set.
const unknown = "unknown"

// Drop reasons: the closed enum of finops_agent_data_dropped_total{reason}. Each is raised by
// exactly one emitter (dropReasons). Summing the counter over every reason counts each lost item
// once.
const (
	// Cloudability emitter: sample writes and the disk budget (chunk 04).

	// ReasonDiskPressure: a queued sample or payload was evicted to make room on the scratch
	// volume.
	ReasonDiskPressure = "disk_pressure"
	// ReasonDiskPressureSkipped: a sample was not written because eviction couldn't free enough
	// space.
	ReasonDiskPressureSkipped = "disk_pressure_skipped"
	// ReasonShortLivedPodOverflow: short-lived pods were discarded past a buffer's cap (the
	// Cloudability emitter's pending pods, or the cluster cache's buffer for the exporter).
	ReasonShortLivedPodOverflow = "short_lived_pod_overflow"

	// Cloudability startup recovery and payloads (chunk 01).

	// ReasonRecoveryExpired: a sample or payload found at startup was older than
	// CLOUDABILITY_RECOVERY_PERIOD.
	ReasonRecoveryExpired = "recovery_expired"
	// ReasonClusterIDMismatch: a queued payload belongs to another cluster ID. Quarantined.
	ReasonClusterIDMismatch = "cluster_id_mismatch"
	// ReasonCorruptPayload: a payload failed its end-to-end read before upload. Quarantined.
	ReasonCorruptPayload = "corrupt_payload"
	// ReasonInvalidSample: a sample directory that is neither staging nor a valid finalised
	// sample. Quarantined.
	ReasonInvalidSample = "invalid_sample"
	// ReasonInvalidPayload: a file in the upload queue that isn't a valid payload name.
	// Quarantined.
	ReasonInvalidPayload = "invalid_payload"

	// Cloudability upload queue (chunk 02).

	// ReasonRejectedByBackend: the backend refused a payload for good (400 or 413). Quarantined.
	ReasonRejectedByBackend = "rejected_by_backend"
	// ReasonUndeliverable: a payload kept failing at the head of the queue while others were
	// delivered. Quarantined.
	ReasonUndeliverable = "undeliverable"
	// ReasonNoUploader: queued data was evicted while no storage service was configured.
	ReasonNoUploader = "no_uploader"
	// ReasonBacklogBytes: the oldest queued data was evicted to keep the backlog within
	// CLOUDABILITY_BACKLOG_MAX_MB.
	ReasonBacklogBytes = "backlog_bytes"
	// ReasonBacklogAge: queued data older than the recovery period was evicted.
	ReasonBacklogAge = "backlog_age"

	// Exporter (chunk 05).

	// ReasonSnapshotFailed: short-lived pods drained from the cluster cache by a snapshot that
	// then failed.
	ReasonSnapshotFailed = emitter.DropReasonSnapshotFailed
	// ReasonSnapshotAbandoned: short-lived pods in a snapshot that completed but was never
	// emitted: it finished after a newer one, or after the exporter stopped.
	ReasonSnapshotAbandoned = emitter.DropReasonSnapshotAbandoned

	// Kubecost (chunk 07).

	// ReasonExportRejectedAfterStop: a Kubecost export write refused because the emitter had
	// stopped before it drained.
	ReasonExportRejectedAfterStop = "export_rejected_after_stop"
)

// ReasonQuarantineEvicted is not a data_dropped_total reason. It marks a quarantined item removed
// to keep the quarantine within its bounds. The item was already counted as dropped when it was
// quarantined, so it is counted in finops_agent_cldy_quarantine_evicted_total instead, and summing
// data_dropped_total over its reasons doesn't count it twice.
const ReasonQuarantineEvicted = dropevent.QuarantineEvicted

// dropReasons maps each drop reason to the emitters that raise it.
var dropReasons = map[string][]string{
	ReasonDiskPressure:            {EmitterCloudability},
	ReasonDiskPressureSkipped:     {EmitterCloudability},
	ReasonShortLivedPodOverflow:   {EmitterCloudability, EmitterExporter},
	ReasonRecoveryExpired:         {EmitterCloudability},
	ReasonClusterIDMismatch:       {EmitterCloudability},
	ReasonCorruptPayload:          {EmitterCloudability},
	ReasonInvalidSample:           {EmitterCloudability},
	ReasonInvalidPayload:          {EmitterCloudability},
	ReasonRejectedByBackend:       {EmitterCloudability},
	ReasonUndeliverable:           {EmitterCloudability},
	ReasonNoUploader:              {EmitterCloudability},
	ReasonBacklogBytes:            {EmitterCloudability},
	ReasonBacklogAge:              {EmitterCloudability},
	ReasonSnapshotFailed:          {EmitterExporter},
	ReasonSnapshotAbandoned:       {EmitterExporter},
	ReasonExportRejectedAfterStop: {EmitterKubecost},
}

// DropReasons returns every drop reason, sorted.
func DropReasons() []string {
	out := make([]string, 0, len(dropReasons))
	for r := range dropReasons {
		out = append(out, r)
	}
	slices.Sort(out)
	return out
}

// DropReasonEmitters returns the emitters that raise reason, or nil if it isn't a drop reason.
func DropReasonEmitters(reason string) []string {
	return slices.Clone(dropReasons[reason])
}

// validDrop reports whether emitter raises reason.
func validDrop(emitter, reason string) bool {
	return slices.Contains(dropReasons[reason], emitter)
}

// Upload attempt results for finops_agent_cldy_upload_attempts_total{result}.
const (
	UploadResultOK        = "ok"
	UploadResultRetryable = "retryable"
	UploadResultTimeout   = "timeout"
	UploadResultAuth      = "auth"
	UploadResultRejected  = "rejected"
)

var uploadResults = []string{UploadResultOK, UploadResultRetryable, UploadResultTimeout, UploadResultAuth, UploadResultRejected}

// Cloudability storage services, the service label.
const (
	ServiceFrontdoor        = "frontdoor"
	ServiceMetricsCollector = "metrics_collector"
	ServiceCustomS3         = "custom_s3"
	ServiceCustomAzureBlob  = "custom_azure_blob"
)

var uploadServices = []string{ServiceFrontdoor, ServiceMetricsCollector, ServiceCustomS3, ServiceCustomAzureBlob}

// Emit results for finops_agent_emit_total{result}: one per Init or Emit call the exporter makes,
// or skips because the emitter's previous call is still running.
const (
	EmitResultOK      = emitter.EmitResultOK
	EmitResultError   = emitter.EmitResultError
	EmitResultSkipped = emitter.EmitResultSkipped
)

var emitResults = []string{EmitResultOK, EmitResultError, EmitResultSkipped}

// Startup recovery (chunk 01), finops_agent_cldy_recovery_items_total{kind,outcome}.
const (
	RecoveryKindSample  = "sample"
	RecoveryKindPayload = "payload"

	// RecoveryRecovered: queued for upload.
	RecoveryRecovered = "recovered"
	// RecoveryQuarantined: moved to the quarantine, and counted as a drop.
	RecoveryQuarantined = "quarantined"
	// RecoveryDropped: removed as expired, and counted as a drop.
	RecoveryDropped = "dropped"
	// RecoveryDiscardedUnfinalized: an unfinished sample or payload removed; not a drop.
	RecoveryDiscardedUnfinalized = "discarded_unfinalized"
)

var (
	recoveryKinds    = []string{RecoveryKindSample, RecoveryKindPayload}
	recoveryOutcomes = []string{RecoveryRecovered, RecoveryQuarantined, RecoveryDropped, RecoveryDiscardedUnfinalized}
)

// Snapshot components (chunk 06), finops_agent_snapshot_component_failures_total{component}.
var snapshotComponents = []string{"cluster_info", "kubernetes", "node_stats", "metrics"}

// Kubernetes resources, finops_agent_snapshot_object_conversion_failures_total{resource}.
var conversionResources = []string{
	"nodes", "pods", "namespaces", "services", "deployments", "daemonsets", "statefulsets", "replicasets",
	"jobs", "cronjobs", "persistentvolumes", "persistentvolumeclaims", "replicationcontrollers",
	"storageclasses", "poddisruptionbudgets", "resourcequotas",
}

// Metrics windows (chunk 06), finops_agent_window_gaps_total{resolution,reason}.
var (
	windowResolutions = []string{"10m", "1h", "1d"}
	// WindowGapBackfillLimit: closed windows older than the backfill limit were never queried.
	WindowGapBackfillLimit = "backfill_limit"
	windowGapReasons       = []string{WindowGapBackfillLimit}
)

// closed returns v if it is in set, and otherwise "unknown", logging the first time each label
// sees a value outside its set.
func closed(label, v string, set []string) string {
	if slices.Contains(set, v) {
		return v
	}
	warnUnknown(label, v)
	return unknown
}

// maxUnknownLogged bounds the unknown label values remembered for logging once (I7).
const maxUnknownLogged = 64

var (
	unknownMu   sync.Mutex
	unknownSeen = map[string]struct{}{} // label+"="+value
)

// warnUnknown logs, once per label and value, that a value outside the label's closed set was
// recorded as unknown. The value is logged but never used as a label. Past maxUnknownLogged
// distinct values it stops logging.
func warnUnknown(label, v string) {
	key := label + "=" + v
	unknownMu.Lock()
	_, seen := unknownSeen[key]
	full := len(unknownSeen) >= maxUnknownLogged
	if !seen && !full {
		unknownSeen[key] = struct{}{}
	}
	unknownMu.Unlock()
	if !seen && !full {
		log.Errorf("telemetry: %q is not a known value of the %s label; recording it as %q", v, label, unknown)
	}
}
