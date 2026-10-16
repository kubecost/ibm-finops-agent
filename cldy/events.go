package cldy

import "maps"

import "sync"

// Drop reasons for finops_agent_data_dropped_total{reason} raised by this package. Chunk 09
// gathers every reason into one closed enum.
const (
	// dropReasonDiskPressure: a finalised sample was evicted to make room for a new one.
	dropReasonDiskPressure = "disk_pressure"
	// dropReasonDiskPressureSkipped: a sample was not written because eviction couldn't free
	// enough space.
	dropReasonDiskPressureSkipped = "disk_pressure_skipped"
	// dropReasonShortLivedPodOverflow: the oldest pending short-lived pods were discarded because
	// more than maxPendingShortLivedPods were waiting for a sample.
	dropReasonShortLivedPodOverflow = "short_lived_pod_overflow"

	// Startup recovery and payload handling (chunk 01).

	// dropReasonRecoveryExpired: a finalised sample or payload found at startup was older than
	// the recovery period (CLOUDABILITY_RECOVERY_PERIOD).
	dropReasonRecoveryExpired = "recovery_expired"
	// dropReasonClusterIDMismatch: a payload recovered at startup belongs to a cluster ID other
	// than the live one. It is quarantined.
	dropReasonClusterIDMismatch = "cluster_id_mismatch"
	// dropReasonCorruptPayload: a payload failed its end-to-end read just before upload. It is
	// quarantined.
	dropReasonCorruptPayload = "corrupt_payload"
	// dropReasonInvalidSample: a directory in scratch/<clusterID>/ that is neither staging nor a
	// valid finalised sample (written before the manifest existed, or torn). It is quarantined.
	dropReasonInvalidSample = "invalid_sample"
	// dropReasonInvalidPayload: a file in upload/ whose name is not <clusterID>_<timestamp>.tgz,
	// or has an empty cluster ID. It is quarantined.
	dropReasonInvalidPayload = "invalid_payload"
	// dropReasonQuarantineEvicted: a quarantined item was removed to keep the quarantine within
	// its size and age bounds.
	dropReasonQuarantineEvicted = "quarantine_evicted"

	// The upload queue (chunk 02).

	// dropReasonRejectedByBackend: the backend refused a payload for good (400 or 413). It is
	// quarantined.
	dropReasonRejectedByBackend = "rejected_by_backend"
	// dropReasonUndeliverable: a payload kept failing at the head of the queue after it had
	// already been moved behind the rest once, while other payloads were delivered. It is
	// quarantined.
	dropReasonUndeliverable = "undeliverable"
	// dropReasonNoUploader: queued data was evicted to make room while no storage service was
	// configured, so it could never have been uploaded.
	dropReasonNoUploader = "no_uploader"
	// dropReasonBacklogBytes: the oldest queued data was evicted to keep the backlog within
	// CLOUDABILITY_BACKLOG_MAX_MB.
	dropReasonBacklogBytes = "backlog_bytes"
	// dropReasonBacklogAge: queued data older than the recovery period was evicted.
	dropReasonBacklogAge = "backlog_age"
)

// Emit results for finops_agent_emit_total{result}.
const (
	emitResultOK      = "ok"
	emitResultError   = "error"
	emitResultSkipped = "skipped"
)

// Conditions the Cloudability emitter raises. Chunk 08 folds them into readiness and status.
const (
	// conditionDiskPressure: the scratch volume can't hold the next sample without evicting.
	conditionDiskPressure = "disk_pressure"
	// conditionDiskSpaceUnknown: the free space on the scratch volume can't be read (statfs failed).
	conditionDiskSpaceUnknown = "disk_space_unknown"
	// conditionUninitialised: Init has not succeeded yet, so nothing is written.
	conditionUninitialised = "cldy_emitter_uninitialised"

	// conditionUploaderUnconfigured: no storage service is configured, so nothing is uploaded and
	// the backlog is kept until the disk budget evicts it.
	conditionUploaderUnconfigured = "uploader_unconfigured"
	// conditionUploaderMisconfigured: an upload path is selected but its settings or credentials
	// are incomplete, so its storage service could not be built.
	conditionUploaderMisconfigured = "uploader_misconfigured"
	// conditionUploadConnectivityFailed: the startup connectivity test failed. It is advisory:
	// the service is still used, and the condition clears on the first delivered payload.
	conditionUploadConnectivityFailed = "upload_connectivity_failed"
	// conditionUploadAuthFailed: the backend refused the agent's credentials (401 or 403 on login
	// or presign). It clears on the next delivered payload.
	conditionUploadAuthFailed = "upload_auth_failed"
	// conditionUploadsRejected: the backend rejected several payloads in a row (400 or 413), so
	// the fault is taken to be the backend's and nothing is quarantined. It clears on the next
	// delivered payload.
	conditionUploadsRejected = "upload_rejected"
	// conditionRegionFallback: CLOUDABILITY_UPLOAD_REGION is not a known region, so uploads go to the
	// US endpoints (D3).
	conditionRegionFallback = "region_fallback"
)

// Upload attempt results for finops_agent_cldy_upload_attempts_total{result}.
const (
	uploadResultOK        = "ok"
	uploadResultRetryable = "retryable"
	uploadResultTimeout   = "timeout"
	uploadResultAuth      = "auth"
	uploadResultRejected  = "rejected"
)

// EventSink receives the Cloudability emitter's and uploader's reliability events: data drops,
// unfinalised discards, emit outcomes, skipped emission slots, condition changes and upload
// attempts. The emitter and uploader log each event themselves; a sink only records it. The default sink is an *EventCounts. Chunk 09 plugs in
// one backed by Prometheus.
type EventSink interface {
	// DataDropped records count items of collected data lost for reason (a dropReason* value).
	DataDropped(reason string, count int)
	// UnfinalizedDiscarded records count staging directories removed before they were
	// finalised. This is not a drop: no finalised sample was lost.
	UnfinalizedDiscarded(count int)
	// EmitResult records the outcome of an emitting Emit call (an emitResult* value).
	EmitResult(result string)
	// EmissionSlotsSkipped records count emission slots skipped after a stall.
	EmissionSlotsSkipped(count int)
	// SetCondition records whether the named condition (a condition* value) is active.
	SetCondition(name string, active bool)
	// UploadAttempt records the outcome of one payload upload attempt (an uploadResult* value).
	UploadAttempt(result string)
	// HeadDeferred records count payloads moved behind the rest of the upload queue by the
	// head-of-line rule.
	HeadDeferred(count int)
}

// EventCounts is an EventSink that keeps counts in memory.
type EventCounts struct {
	mu                   sync.Mutex
	dropped              map[string]int
	unfinalizedDiscarded int
	emitResults          map[string]int
	emissionSlotsSkipped int
	conditions           map[string]bool
	uploadAttempts       map[string]int
	headDeferred         int
}

// NewEventCounts returns an empty EventCounts.
func NewEventCounts() *EventCounts {
	return &EventCounts{
		dropped:        map[string]int{},
		emitResults:    map[string]int{},
		conditions:     map[string]bool{},
		uploadAttempts: map[string]int{},
	}
}

func (c *EventCounts) DataDropped(reason string, count int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dropped[reason] += count
}

func (c *EventCounts) UnfinalizedDiscarded(count int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.unfinalizedDiscarded += count
}

func (c *EventCounts) EmitResult(result string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.emitResults[result]++
}

func (c *EventCounts) EmissionSlotsSkipped(count int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.emissionSlotsSkipped += count
}

func (c *EventCounts) SetCondition(name string, active bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conditions[name] = active
}

func (c *EventCounts) UploadAttempt(result string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.uploadAttempts[result]++
}

func (c *EventCounts) HeadDeferred(count int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.headDeferred += count
}

// EventCountsSnapshot is a point-in-time copy of an EventCounts.
type EventCountsSnapshot struct {
	Dropped              map[string]int
	UnfinalizedDiscarded int
	EmitResults          map[string]int
	EmissionSlotsSkipped int
	Conditions           map[string]bool
	UploadAttempts       map[string]int
	HeadDeferred         int
}

// Snapshot returns a copy of the current counts.
func (c *EventCounts) Snapshot() EventCountsSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := EventCountsSnapshot{
		Dropped:              map[string]int{},
		UnfinalizedDiscarded: c.unfinalizedDiscarded,
		EmitResults:          map[string]int{},
		EmissionSlotsSkipped: c.emissionSlotsSkipped,
		Conditions:           map[string]bool{},
		UploadAttempts:       map[string]int{},
		HeadDeferred:         c.headDeferred,
	}
	maps.Copy(s.Dropped, c.dropped)
	maps.Copy(s.EmitResults, c.emitResults)
	maps.Copy(s.Conditions, c.conditions)
	maps.Copy(s.UploadAttempts, c.uploadAttempts)
	return s
}
