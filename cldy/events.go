package cldy

import (
	"maps"
	"sync"
	"time"

	"github.com/ibm/finops-agent/pkg/telemetry"
)

// Drop reasons for finops_agent_data_dropped_total{reason} raised by this package. The closed
// enum, with what each means, is in pkg/telemetry.
const (
	// Sample writes and the disk budget (chunk 04).
	dropReasonDiskPressure          = telemetry.ReasonDiskPressure
	dropReasonDiskPressureSkipped   = telemetry.ReasonDiskPressureSkipped
	dropReasonShortLivedPodOverflow = telemetry.ReasonShortLivedPodOverflow

	// Startup recovery and payload handling (chunk 01).
	dropReasonRecoveryExpired   = telemetry.ReasonRecoveryExpired
	dropReasonClusterIDMismatch = telemetry.ReasonClusterIDMismatch
	dropReasonCorruptPayload    = telemetry.ReasonCorruptPayload
	dropReasonInvalidSample     = telemetry.ReasonInvalidSample
	dropReasonInvalidPayload    = telemetry.ReasonInvalidPayload
	// dropReasonQuarantineEvicted: a quarantined item was removed to keep the quarantine within
	// its size and age bounds. It was counted as dropped when it was quarantined, so sinks count
	// it apart from the other reasons (telemetry.ReasonQuarantineEvicted).
	dropReasonQuarantineEvicted = telemetry.ReasonQuarantineEvicted

	// The upload queue (chunk 02).
	dropReasonRejectedByBackend = telemetry.ReasonRejectedByBackend
	dropReasonUndeliverable     = telemetry.ReasonUndeliverable
	dropReasonNoUploader        = telemetry.ReasonNoUploader
	dropReasonBacklogBytes      = telemetry.ReasonBacklogBytes
	dropReasonBacklogAge        = telemetry.ReasonBacklogAge
)

// Emit results the emitter records in its EventCounts. finops_agent_emit_total counts the
// exporter's Init and Emit calls instead, for every emitter alike.
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
)

// Upload attempt results for finops_agent_cldy_upload_attempts_total{result}.
const (
	uploadResultOK        = telemetry.UploadResultOK
	uploadResultRetryable = telemetry.UploadResultRetryable
	uploadResultTimeout   = telemetry.UploadResultTimeout
	uploadResultAuth      = telemetry.UploadResultAuth
	uploadResultRejected  = telemetry.UploadResultRejected
)

// EventSink receives the Cloudability emitter's and uploader's reliability events: data drops,
// unfinalised discards, emit outcomes, skipped emission slots, condition changes and upload
// attempts. The emitter and uploader log each event themselves; a sink only records it. The
// uploader always keeps an *EventCounts, and also sends every event to UploaderConfig.Events when
// it is set: telemetry.CloudabilitySink in production.
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
	// UploadAttempt records the outcome of one payload upload attempt to a storage service (a
	// telemetry.Service* value) as an uploadResult* value.
	UploadAttempt(service, result string)
	// UploadDuration records how long one payload upload attempt to a storage service took.
	UploadDuration(service string, d time.Duration)
	// RecoveryItem records count samples or payloads found by startup recovery (kind and outcome
	// are telemetry.Recovery* values).
	RecoveryItem(kind, outcome string, count int)
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
	recoveryItems        map[string]int
}

// NewEventCounts returns an empty EventCounts.
func NewEventCounts() *EventCounts {
	return &EventCounts{
		dropped:        map[string]int{},
		emitResults:    map[string]int{},
		conditions:     map[string]bool{},
		uploadAttempts: map[string]int{},
		recoveryItems:  map[string]int{},
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

// UploadAttempt counts attempts by result, whatever the service.
func (c *EventCounts) UploadAttempt(_, result string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.uploadAttempts[result]++
}

func (c *EventCounts) UploadDuration(string, time.Duration) {}

// RecoveryItem counts recovery items by "<kind>/<outcome>".
func (c *EventCounts) RecoveryItem(kind, outcome string, count int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recoveryItems[kind+"/"+outcome] += count
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
	// RecoveryItems is keyed by "<kind>/<outcome>".
	RecoveryItems map[string]int
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
		RecoveryItems:        map[string]int{},
	}
	maps.Copy(s.Dropped, c.dropped)
	maps.Copy(s.EmitResults, c.emitResults)
	maps.Copy(s.Conditions, c.conditions)
	maps.Copy(s.UploadAttempts, c.uploadAttempts)
	maps.Copy(s.RecoveryItems, c.recoveryItems)
	return s
}

// newEventSinks returns the EventCounts every emitter and uploader keeps, and the sink to send
// events to: the counts, and extra too when it is set.
func newEventSinks(extra EventSink) (*EventCounts, EventSink) {
	counts := NewEventCounts()
	if extra == nil {
		return counts, counts
	}
	return counts, teeSink{counts, extra}
}

// teeSink sends every event to each of its sinks.
type teeSink []EventSink

func (t teeSink) DataDropped(reason string, count int) {
	for _, s := range t {
		s.DataDropped(reason, count)
	}
}

func (t teeSink) UnfinalizedDiscarded(count int) {
	for _, s := range t {
		s.UnfinalizedDiscarded(count)
	}
}

func (t teeSink) EmitResult(result string) {
	for _, s := range t {
		s.EmitResult(result)
	}
}

func (t teeSink) EmissionSlotsSkipped(count int) {
	for _, s := range t {
		s.EmissionSlotsSkipped(count)
	}
}

func (t teeSink) SetCondition(name string, active bool) {
	for _, s := range t {
		s.SetCondition(name, active)
	}
}

func (t teeSink) UploadAttempt(service, result string) {
	for _, s := range t {
		s.UploadAttempt(service, result)
	}
}

func (t teeSink) UploadDuration(service string, d time.Duration) {
	for _, s := range t {
		s.UploadDuration(service, d)
	}
}

func (t teeSink) HeadDeferred(count int) {
	for _, s := range t {
		s.HeadDeferred(count)
	}
}

func (t teeSink) RecoveryItem(kind, outcome string, count int) {
	for _, s := range t {
		s.RecoveryItem(kind, outcome, count)
	}
}
