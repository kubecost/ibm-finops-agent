package cldy

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
)

// EventSink receives the Cloudability emitter's reliability events: data drops, unfinalised
// discards, emit outcomes, skipped emission slots and condition changes. The emitter logs each
// event itself; a sink only records it. The default sink is an *EventCounts. Chunk 09 plugs in
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
}

// EventCounts is an EventSink that keeps counts in memory.
type EventCounts struct {
	mu                   sync.Mutex
	dropped              map[string]int
	unfinalizedDiscarded int
	emitResults          map[string]int
	emissionSlotsSkipped int
	conditions           map[string]bool
}

// NewEventCounts returns an empty EventCounts.
func NewEventCounts() *EventCounts {
	return &EventCounts{
		dropped:     map[string]int{},
		emitResults: map[string]int{},
		conditions:  map[string]bool{},
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

// EventCountsSnapshot is a point-in-time copy of an EventCounts.
type EventCountsSnapshot struct {
	Dropped              map[string]int
	UnfinalizedDiscarded int
	EmitResults          map[string]int
	EmissionSlotsSkipped int
	Conditions           map[string]bool
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
	}
	for k, v := range c.dropped {
		s.Dropped[k] = v
	}
	for k, v := range c.emitResults {
		s.EmitResults[k] = v
	}
	for k, v := range c.conditions {
		s.Conditions[k] = v
	}
	return s
}
