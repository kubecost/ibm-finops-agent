package telemetry

import (
	"context"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// SummaryKey is the key the status summary is sent under: a top-level field of Cloudability's
// agent-measurement.json and a Kubecost heartbeat metadata key.
const SummaryKey = "agent_health"

// SummarySchemaVersion is bumped when a field of Summary changes meaning. Fields are only added.
const SummarySchemaVersion = 1

// Summary is the agent's health as IBM receives it (I6): the same in Cloudability's
// agent-measurement.json and in the Kubecost heartbeat. It only reaches IBM while uploads work,
// so the backend must also alert on a missing heartbeat or measurement (docs/reliability/alerts.md).
//
// Counters count since CountersSinceTS, the agent's start; a restart resets them.
type Summary struct {
	SchemaVersion int `json:"schema_version"`
	// Ready and Phase are the agent's readiness and startup phase (/readyz).
	Ready            bool               `json:"ready"`
	Phase            string             `json:"phase,omitempty"`
	ActiveConditions []SummaryCondition `json:"active_conditions"`
	// DataDroppedTotal is every item lost; DataDropped splits it by emitter and reason (only
	// those with drops).
	DataDroppedTotal       uint64                       `json:"data_dropped_total"`
	DataDropped            map[string]map[string]uint64 `json:"data_dropped,omitempty"`
	QuarantineEvictedTotal uint64                       `json:"quarantine_evicted_total"`
	// WindowGapsTotal is left out until something measures window gaps (chunk 06), so a
	// missing measurement is never read as "no gaps".
	WindowGapsTotal           *uint64 `json:"window_gaps_total,omitempty"`
	EmissionSlotsSkippedTotal uint64  `json:"emission_slots_skipped_total"`
	// Cloudability upload queue at the end of the last upload cycle, and when a payload was last
	// delivered (Unix seconds, 0 if none since the start).
	BacklogFiles        int   `json:"backlog_files"`
	BacklogBytes        int64 `json:"backlog_bytes"`
	LastUploadSuccessTS int64 `json:"last_upload_success_ts"`
	CountersSinceTS     int64 `json:"counters_since_ts"`
}

// SummaryCondition is one active condition in the Summary.
type SummaryCondition struct {
	Component string `json:"component"`
	Type      string `json:"type"`
	Reason    string `json:"reason,omitempty"`
	SinceTS   int64  `json:"since_ts"`
}

// Summary returns the agent's current health summary. Readiness and conditions come from the
// health registry (SetHealthRegistry), which it checks, bounded by the registry's check timeout;
// without one the summary is ready with no conditions.
func (m *Metrics) Summary() Summary {
	m.mu.Lock()
	registry, uploadStatus, windowGapsSet := m.registry, m.uploadStatus, m.windowGapsSet
	m.mu.Unlock()

	s := Summary{
		SchemaVersion:    SummarySchemaVersion,
		Ready:            true,
		ActiveConditions: []SummaryCondition{},
		CountersSinceTS:  m.start.Unix(),
	}
	if registry != nil {
		result := registry.Check(context.Background())
		s.Ready, s.Phase = result.Ready, result.Phase
		for _, c := range result.Components {
			for _, cond := range c.Conditions {
				s.ActiveConditions = append(s.ActiveConditions, SummaryCondition{
					Component: c.Name, Type: cond.Type, Reason: cond.Reason, SinceTS: unixOrZero(cond.Since),
				})
			}
		}
	}

	values, _ := m.dataDropped.totals()
	for key, v := range values {
		if v <= 0 {
			continue
		}
		labels := strings.Split(key, labelSep)
		if s.DataDropped == nil {
			s.DataDropped = map[string]map[string]uint64{}
		}
		if s.DataDropped[labels[0]] == nil {
			s.DataDropped[labels[0]] = map[string]uint64{}
		}
		s.DataDropped[labels[0]][labels[1]] += uint64(v)
		s.DataDroppedTotal += uint64(v)
	}
	if windowGapsSet {
		gaps, _ := m.windowGaps.totals()
		var total uint64
		for _, v := range gaps {
			total += uint64(v)
		}
		s.WindowGapsTotal = &total
	}
	s.QuarantineEvictedTotal = counterValue(m.quarantineEvicted)
	s.EmissionSlotsSkippedTotal = counterValue(m.emissionSlotsSkipped)

	if uploadStatus != nil {
		u := uploadStatus()
		s.BacklogFiles, s.BacklogBytes = u.BacklogFiles, u.BacklogBytes
		s.LastUploadSuccessTS = unixOrZero(u.LastSuccess)
	}
	return s
}

func unixOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

func counterValue(c prometheus.Counter) uint64 {
	var m dto.Metric
	if err := c.Write(&m); err != nil || m.Counter == nil {
		return 0
	}
	return uint64(m.Counter.GetValue())
}

// HeartbeatMetadata is a Kubecost heartbeat metadata provider (OpenCost's
// HeartbeatMetadataProvider) that sends the status summary under SummaryKey.
type HeartbeatMetadata struct{ m *Metrics }

// HeartbeatMetadata returns the Kubecost heartbeat metadata provider.
func (m *Metrics) HeartbeatMetadata() HeartbeatMetadata {
	return HeartbeatMetadata{m: m}
}

// GetMetadata implements HeartbeatMetadataProvider.
func (h HeartbeatMetadata) GetMetadata() map[string]any {
	return map[string]any{SummaryKey: h.m.Summary()}
}
