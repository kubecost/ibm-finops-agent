package telemetry

import (
	"time"

	"github.com/ibm/finops-agent/pkg/emitter"
)

// CloudabilitySink records the Cloudability emitter's and uploader's events. It implements
// cldy.EventSink; pass it as cldy.UploaderConfig.Events.
type CloudabilitySink struct{ m *Metrics }

// Cloudability returns the sink for the Cloudability emitter and uploader.
func (m *Metrics) Cloudability() CloudabilitySink {
	return CloudabilitySink{m: m}
}

// DataDropped counts count items lost for reason. quarantine_evicted is counted apart
// (ReasonQuarantineEvicted).
func (s CloudabilitySink) DataDropped(reason string, count int) {
	if count <= 0 {
		return
	}
	if reason == ReasonQuarantineEvicted {
		s.m.quarantineEvicted.Add(float64(count))
		return
	}
	if !validDrop(EmitterCloudability, reason) {
		warnUnknown("reason", reason)
		reason = unknown
	}
	s.m.dataDropped.add(float64(count), EmitterCloudability, reason)
}

func (s CloudabilitySink) UnfinalizedDiscarded(count int) {
	if count > 0 {
		s.m.unfinalizedDiscarded.Add(float64(count))
	}
}

// EmitResult is not recorded here: finops_agent_emit_total counts the exporter's calls, for every
// emitter alike (ExporterObserver).
func (s CloudabilitySink) EmitResult(string) {}

func (s CloudabilitySink) EmissionSlotsSkipped(count int) {
	if count > 0 {
		s.m.emissionSlotsSkipped.Add(float64(count))
	}
}

// SetCondition is not recorded here: finops_agent_health_condition reads every component's
// conditions from the health registry.
func (s CloudabilitySink) SetCondition(string, bool) {}

func (s CloudabilitySink) UploadAttempt(service, result string) {
	s.m.uploadAttempts.WithLabelValues(closed("service", service, uploadServices), closed("result", result, uploadResults)).Inc()
}

func (s CloudabilitySink) UploadDuration(service string, d time.Duration) {
	s.m.uploadDuration.WithLabelValues(closed("service", service, uploadServices)).Observe(d.Seconds())
}

func (s CloudabilitySink) HeadDeferred(count int) {
	if count > 0 {
		s.m.headDeferred.Add(float64(count))
	}
}

func (s CloudabilitySink) RecoveryItem(kind, outcome string, count int) {
	if count > 0 {
		s.m.recoveryItems.WithLabelValues(closed("kind", kind, recoveryKinds), closed("outcome", outcome, recoveryOutcomes)).Add(float64(count))
	}
}

// ExporterObserver records the exporter's snapshots and emitter calls. It implements
// emitter.ExporterObserver; pass it as emitter.ExporterConfig.Observer.
type ExporterObserver struct{ m *Metrics }

// Exporter returns the observer for the exporter.
func (m *Metrics) Exporter() ExporterObserver {
	return ExporterObserver{m: m}
}

func (o ExporterObserver) SnapshotDuration(d time.Duration) {
	o.m.snapshotDuration.Observe(d.Seconds())
}

func (o ExporterObserver) EmitterCall(id emitter.EmitterID, result string) {
	o.m.emitTotal.WithLabelValues(EmitterName(id), closed("result", result, emitResults)).Inc()
}

func (o ExporterObserver) SnapshotComponentFailed(component string) {
	o.m.SnapshotComponentFailed(component)
}
