package emitter

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ibm/finops-agent/pkg/condition"
	"github.com/ibm/finops-agent/pkg/health"
)

// Conditions the exporter's health component raises.
const (
	// ConditionSnapshotFailing: the last NotReadyAfterFailures snapshots failed. Emitters get
	// nothing until one succeeds.
	ConditionSnapshotFailing = "snapshot_failing"
	// ConditionEmitterUninitialised: an enabled emitter has not had a successful Init, so it
	// emits nothing. The reason is the emitter ID.
	ConditionEmitterUninitialised = "emitter_uninitialised"
	// ConditionEmitterFailing: an emitter's last NotReadyAfterFailures cycles failed. The reason
	// is the emitter ID.
	ConditionEmitterFailing = "emitter_failing"
	// ConditionExporterStopped: the exporter loop isn't running.
	ConditionExporterStopped = "exporter_stopped"
)

// NotReadyAfterFailures is how many consecutive snapshot, or emitter, failures make the
// exporter not ready. One or two failures in a row are routine (a slow source, a rollover).
const NotReadyAfterFailures = 3

// HealthComponent returns the exporter's health component.
//
// It is not live only while the loop is Stalled: a cycle overran its deadlines, no cycle started
// when one was due, or an Init or Emit call is stuck StuckCallCycles intervals past its deadline.
// A stuck call has ignored its context's cancellation for that long, so it is a local wedge
// (a deadlock, or a call with no deadline of its own on a dead connection) that nothing but a
// restart reclaims; its emitter is skipped until it returns. A remote outage makes calls fail or
// return at their deadline, which is not a stall. Snapshot and emitter failures, whatever their
// cause, only make the exporter not ready (I4, F-38).
func HealthComponent(hb Heartbeat) health.Component {
	return health.ComponentFunc(func(_ context.Context, now time.Time) health.Report {
		status := hb.Status()
		report := health.Report{Live: true, Status: newExporterHealthStatus(status)}
		if hb.Stalled(now) {
			report.Live = false
			report.NotLiveReason = stalledReason(status, now)
		}
		report.Conditions = exporterConditions(status)
		return report
	})
}

func exporterConditions(status ExporterStatus) []condition.Condition {
	var conditions []condition.Condition
	if !status.Running {
		conditions = append(conditions, condition.Condition{
			Type: ConditionExporterStopped, Message: "the exporter loop is not running",
		})
	}
	if status.ConsecutiveSnapshotFailures >= NotReadyAfterFailures {
		conditions = append(conditions, condition.Condition{
			Type:   ConditionSnapshotFailing,
			Reason: "consecutive_failures",
			Message: fmt.Sprintf("%d snapshots in a row failed, so nothing is emitted; last error: %s",
				status.ConsecutiveSnapshotFailures, errorText(status.LastSnapshotError)),
		})
	}
	for _, es := range status.Emitters {
		reason := emitterReason(es.ID)
		if es.State != EmitterReady {
			msg := fmt.Sprintf("%s has not initialised, so it emits nothing", es.ID)
			if es.LastInitError != nil {
				msg += "; last error: " + errorText(es.LastInitError)
			}
			conditions = append(conditions, condition.Condition{Type: ConditionEmitterUninitialised, Reason: reason, Message: msg})
			continue
		}
		if es.ConsecutiveFailures >= NotReadyAfterFailures {
			conditions = append(conditions, condition.Condition{
				Type:   ConditionEmitterFailing,
				Reason: reason,
				Message: fmt.Sprintf("%s failed %d cycles in a row; last error: %s",
					es.ID, es.ConsecutiveFailures, errorText(es.LastEmitError)),
			})
		}
	}
	return conditions
}

// stalledReason describes a stall. Stalled is true when a cycle overran its deadlines, no cycle
// started when one was due, or an Init or Emit call is stuck past its deadline.
func stalledReason(status ExporterStatus, now time.Time) string {
	var busy []string
	for _, es := range status.Emitters {
		if es.Busy {
			busy = append(busy, string(es.ID))
		}
	}
	reason := fmt.Sprintf("exporter stalled: last cycle started %s and ended %s (interval %s, snapshot deadline %s, emit deadline %s)",
		since(now, status.LastCycleStart), since(now, status.LastCycleEnd), status.Interval, status.SnapshotTimeout, status.EmitTimeout)
	if len(busy) > 0 {
		reason += "; emitters with a call running: " + strings.Join(busy, ", ")
	}
	return reason
}

func since(now, t time.Time) string {
	if t.IsZero() {
		return "never"
	}
	return now.Sub(t).Round(time.Millisecond).String() + " ago"
}

// emitterReason turns an emitter ID into a snake_case condition reason.
func emitterReason(id EmitterID) string {
	return strings.ReplaceAll(string(id), "-", "_")
}

// errorText is err's message with URL query strings redacted, or "" for nil. Error text can
// carry presigned URLs.
func errorText(err error) string {
	if err == nil {
		return ""
	}
	return condition.Redact(err.Error())
}

// exporterHealthStatus is the exporter's progress in /status.
type exporterHealthStatus struct {
	Running                     bool                  `json:"running"`
	Interval                    string                `json:"interval"`
	LastCycleStart              time.Time             `json:"lastCycleStart"`
	LastCycleEnd                time.Time             `json:"lastCycleEnd"`
	LastSnapshotSuccess         time.Time             `json:"lastSnapshotSuccess"`
	LastSnapshotError           string                `json:"lastSnapshotError,omitempty"`
	ConsecutiveSnapshotFailures int                   `json:"consecutiveSnapshotFailures"`
	CyclesTotal                 uint64                `json:"cyclesTotal"`
	CycleOverrunsTotal          uint64                `json:"cycleOverrunsTotal"`
	SnapshotTimeoutsTotal       uint64                `json:"snapshotTimeoutsTotal"`
	EmitTimeoutsTotal           uint64                `json:"emitTimeoutsTotal"`
	Emitters                    []emitterHealthStatus `json:"emitters"`
}

type emitterHealthStatus struct {
	ID                  EmitterID    `json:"id"`
	State               EmitterState `json:"state"`
	Busy                bool         `json:"busy"`
	LastEmitSuccess     time.Time    `json:"lastEmitSuccess"`
	LastError           string       `json:"lastError,omitempty"`
	LastErrorTime       time.Time    `json:"lastErrorTime"`
	ConsecutiveFailures int          `json:"consecutiveFailures"`
}

func newExporterHealthStatus(s ExporterStatus) exporterHealthStatus {
	out := exporterHealthStatus{
		Running:                     s.Running,
		Interval:                    s.Interval.String(),
		LastCycleStart:              s.LastCycleStart,
		LastCycleEnd:                s.LastCycleEnd,
		LastSnapshotSuccess:         s.LastSnapshotSuccess,
		LastSnapshotError:           errorText(s.LastSnapshotError),
		ConsecutiveSnapshotFailures: s.ConsecutiveSnapshotFailures,
		CyclesTotal:                 s.CyclesTotal,
		CycleOverrunsTotal:          s.CycleOverrunsTotal,
		SnapshotTimeoutsTotal:       s.SnapshotTimeoutsTotal,
		EmitTimeoutsTotal:           s.EmitTimeoutsTotal,
		Emitters:                    make([]emitterHealthStatus, 0, len(s.Emitters)),
	}
	for _, es := range s.Emitters {
		last := es.LastEmitError
		if last == nil {
			last = es.LastInitError
		}
		out.Emitters = append(out.Emitters, emitterHealthStatus{
			ID:                  es.ID,
			State:               es.State,
			Busy:                es.Busy,
			LastEmitSuccess:     es.LastEmitSuccess,
			LastError:           errorText(last),
			LastErrorTime:       es.LastEmitErrorTime,
			ConsecutiveFailures: es.ConsecutiveFailures,
		})
	}
	return out
}
