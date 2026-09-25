package cldy

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ibm/finops-agent/pkg/condition"
	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/ibm/finops-agent/pkg/health"
	"github.com/ibm/finops-agent/pkg/telemetry"
)

// conditionUploadsFailing: the last uploadsFailingCycles upload cycles stopped on a failed
// upload while payloads were queued. The upload loop sets it at the end of each cycle.
const conditionUploadsFailing = "uploads_failing"

// uploadsFailingCycles is how many upload cycles in a row may fail, with a backlog, before the
// emitter is not ready: 3 × UploadFrequency with no delivery.
const uploadsFailingCycles = 3

// conditionStore holds the active conditions of the emitter and its uploader, which share one.
// It is safe for concurrent use: the exporter and upload loops set conditions while health
// checks read them. A nil store keeps nothing.
type conditionStore struct {
	now    func() time.Time
	mu     sync.Mutex
	active map[string]condition.Condition
}

func newConditionStore(now func() time.Time) *conditionStore {
	if now == nil {
		now = time.Now
	}
	return &conditionStore{now: now, active: map[string]condition.Condition{}}
}

// set records whether the condition is active and reports whether that changed. The message is
// kept, redacted, while the condition is active.
func (s *conditionStore) set(name string, active bool, msg string) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	prev, was := s.active[name]
	if !active {
		delete(s.active, name)
		return was
	}
	c := condition.Condition{Type: name, Message: condition.Redact(msg), Since: s.now()}
	if was {
		c.Since = prev.Since
	}
	s.active[name] = c
	return !was
}

// list returns the active conditions sorted by type.
func (s *conditionStore) list() []condition.Condition {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	out := make([]condition.Condition, 0, len(s.active))
	for _, c := range s.active {
		out = append(out, c)
	}
	s.mu.Unlock()
	slices.SortFunc(out, func(a, b condition.Condition) int { return strings.Compare(a.Type, b.Type) })
	return out
}

// Conditions returns the emitter's and its uploader's active conditions.
func (ce *Emitter) Conditions() []condition.Condition {
	return ce.conditions.list()
}

// HealthCheck implements health.Component for the Cloudability emitter and its uploader.
//
// It is not live only when the upload loop has stopped making progress (UploadStalled): a restart
// fixes a hung upload, and no longer loses the queue (F-01, F-36). It is not ready while any
// condition is active: no storage service, refused credentials, rejected payloads, disk
// pressure, an uninitialised emitter, or uploads failing with a backlog for uploadsFailingCycles
// cycles. None of those make it not live (I4).
func (ce *Emitter) HealthCheck(_ context.Context, now time.Time) health.Report {
	// The emitter and uploader log their conditions' transitions themselves.
	report := health.Report{Live: true, Conditions: ce.Conditions(), ConditionsLogged: true}
	cu, ok := ce.Uploader.(*CldyUploader)
	if !ok {
		return report
	}
	report.Status = cu.UploadHeartbeat()
	if stalled, reason := cu.UploadStalled(now); stalled {
		report.Live = false
		report.NotLiveReason = reason
	}
	return report
}

// UploadStalled reports whether the upload loop has stopped making progress, and why. A cycle
// makes progress with every upload attempt, so it is stalled when nothing has happened for
// longer than one payload's worst case (uploadAttemptBudget) plus one upload interval. Between
// cycles it is stalled when no cycle has started for two upload intervals. Before the loop has
// started it is never stalled.
func (cu *CldyUploader) UploadStalled(now time.Time) (bool, string) {
	hb := cu.UploadHeartbeat()
	if hb.LoopStart.IsZero() {
		return false, ""
	}
	interval := max(cu.config.UploadFrequency, time.Second)
	if hb.LastCycleStart.After(hb.LastCycleEnd) {
		last := hb.LastCycleStart
		if hb.LastProgress.After(last) {
			last = hb.LastProgress
		}
		budget := uploadAttemptBudget(cu.config.Timeout) + interval
		if now.Sub(last) > budget {
			return true, fmt.Sprintf("upload loop stalled: the cycle that started %s ago has made no progress for %s (budget %s)",
				now.Sub(hb.LastCycleStart).Round(time.Second), now.Sub(last).Round(time.Second), budget)
		}
		return false, ""
	}
	last := hb.LastCycleEnd
	if hb.LoopStart.After(last) {
		last = hb.LoopStart
	}
	if now.Sub(last) > 2*interval {
		return true, fmt.Sprintf("upload loop stalled: no upload cycle has started for %s (interval %s)",
			now.Sub(last).Round(time.Second), interval)
	}
	return false, ""
}

// uploadAttemptBudget is the longest one payload's upload can take when every request uses its
// timeout: five stages (login, presign, put, and presign and put again after a 403), each tried
// maxAttempts times with backoff, plus a minute of margin. It is at least minUploadAttemptBudget:
// custom S3 and Azure uploads have no deadline of their own yet (F-13), and a connection that a
// remote fault black-holes can hang for ~15 min before the kernel gives up (tcp_retries2), which
// is a remote fault, not a wedge (I4).
func uploadAttemptBudget(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	return max(5*time.Duration(maxAttempts)*timeout+time.Minute, minUploadAttemptBudget)
}

// minUploadAttemptBudget is the floor of uploadAttemptBudget.
const minUploadAttemptBudget = 30 * time.Minute

// checkUploadsFailing raises uploads_failing when the last uploadsFailingCycles cycles failed with
// a backlog, and clears it otherwise. The upload loop calls it at the end of each cycle.
func (cu *CldyUploader) checkUploadsFailing(hb UploadHeartbeat) {
	failing := hb.ConsecutiveFailures >= uploadsFailingCycles && hb.BacklogFiles > 0
	cu.setCondition(conditionUploadsFailing, failing,
		fmt.Sprintf("the last %d upload cycles failed with %d files (%d bytes) queued; last delivery %s",
			hb.ConsecutiveFailures, hb.BacklogFiles, hb.BacklogBytes, formatSince(hb.LastCycleEnd, hb.LastSuccess)))
}

func formatSince(now, t time.Time) string {
	if t.IsZero() {
		return "never"
	}
	return now.Sub(t).Round(time.Second).String() + " ago"
}

// UploadStatus returns the upload loop's progress for metrics and the status summary. It is zero
// when the emitter has no CldyUploader.
func (ce *Emitter) UploadStatus() telemetry.UploadStatus {
	cu, ok := ce.Uploader.(*CldyUploader)
	if !ok {
		return telemetry.UploadStatus{}
	}
	hb := cu.UploadHeartbeat()
	return telemetry.UploadStatus{LastSuccess: hb.LastSuccess, BacklogFiles: hb.BacklogFiles, BacklogBytes: hb.BacklogBytes}
}

// statusSummary is the agent's health for agent-measurement.json: config.StatusSummary, or else
// the emitter's own conditions, drops and upload queue.
func (ce *Emitter) statusSummary() *telemetry.Summary {
	if ce.config.StatusSummary != nil {
		s := ce.config.StatusSummary()
		return &s
	}
	s := telemetry.Summary{
		SchemaVersion:    telemetry.SummarySchemaVersion,
		ActiveConditions: []telemetry.SummaryCondition{},
		CountersSinceTS:  ce.startTime.Unix(),
	}
	for _, c := range ce.Conditions() {
		s.ActiveConditions = append(s.ActiveConditions, telemetry.SummaryCondition{
			Component: string(emitter.CldyEmitterID), Type: c.Type, Reason: c.Reason, SinceTS: c.Since.Unix(),
		})
	}
	s.Ready = len(s.ActiveConditions) == 0
	if ce.counts != nil {
		counts := ce.counts.Snapshot()
		for reason, n := range counts.Dropped {
			if n <= 0 {
				continue
			}
			if reason == dropReasonQuarantineEvicted {
				s.QuarantineEvictedTotal += uint64(n)
				continue
			}
			if s.DataDropped == nil {
				s.DataDropped = map[string]map[string]uint64{telemetry.EmitterCloudability: {}}
			}
			s.DataDropped[telemetry.EmitterCloudability][reason] += uint64(n)
			s.DataDroppedTotal += uint64(n)
		}
		s.EmissionSlotsSkippedTotal = uint64(counts.EmissionSlotsSkipped)
	}
	u := ce.UploadStatus()
	s.BacklogFiles, s.BacklogBytes = u.BacklogFiles, u.BacklogBytes
	if !u.LastSuccess.IsZero() {
		s.LastUploadSuccessTS = u.LastSuccess.Unix()
	}
	return &s
}
