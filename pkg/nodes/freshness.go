package nodes

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ibm/finops-agent/pkg/condition"
	"github.com/ibm/finops-agent/pkg/health"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

// ConditionNodeStatsStale: no node-stats collection has returned stats for longer than the
// freshness threshold. It is a readiness condition, never a liveness one (D1): a restart doesn't
// make kubelets reachable.
const ConditionNodeStatsStale = "node_stats_stale"

// CachedStatSummaryClient is a StatSummaryClient that serves stats collected earlier (background
// collection) and says when they were collected.
type CachedStatSummaryClient interface {
	StatSummaryClient
	GetCachedNodeData() ([]*stats.Summary, time.Time, error)
}

// CollectionTimer reports when node stats were last collected successfully, measured where they
// are collected rather than from snapshot success (D1). *NodeStatsSummaryProvider and
// *CollectionRecorder implement it.
type CollectionTimer interface {
	LastCollection() time.Time
}

// CollectionRecorder wraps the StatSummaryClient that snapshots use on the foreground path and
// records when a collection last returned stats. The client itself is shared with OpenCost's
// collector, whose collections don't count.
type CollectionRecorder struct {
	client StatSummaryClient
	now    func() time.Time

	mu   sync.Mutex
	last time.Time
}

// NewCollectionRecorder wraps client.
func NewCollectionRecorder(client StatSummaryClient) *CollectionRecorder {
	return &CollectionRecorder{client: client, now: time.Now}
}

// GetNodeData implements StatSummaryClient.
func (r *CollectionRecorder) GetNodeData() ([]*stats.Summary, error) {
	data, err := r.client.GetNodeData()
	r.record(data)
	return data, err
}

// GetNodeDataContext implements ContextStatSummaryClient. A client that takes no context is
// called without one.
func (r *CollectionRecorder) GetNodeDataContext(ctx context.Context) ([]*stats.Summary, error) {
	cc, ok := r.client.(ContextStatSummaryClient)
	if !ok {
		return r.GetNodeData()
	}
	data, err := cc.GetNodeDataContext(ctx)
	r.record(data)
	return data, err
}

func (r *CollectionRecorder) record(data []*stats.Summary) {
	if len(data) == 0 {
		return
	}
	now := r.now()
	r.mu.Lock()
	r.last = now
	r.mu.Unlock()
}

// LastCollection implements CollectionTimer.
func (r *CollectionRecorder) LastCollection() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last
}

// FreshnessComponent returns the node-stats health component. It is always live, and not ready
// (node_stats_stale) when no collection has returned stats for longer than maxAge, counted from
// started until the first one does.
func FreshnessComponent(timer CollectionTimer, maxAge time.Duration, started time.Time) health.Component {
	return health.ComponentFunc(func(_ context.Context, now time.Time) health.Report {
		last := timer.LastCollection()
		report := health.Report{Live: true, Status: freshnessStatus{LastCollection: last, MaxAge: maxAge.String()}}
		from, reason := last, "no_recent_collection"
		if last.IsZero() {
			from, reason = started, "never_collected"
		}
		if age := now.Sub(from); age > maxAge {
			msg := fmt.Sprintf("no node-stats collection has returned stats for %s (threshold %s); last collection %s",
				age.Round(time.Second), maxAge, formatTime(last))
			report.Conditions = []condition.Condition{{Type: ConditionNodeStatsStale, Reason: reason, Message: msg}}
		}
		return report
	})
}

type freshnessStatus struct {
	LastCollection time.Time `json:"lastCollection"`
	MaxAge         string    `json:"maxAge"`
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return "never"
	}
	return t.UTC().Format(time.RFC3339)
}
