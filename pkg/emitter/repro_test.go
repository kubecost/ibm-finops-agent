package emitter

// Reliability reproductions for the snapshot findings in docs/reliability/FINDINGS.md (chunk 06:
// F-06, F-14, F-39). Each assertion names its finding.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ibm/finops-agent/internal/mocks"
	"github.com/ibm/finops-agent/pkg/nodes"
	"github.com/opencost/opencost/core/pkg/source"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

// reproDataSource is a mock data source whose node stats and metrics can be made to fail.
type reproDataSource struct {
	*mocks.MockDataSource
	statsFail   atomic.Bool
	metrics     source.MetricsQuerier
	statsClient nodes.StatSummaryClient
}

func newReproDataSource() *reproDataSource {
	ds := &reproDataSource{MockDataSource: mocks.NewMockDataSource()}
	ds.statsClient = &failableStats{fail: &ds.statsFail}
	ds.metrics = ds.MockDataSource.Metrics()
	return ds
}

func (ds *reproDataSource) StatsSummary() nodes.StatSummaryClient { return ds.statsClient }
func (ds *reproDataSource) Metrics() source.MetricsQuerier        { return ds.metrics }

// failableStats returns no stats and a collection error for every node while fail is set.
type failableStats struct{ fail *atomic.Bool }

func (s *failableStats) GetNodeData() ([]*stats.Summary, error) {
	if s.fail.Load() {
		return nil, errors.New("node stats unavailable")
	}
	return nil, nil
}

// windowQuery is one metrics query for [Start, End) made at fake-clock time At.
type windowQuery struct {
	Start, End, At time.Time
}

// recordingQuerier records the fake-clock time of every QueryNodeActiveMinutes call, one of
// the queries snapshotMetrics issues per window, and fails it while fail is set.
type recordingQuerier struct {
	source.MetricsQuerier
	now  func() time.Time
	fail atomic.Bool

	mu      sync.Mutex
	queries []windowQuery
}

func (q *recordingQuerier) QueryNodeActiveMinutes(start, end time.Time) *source.Future[source.NodeActiveMinutesResult] {
	q.mu.Lock()
	q.queries = append(q.queries, windowQuery{Start: start, End: end, At: q.now()})
	q.mu.Unlock()
	if q.fail.Load() {
		return erroredFuture[source.NodeActiveMinutesResult](errors.New("metrics source unavailable"))
	}
	return q.MetricsQuerier.QueryNodeActiveMinutes(start, end)
}

func (q *recordingQuerier) lastQueryOf(start, end time.Time) (time.Time, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var last time.Time
	found := false
	for _, wq := range q.queries {
		if wq.Start.Equal(start) && wq.End.Equal(end) && (!found || wq.At.After(last)) {
			last, found = wq.At, true
		}
	}
	return last, found
}

func erroredFuture[T any](err error) *source.Future[T] {
	ch := make(source.QueryResultsChan, 1)
	ch <- &source.QueryResults{Query: "repro", Error: err}
	return source.NewFuture[T](func(*source.QueryResult) *T { return nil }, ch)
}

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return ts
}

// commitDelivered tells the provider that snap's metrics reached their consumer, as the
// exporter does after Kubecost's Emit succeeds. Providers without window bookkeeping ignore it.
func commitDelivered(provider SnapshotProvider, snap *ClusterSnapshot) {
	if c, ok := provider.(interface{ CommitWindows(*ClusterSnapshot) }); ok {
		c.CommitWindows(snap)
	}
}

// windowGapsTotal returns the windows the provider reported as gaps at resolution, or 0 if it
// reports none.
func windowGapsTotal(provider SnapshotProvider, resolution time.Duration) uint64 {
	if g, ok := provider.(interface{ WindowGapsTotal(time.Duration) uint64 }); ok {
		return g.WindowGapsTotal(resolution)
	}
	return 0
}

// F-06: a metrics failure lasting several 10m windows drops the windows in between. Every
// closed window must be snapshotted after it closes, or reported as a gap.
func TestReproF06WindowsDroppedAfterSnapshotFailure(t *testing.T) {
	clock := newTimeBender()
	ds := newReproDataSource()
	querier := &recordingQuerier{MetricsQuerier: ds.metrics, now: clock.now}
	ds.metrics = querier
	provider := NewConcurrentSnapshotProvider(&SnapshotConfig{
		MinutelyMetricsEnabled: true,
		KubernetesSnapshot:     NewKubernetesSnapshotConfig().EnableAll(),
		Now:                    clock.now,
	})

	// closedAt records, per 10m window start, whether a delivered snapshot covered it after
	// the window had closed.
	closedAt := map[time.Time]bool{}
	snapshotAt := func(at time.Time) bool {
		clock.current = at
		snap, err := provider.SnapshotOf(ds)
		if err != nil || snap.Metrics == nil {
			return false
		}
		for _, m := range snap.Metrics.Minutely {
			if !at.Before(*m.Window.End()) {
				closedAt[*m.Window.Start()] = true
			}
		}
		commitDelivered(provider, snap)
		return true
	}

	start := mustTime(t, "2026-01-01T09:05:00Z")
	if !snapshotAt(start) {
		t.Fatal("initial snapshot has no metrics")
	}
	// 35 minutes of failing metrics, one snapshot per minute, 09:06..09:40.
	querier.fail.Store(true)
	for m := 1; m <= 35; m++ {
		if snapshotAt(start.Add(time.Duration(m) * time.Minute)) {
			t.Fatalf("metrics at +%dm unexpectedly succeeded", m)
		}
	}
	querier.fail.Store(false)
	if !snapshotAt(mustTime(t, "2026-01-01T09:41:00Z")) {
		t.Fatal("recovery snapshot has no metrics")
	}

	var missing []string
	for w := mustTime(t, "2026-01-01T09:00:00Z"); w.Before(mustTime(t, "2026-01-01T09:40:00Z")); w = w.Add(10 * time.Minute) {
		if !closedAt[w] {
			missing = append(missing, fmt.Sprintf("%s-%s", w.Format("15:04"), w.Add(10*time.Minute).Format("15:04")))
		}
	}
	if gaps := windowGapsTotal(provider, 10*time.Minute); uint64(len(missing)) != gaps {
		t.Fatalf("F-06: after a 35 min metrics failure, closed 10m windows %v were never snapshotted after closing, and %d were reported as gaps", missing, gaps)
	}
}

// F-39: in Prometheus mode (metrics cache on) the closed hourly window keeps a snapshot taken
// up to ~5 min before its end and is never re-queried after rollover.
func TestReproF39CacheTruncatesClosedWindow(t *testing.T) {
	clock := newTimeBender()
	ds := newReproDataSource()
	querier := &recordingQuerier{MetricsQuerier: ds.metrics, now: clock.now}
	ds.metrics = querier
	provider := NewConcurrentSnapshotProvider(&SnapshotConfig{
		UseMetricsCache:    true,
		KubernetesSnapshot: NewKubernetesSnapshotConfig().EnableAll(),
		Now:                clock.now,
	})

	rollover := mustTime(t, "2026-01-01T10:00:00Z")
	var atRollover *MetricsSummary
	// exporter ticks every minute, 09:56 .. 10:06
	for at := mustTime(t, "2026-01-01T09:56:00Z"); !at.After(mustTime(t, "2026-01-01T10:06:00Z")); at = at.Add(time.Minute) {
		clock.current = at
		snap, err := provider.SnapshotOf(ds)
		if err != nil {
			t.Fatalf("snapshot at %s: %v", at.Format("15:04"), err)
		}
		commitDelivered(provider, snap)
		if at.Equal(rollover) {
			atRollover = snap.Metrics
		}
	}

	closedStart := rollover.Add(-time.Hour)
	last, ok := querier.lastQueryOf(closedStart, rollover)
	if !ok || last.Before(rollover) {
		t.Errorf("F-39: closed hourly window 09:00-10:00 was last queried at %s, before its end, and never re-queried after rollover (metrics cache)", last.Format("15:04:05"))
	}
	if atRollover != nil {
		var starts []string
		hasNew := false
		for _, m := range atRollover.Hourly {
			starts = append(starts, m.Window.Start().Format("15:04"))
			if m.Window.Start().Equal(rollover) {
				hasNew = true
			}
		}
		sort.Strings(starts)
		if !hasNew {
			t.Errorf("F-39: at the 10:00 tick the hourly snapshot has no entry for the new 10:00-11:00 window (windows: %v)", starts)
		}
	}
}

// lifecycleEmitter counts Init and Emit calls.
type lifecycleEmitter struct {
	id           EmitterID
	inits, emits atomic.Int32
}

func (e *lifecycleEmitter) ID() EmitterID { return e.id }

// RequiredComponents declares what Cloudability needs: no metrics.
func (e *lifecycleEmitter) RequiredComponents() []SnapshotComponent {
	return []SnapshotComponent{ComponentKubernetes, ComponentNodeStats}
}
func (e *lifecycleEmitter) Init(*ClusterSnapshot) error {
	e.inits.Add(1)
	return nil
}
func (e *lifecycleEmitter) Emit(context.Context, *ClusterSnapshot) error {
	e.emits.Add(1)
	return nil
}

// F-14: the snapshot is all-or-nothing, so a metrics failure (which Cloudability doesn't use)
// stops Cloudability emission.
func TestReproF14MetricsFailureStopsCloudability(t *testing.T) {
	ds := newReproDataSource()
	querier := &recordingQuerier{MetricsQuerier: ds.metrics, now: defaultNow}
	ds.metrics = querier
	provider := NewConcurrentSnapshotProvider(&SnapshotConfig{
		KubernetesSnapshot: NewKubernetesSnapshotConfig().EnableAll(),
		Now:                defaultNow,
	})
	cldy := &lifecycleEmitter{id: CldyEmitterID}
	exporter := NewExporter(ds, provider, cldy)
	if !exporter.Start(20 * time.Millisecond) {
		t.Fatal("failed to start exporter")
	}
	defer exporter.Stop()

	// Let Init and a healthy emission happen, then fail the metrics source only.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && cldy.emits.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if cldy.emits.Load() == 0 {
		t.Fatal("emitter never emitted while every source was healthy")
	}
	querier.fail.Store(true)
	time.Sleep(50 * time.Millisecond) // let an in-flight cycle finish
	before := cldy.emits.Load()
	time.Sleep(300 * time.Millisecond)
	if after := cldy.emits.Load(); after == before {
		t.Fatalf("F-14: Cloudability Emit not called for 300ms of exporter ticks while only the metrics querier was failing (emits stayed at %d)", after)
	}
}
