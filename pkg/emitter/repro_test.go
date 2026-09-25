//go:build reliability_repro

package emitter

// Reliability reproductions for the snapshot and exporter findings in
// docs/reliability/FINDINGS.md. Each test fails, naming its finding, until the chunk that fixes
// the finding removes the build tag.

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
	"github.com/ibm/finops-agent/pkg/core"
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

// F-06: a snapshot failure lasting several 10m windows drops the windows in between. Every
// closed window must be snapshotted after it closes, or the gap reported.
func TestReproF06WindowsDroppedAfterSnapshotFailure(t *testing.T) {
	clock := newTimeBender()
	ds := newReproDataSource()
	provider := NewConcurrentSnapshotProvider(&SnapshotConfig{
		MinutelyMetricsEnabled: true,
		KubernetesSnapshot:     NewKubernetesSnapshotConfig().EnableAll(),
		Now:                    clock.now,
	})

	// closedAt records, per 10m window start, whether a successful snapshot covered it after
	// the window had closed.
	closedAt := map[time.Time]bool{}
	snapshotAt := func(at time.Time) error {
		clock.current = at
		snap, err := provider.SnapshotOf(ds)
		if err != nil {
			return err
		}
		for _, m := range snap.Metrics.Minutely {
			if !at.Before(*m.Window.End()) {
				closedAt[*m.Window.Start()] = true
			}
		}
		return nil
	}

	start := mustTime(t, "2026-01-01T09:05:00Z")
	if err := snapshotAt(start); err != nil {
		t.Fatalf("initial snapshot: %v", err)
	}
	// 35 minutes of failing snapshots, one per minute, 09:06..09:40.
	ds.statsFail.Store(true)
	for m := 1; m <= 35; m++ {
		if err := snapshotAt(start.Add(time.Duration(m) * time.Minute)); err == nil {
			t.Fatalf("snapshot at +%dm unexpectedly succeeded", m)
		}
	}
	ds.statsFail.Store(false)
	if err := snapshotAt(mustTime(t, "2026-01-01T09:41:00Z")); err != nil {
		t.Fatalf("recovery snapshot: %v", err)
	}

	var missing []string
	for w := mustTime(t, "2026-01-01T09:00:00Z"); w.Before(mustTime(t, "2026-01-01T09:40:00Z")); w = w.Add(10 * time.Minute) {
		if !closedAt[w] {
			missing = append(missing, fmt.Sprintf("%s-%s", w.Format("15:04"), w.Add(10*time.Minute).Format("15:04")))
		}
	}
	if len(missing) > 0 {
		t.Fatalf("F-06: after a 35 min snapshot failure, closed 10m windows %v were never snapshotted after closing, and no gap was reported", missing)
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

// failFirstProvider fails its first n snapshots, then returns an empty snapshot.
type failFirstProvider struct {
	calls atomic.Int32
	n     int32
}

func (p *failFirstProvider) SnapshotOf(core.DataSource) (*ClusterSnapshot, error) {
	if p.calls.Add(1) <= p.n {
		return nil, errors.New("snapshot source unavailable")
	}
	return &ClusterSnapshot{}, nil
}

// F-09: one failed initial snapshot ends emission permanently.
func TestReproF09ExporterDiesOnInitialSnapshotFailure(t *testing.T) {
	provider := &failFirstProvider{n: 1}
	em := newCountingEmitter("f09")
	exporter := NewExporter(newEmptyDataSource(), provider, em)
	if !exporter.Start(20 * time.Millisecond) {
		t.Fatal("failed to start exporter")
	}
	defer exporter.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && em.count.Load() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if em.count.Load() == 0 {
		t.Fatalf("F-09: exporter never emitted after its first snapshot failed (%d snapshot attempts; the source recovered after 1)", provider.calls.Load())
	}
}

// lifecycleEmitter counts Init and Emit calls.
type lifecycleEmitter struct {
	id           EmitterID
	inits, emits atomic.Int32
}

func (e *lifecycleEmitter) ID() EmitterID { return e.id }
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
