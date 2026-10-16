package emitter

// Exporter supervision tests (docs/reliability/FINDINGS.md chunk 05: F-09, F-10, F-15).

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ibm/finops-agent/internal/mocks"
	"github.com/ibm/finops-agent/pkg/core"
	"github.com/ibm/finops-agent/pkg/nodes"
	corev1 "k8s.io/api/core/v1"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

// waitFor polls cond every 5ms until it holds or timeout elapses, and reports whether it held.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
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

// A snapshot source that fails 5 times and then recovers lets emission resume, and stopping the
// exporter leaves no goroutines behind.
func TestExporterResumesAfterRepeatedSnapshotFailures(t *testing.T) {
	before := runtime.NumGoroutine()

	provider := &failFirstProvider{n: 5}
	em := newCountingEmitter("resume")
	exporter := NewExporter(newEmptyDataSource(), provider, em)
	if !exporter.Start(20 * time.Millisecond) {
		t.Fatal("failed to start exporter")
	}
	if !waitFor(3*time.Second, func() bool { return em.count.Load() >= 3 }) {
		exporter.Stop()
		t.Fatalf("F-09: emission did not resume after 5 failed snapshots (%d snapshot attempts, %d emits)", provider.calls.Load(), em.count.Load())
	}
	exporter.Stop()

	if !waitFor(time.Second, func() bool { return runtime.NumGoroutine() <= before }) {
		t.Errorf("goroutine leak: %d goroutines before Start, %d after Stop", before, runtime.NumGoroutine())
	}
}

// hangFirstProvider blocks its first snapshot, ignoring any deadline, until release is closed.
// Later snapshots succeed.
type hangFirstProvider struct {
	calls   atomic.Int32
	release chan struct{}
}

func (p *hangFirstProvider) SnapshotOf(core.DataSource) (*ClusterSnapshot, error) {
	if p.calls.Add(1) == 1 {
		<-p.release
		return nil, errors.New("released")
	}
	return &ClusterSnapshot{}, nil
}

// F-15: a hung snapshot ends its cycle at the deadline and the next cycle runs.
func TestExporterHungSnapshotEndsCycleAtDeadline(t *testing.T) {
	provider := &hangFirstProvider{release: make(chan struct{})}
	t.Cleanup(func() { close(provider.release) })

	em := newCountingEmitter("hung-snapshot")
	exporter := NewExporterWithConfig(newEmptyDataSource(), provider, ExporterConfig{StopGrace: 10 * time.Millisecond}, em)
	if !exporter.Start(20 * time.Millisecond) {
		t.Fatal("failed to start exporter")
	}
	defer exporter.Stop()

	if !waitFor(3*time.Second, func() bool { return em.count.Load() > 0 }) {
		t.Fatalf("F-15: a hung snapshot blocked the exporter loop; no later cycle ran (%d snapshot attempts)", provider.calls.Load())
	}
}

// lifecycleCheckingEmitter fails its first failInits Init calls and records any Emit that
// arrives before a successful Init.
type lifecycleCheckingEmitter struct {
	id        EmitterID
	failInits int32

	inits, emits, earlyEmits atomic.Int32
	ready                    atomic.Bool
}

func (e *lifecycleCheckingEmitter) ID() EmitterID { return e.id }

func (e *lifecycleCheckingEmitter) Init(*ClusterSnapshot) error {
	if e.inits.Add(1) <= e.failInits {
		return errors.New("bucket config not mounted yet")
	}
	e.ready.Store(true)
	return nil
}

func (e *lifecycleCheckingEmitter) Emit(context.Context, *ClusterSnapshot) error {
	if !e.ready.Load() {
		e.earlyEmits.Add(1)
		return errors.New("emit before init")
	}
	e.emits.Add(1)
	return nil
}

// F-10: an emitter whose Init fails is retried until it succeeds, is never passed to Emit
// before that, and is never re-initialised after. Another emitter emits throughout (I5).
func TestExporterRetriesInitUntilReady(t *testing.T) {
	flaky := &lifecycleCheckingEmitter{id: KubecostEmitterID, failInits: 3}
	steady := &lifecycleCheckingEmitter{id: CldyEmitterID}

	exporter := NewExporter(newEmptyDataSource(), newEmptySnapshotProvider(), flaky, steady)
	if !exporter.Start(20 * time.Millisecond) {
		t.Fatal("failed to start exporter")
	}
	if !waitFor(3*time.Second, func() bool { return flaky.emits.Load() >= 3 }) {
		exporter.Stop()
		t.Fatalf("F-10: emitter never became ready after 3 failed Inits (inits=%d, emits=%d)", flaky.inits.Load(), flaky.emits.Load())
	}
	exporter.Stop()

	if got := flaky.inits.Load(); got != 4 {
		t.Errorf("Init called %d times; want 3 failures and exactly 1 success", got)
	}
	if got := flaky.earlyEmits.Load(); got != 0 {
		t.Errorf("F-10: Emit called %d times on an uninitialised emitter", got)
	}
	if got := steady.inits.Load(); got != 1 {
		t.Errorf("steady emitter Init called %d times; want 1", got)
	}
	// The steady emitter kept emitting while the flaky one was failing Init.
	if steady.emits.Load() < flaky.emits.Load()+3 {
		t.Errorf("I5: steady emitter emitted %d times vs flaky %d; it should have emitted during the 3 failed Init cycles", steady.emits.Load(), flaky.emits.Load())
	}
}

// blockingEmitter blocks every Emit, ignoring its context, until release is closed.
type blockingEmitter struct {
	release chan struct{}
	calls   atomic.Int32
}

func (e *blockingEmitter) ID() EmitterID               { return "blocking" }
func (e *blockingEmitter) Init(*ClusterSnapshot) error { return nil }
func (e *blockingEmitter) Emit(context.Context, *ClusterSnapshot) error {
	e.calls.Add(1)
	<-e.release
	return nil
}

// F-15 / I5: an Emit that hangs past its deadline does not stop other emitters.
func TestExporterHungEmitterDoesNotBlockOthers(t *testing.T) {
	hung := &blockingEmitter{release: make(chan struct{})}
	t.Cleanup(func() { close(hung.release) })
	steady := newCountingEmitter("steady")

	exporter := NewExporterWithConfig(newEmptyDataSource(), newEmptySnapshotProvider(), ExporterConfig{StopGrace: 10 * time.Millisecond}, hung, steady)
	if !exporter.Start(20 * time.Millisecond) {
		t.Fatal("failed to start exporter")
	}
	defer exporter.Stop()

	if !waitFor(3*time.Second, func() bool { return steady.count.Load() >= 5 }) {
		t.Fatalf("I5: a hung emitter stopped the other emitter (steady emitted %d times)", steady.count.Load())
	}
	if got := hung.calls.Load(); got != 1 {
		t.Errorf("hung emitter's Emit was called %d times; a new Emit must not start while the previous one is running", got)
	}
}

// panicOnceProvider panics on its first snapshot, then succeeds.
type panicOnceProvider struct{ calls atomic.Int32 }

func (p *panicOnceProvider) SnapshotOf(core.DataSource) (*ClusterSnapshot, error) {
	if p.calls.Add(1) == 1 {
		panic("snapshot exploded")
	}
	return &ClusterSnapshot{}, nil
}

// panicInitEmitter panics on its first Init.
type panicInitEmitter struct {
	inits, emits atomic.Int32
}

func (e *panicInitEmitter) ID() EmitterID { return "panic-init" }
func (e *panicInitEmitter) Init(*ClusterSnapshot) error {
	if e.inits.Add(1) == 1 {
		panic("init exploded")
	}
	return nil
}
func (e *panicInitEmitter) Emit(context.Context, *ClusterSnapshot) error {
	e.emits.Add(1)
	return nil
}

// Panics in SnapshotOf and Init are recovered and treated as failures.
func TestExporterRecoversSnapshotAndInitPanics(t *testing.T) {
	provider := &panicOnceProvider{}
	em := &panicInitEmitter{}
	exporter := NewExporter(newEmptyDataSource(), provider, em)
	if !exporter.Start(20 * time.Millisecond) {
		t.Fatal("failed to start exporter")
	}
	defer exporter.Stop()

	if !waitFor(3*time.Second, func() bool { return em.emits.Load() > 0 }) {
		t.Fatalf("exporter did not recover from a panicking snapshot and Init (snapshots=%d, inits=%d)", provider.calls.Load(), em.inits.Load())
	}
}

// Stop waits for the loop, so state written by Init and Emit is visible to the caller afterwards
// (run under -race).
func TestExporterStopWaitsForLoop(t *testing.T) {
	em := &lifecycleCheckingEmitter{id: "stop-waits"}
	exporter := NewExporter(newEmptyDataSource(), newEmptySnapshotProvider(), em)
	if !exporter.Start(5 * time.Millisecond) {
		t.Fatal("failed to start exporter")
	}
	waitFor(time.Second, func() bool { return em.emits.Load() > 0 })
	exporter.Stop()
	n := em.emits.Load()
	time.Sleep(50 * time.Millisecond)
	if after := em.emits.Load(); after != n {
		t.Errorf("Emit called %d more times after Stop returned", after-n)
	}
}

// hangAllProvider blocks every snapshot, ignoring any deadline, until release is closed.
type hangAllProvider struct {
	calls   atomic.Int32
	release chan struct{}
}

func (p *hangAllProvider) SnapshotOf(core.DataSource) (*ClusterSnapshot, error) {
	p.calls.Add(1)
	<-p.release
	return nil, errors.New("released")
}

// A hung snapshot ends each cycle at its deadline, the heartbeat shows the failure, cycles keep
// running, the loop is not Stalled, and at most two snapshots are ever in flight.
func TestExporterHungSnapshotHeartbeat(t *testing.T) {
	provider := &hangAllProvider{release: make(chan struct{})}
	t.Cleanup(func() { close(provider.release) })

	exporter := NewExporterWithConfig(newEmptyDataSource(), provider, ExporterConfig{
		SnapshotTimeout: 30 * time.Millisecond,
		InitialBackoff:  5 * time.Millisecond,
		StopGrace:       10 * time.Millisecond,
	}, newCountingEmitter("x"))
	if !exporter.Start(20 * time.Millisecond) {
		t.Fatal("failed to start exporter")
	}
	defer exporter.Stop()

	// Each hung cycle ends at the deadline and records a failure before the next one starts.
	if !waitFor(3*time.Second, func() bool { return exporter.Status().ConsecutiveSnapshotFailures >= 6 }) {
		t.Fatalf("cycles stopped while snapshots hung: %+v", exporter.Status())
	}
	status := exporter.Status()
	if status.SnapshotTimeoutsTotal < 2 {
		t.Errorf("SnapshotTimeoutsTotal = %d; want every hung snapshot counted", status.SnapshotTimeoutsTotal)
	}
	if !errors.Is(status.LastSnapshotError, context.DeadlineExceeded) {
		t.Errorf("heartbeat doesn't show the snapshot deadline: err=%v", status.LastSnapshotError)
	}
	if !status.LastSnapshotSuccess.IsZero() {
		t.Errorf("LastSnapshotSuccess = %s; no snapshot succeeded", status.LastSnapshotSuccess)
	}
	if got := provider.calls.Load(); got > maxSnapshotsInFlight {
		t.Errorf("%d snapshots started while earlier ones were still hung; want at most %d", got, maxSnapshotsInFlight)
	}
	if exporter.Stalled(time.Now()) {
		t.Error("Stalled() is true, but the loop is cycling; a failing snapshot must not fail liveness")
	}
}

// slowEmitter takes d per Emit.
type slowEmitter struct {
	d     time.Duration
	emits atomic.Int32
}

func (e *slowEmitter) ID() EmitterID               { return "slow" }
func (e *slowEmitter) Init(*ClusterSnapshot) error { return nil }
func (e *slowEmitter) Emit(ctx context.Context, _ *ClusterSnapshot) error {
	e.emits.Add(1)
	select {
	case <-time.After(e.d):
	case <-ctx.Done():
	}
	return nil
}

// A cycle longer than the interval counts the missed ticks and doesn't run them back to back.
func TestExporterCountsCycleOverruns(t *testing.T) {
	const interval = 20 * time.Millisecond
	em := &slowEmitter{d: 3 * interval}
	exporter := NewExporter(newEmptyDataSource(), newEmptySnapshotProvider(), em)
	if !exporter.Start(interval) {
		t.Fatal("failed to start exporter")
	}
	time.Sleep(500 * time.Millisecond)
	exporter.Stop()

	status := exporter.Status()
	if status.CycleOverrunsTotal == 0 {
		t.Errorf("CycleOverrunsTotal = 0 after cycles of 3 intervals each")
	}
	// Each emitting cycle takes ≥ 60ms plus the wait for the next tick, so ≤ ~500/80 emits.
	if got := em.emits.Load(); got > 8 {
		t.Errorf("%d emits in 500ms of 60ms cycles; missed ticks were run back to back", got)
	}
}

// The per-emitter status tracks the lifecycle: uninitialised with an Init error, then ready
// with emit successes; a hung emitter is busy with a deadline error.
func TestExporterEmitterStatus(t *testing.T) {
	flaky := &lifecycleCheckingEmitter{id: "flaky", failInits: 2}
	hung := &blockingEmitter{release: make(chan struct{})}
	t.Cleanup(func() { close(hung.release) })

	exporter := NewExporterWithConfig(newEmptyDataSource(), newEmptySnapshotProvider(), ExporterConfig{
		EmitTimeout: 30 * time.Millisecond,
		StopGrace:   10 * time.Millisecond,
	}, flaky, hung)
	if !exporter.Start(20 * time.Millisecond) {
		t.Fatal("failed to start exporter")
	}
	defer exporter.Stop()

	byID := func(id EmitterID) EmitterStatus {
		for _, es := range exporter.Status().Emitters {
			if es.ID == id {
				return es
			}
		}
		t.Fatalf("no status for %s", id)
		return EmitterStatus{}
	}

	if !waitFor(2*time.Second, func() bool { return byID("flaky").LastInitError != nil }) {
		t.Fatal("flaky emitter's Init error never showed in its status")
	}
	if es := byID("flaky"); es.State != EmitterUninitialised {
		t.Errorf("flaky state = %s while Init is failing", es.State)
	}
	if !waitFor(2*time.Second, func() bool { return !byID("flaky").LastEmitSuccess.IsZero() }) {
		t.Fatal("flaky emitter never recorded an emit success")
	}
	if es := byID("flaky"); es.State != EmitterReady || es.LastInitError != nil || es.ConsecutiveFailures != 0 {
		t.Errorf("flaky status after recovery = %+v", es)
	}

	if !waitFor(2*time.Second, func() bool {
		es := byID("blocking")
		return es.Busy && es.ConsecutiveFailures >= 2
	}) {
		t.Errorf("hung emitter status doesn't show the deadline: %+v", byID("blocking"))
	}
	if st := exporter.Status(); st.EmitTimeoutsTotal == 0 {
		t.Errorf("EmitTimeoutsTotal = 0 with a hung emitter")
	}
}

func TestExporterStalled(t *testing.T) {
	const interval = time.Minute
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cfg := ExporterConfig{SnapshotTimeout: 5 * time.Minute, EmitTimeout: 5 * time.Minute}.withDefaults(interval)
	// cycle deadline: 5m + 5m + 1m slack; gap limit: 2m + 1m slack

	tests := []struct {
		name       string
		running    bool
		inCycle    bool
		cycleStart time.Time
		cycleEnd   time.Time
		now        time.Time
		want       bool
	}{
		{name: "not running", running: false, now: base.Add(time.Hour), want: false},
		{name: "just started, no cycle yet", running: true, now: base.Add(time.Minute), want: false},
		{name: "no cycle since start", running: true, now: base.Add(3*time.Minute + time.Second), want: true},
		{name: "cycle in progress within deadline", running: true, inCycle: true, cycleStart: base.Add(time.Minute), now: base.Add(11 * time.Minute), want: false},
		{name: "cycle in progress past deadline", running: true, inCycle: true, cycleStart: base.Add(time.Minute), now: base.Add(12*time.Minute + time.Second), want: true},
		{name: "cycle ended recently", running: true, cycleStart: base.Add(time.Minute), cycleEnd: base.Add(2 * time.Minute), now: base.Add(4 * time.Minute), want: false},
		{name: "long cycle just ended", running: true, cycleStart: base.Add(time.Minute), cycleEnd: base.Add(10 * time.Minute), now: base.Add(11 * time.Minute), want: false},
		{name: "no cycle since last ended", running: true, cycleStart: base.Add(time.Minute), cycleEnd: base.Add(2 * time.Minute), now: base.Add(5*time.Minute + time.Second), want: true},
		{name: "heartbeats from before a restart are ignored", running: true, cycleStart: base.Add(-time.Hour), cycleEnd: base.Add(-time.Hour), now: base.Add(time.Minute), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			de := &defaultExporter{
				running:        tt.running,
				inCycle:        tt.inCycle,
				interval:       interval,
				effective:      cfg,
				startedAt:      base,
				lastCycleStart: tt.cycleStart,
				lastCycleEnd:   tt.cycleEnd,
			}
			if got := de.Stalled(tt.now); got != tt.want {
				t.Errorf("Stalled(%s) = %v; want %v", tt.now.Sub(base), got, tt.want)
			}
		})
	}
}

// ctxStatsClient records whether it was called through the context-aware interface.
type ctxStatsClient struct {
	gotDeadline atomic.Bool
	plainCalls  atomic.Int32
}

func (c *ctxStatsClient) GetNodeData() ([]*stats.Summary, error) {
	c.plainCalls.Add(1)
	return []*stats.Summary{{}}, nil
}

func (c *ctxStatsClient) GetNodeDataContext(ctx context.Context) ([]*stats.Summary, error) {
	_, ok := ctx.Deadline()
	c.gotDeadline.Store(ok)
	return []*stats.Summary{{}}, nil
}

type ctxStatsDataSource struct {
	*mocks.MockDataSource
	stats *ctxStatsClient
}

func (ds ctxStatsDataSource) StatsSummary() nodes.StatSummaryClient { return ds.stats }

// The exporter's snapshot deadline reaches the node-stats fan-out.
func TestSnapshotDeadlineReachesNodeStats(t *testing.T) {
	ds := ctxStatsDataSource{MockDataSource: mocks.NewMockDataSource(), stats: &ctxStatsClient{}}
	provider := NewConcurrentSnapshotProvider(DefaultSnapshotConfig())
	em := newCountingEmitter("x")
	exporter := NewExporter(ds, provider, em)
	if !exporter.Start(20 * time.Millisecond) {
		t.Fatal("failed to start exporter")
	}
	defer exporter.Stop()

	if !waitFor(2*time.Second, func() bool { return em.count.Load() > 0 }) {
		t.Fatalf("no emission: %+v", exporter.Status())
	}
	if !ds.stats.gotDeadline.Load() || ds.stats.plainCalls.Load() != 0 {
		t.Errorf("node stats weren't collected under the snapshot deadline (deadline=%v, context-free calls=%d)",
			ds.stats.gotDeadline.Load(), ds.stats.plainCalls.Load())
	}
}

// slowProvider takes d per snapshot, ignoring any deadline, and succeeds.
type slowProvider struct {
	d     time.Duration
	calls atomic.Int32
}

func (p *slowProvider) SnapshotOf(core.DataSource) (*ClusterSnapshot, error) {
	p.calls.Add(1)
	time.Sleep(p.d)
	return &ClusterSnapshot{}, nil
}

// A source that is always slower than the snapshot deadline still gets through: the late
// result is used when it completes, as it was before deadlines existed.
func TestExporterUsesLateSnapshots(t *testing.T) {
	provider := &slowProvider{d: 80 * time.Millisecond}
	em := newCountingEmitter("late")
	exporter := NewExporterWithConfig(newEmptyDataSource(), provider, ExporterConfig{
		SnapshotTimeout: 30 * time.Millisecond,
	}, em)
	if !exporter.Start(20 * time.Millisecond) {
		t.Fatal("failed to start exporter")
	}
	defer exporter.Stop()

	if !waitFor(3*time.Second, func() bool { return em.count.Load() >= 3 }) {
		t.Fatalf("a snapshot source slower than the deadline never got through (%d snapshots, %+v)", provider.calls.Load(), exporter.Status())
	}
	if st := exporter.Status(); st.SnapshotTimeoutsTotal == 0 {
		t.Errorf("SnapshotTimeoutsTotal = 0; the slow snapshots overran their deadline")
	}
	// Never more than one slow snapshot at a time: a second starts only after 2 deadlines.
	if calls, emits := provider.calls.Load(), int32(em.count.Load()); calls > emits+2 {
		t.Errorf("%d snapshots for %d emits; late snapshots were thrown away", calls, emits)
	}
}

// An Init that hangs well past its deadline is a local wedge: Stalled reports it.
func TestExporterStalledOnStuckCall(t *testing.T) {
	hung := &blockingEmitter{release: make(chan struct{})}
	t.Cleanup(func() { close(hung.release) })

	exporter := NewExporterWithConfig(newEmptyDataSource(), newEmptySnapshotProvider(), ExporterConfig{
		EmitTimeout:     20 * time.Millisecond,
		StuckCallCycles: 2,
		StopGrace:       10 * time.Millisecond,
	}, hung)
	if !exporter.Start(10 * time.Millisecond) {
		t.Fatal("failed to start exporter")
	}
	defer exporter.Stop()

	// Init succeeds; the first Emit hangs. Stuck after 20ms + 2 × 10ms.
	if !waitFor(time.Second, func() bool { return hung.calls.Load() == 1 }) {
		t.Fatal("Emit never called")
	}
	if exporter.Stalled(time.Now()) {
		t.Error("Stalled() true as soon as the call started")
	}
	if !waitFor(2*time.Second, func() bool { return exporter.Stalled(time.Now()) }) {
		t.Errorf("Stalled() stayed false with an Emit stuck far past its deadline: %+v", exporter.Status())
	}
}

// failingStats fails node-stats collection outright.
type failingStats struct{}

func (failingStats) GetNodeData() ([]*stats.Summary, error) {
	return nil, errors.New("node stats unavailable")
}

type failingStatsDataSource struct{ *mocks.MockDataSource }

func (failingStatsDataSource) StatsSummary() nodes.StatSummaryClient { return failingStats{} }

// F-14: a failed component no longer fails the snapshot or loses the short-lived pods: the
// snapshot carries them with the component's error, and nothing is counted as discarded.
func TestFailedComponentKeepsSnapshotAndShortLivedPods(t *testing.T) {
	ds := failingStatsDataSource{mocks.NewMockDataSource()}
	ds.ClusterCache.Pods = []*corev1.Pod{{}, {}, {}}
	provider := NewConcurrentSnapshotProvider(DefaultSnapshotConfig()).(*ConcurrentSnapshotProvider)

	snap, err := provider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("snapshot failed with only node stats failing: %v", err)
	}
	if c, cerr := snap.MissingComponent(AllComponents); c != ComponentNodeStats || cerr == nil {
		t.Errorf("MissingComponent = %q, %v; want node_stats with its error", c, cerr)
	}
	if snap.NodeStats != nil || snap.Kubernetes == nil || snap.Metrics == nil || snap.ClusterInfo == nil {
		t.Errorf("want only NodeStats nil, got %+v", snap)
	}
	if got := len(snap.Kubernetes.ShortLivedPods); got != 3 {
		t.Errorf("snapshot has %d short-lived pods; want 3", got)
	}
	if got := provider.DiscardedShortLivedPods(); got != 0 {
		t.Errorf("DiscardedShortLivedPods() = %d; want 0", got)
	}
}
