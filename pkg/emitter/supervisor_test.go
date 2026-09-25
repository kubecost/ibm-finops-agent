package emitter

// Exporter supervision tests (docs/reliability/FINDINGS.md chunk 05: F-09, F-10, F-15).

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ibm/finops-agent/pkg/core"
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
	exporter := NewExporter(newEmptyDataSource(), provider, em)
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

	exporter := NewExporter(newEmptyDataSource(), newEmptySnapshotProvider(), hung, steady)
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
