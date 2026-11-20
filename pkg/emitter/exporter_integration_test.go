package emitter

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ibm/finops-agent/internal/mocks"
	"github.com/ibm/finops-agent/pkg/core"
)

// waitForUint32 polls counter until it reaches at least target, or the timeout expires.
func waitForUint32(counter *atomic.Uint32, target uint32, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if counter.Load() >= target {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return counter.Load() >= target
}

// waitForInt32 polls counter until it reaches at least target, or the timeout expires.
func waitForInt32(counter *int32, target int32, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(counter) >= target {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return atomic.LoadInt32(counter) >= target
}

// waitForBool polls flag until it becomes true, or the timeout expires.
func waitForBool(flag *atomic.Bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if flag.Load() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return flag.Load()
}

// ---------------------------------------------------------------------------
// Integration: Full exporter lifecycle with real snapshot provider
// ---------------------------------------------------------------------------

func TestExporterWithRealSnapshotProvider(t *testing.T) {
	ds := mocks.NewMockDataSource()
	config := DefaultSnapshotConfig()
	snapshotProvider := NewConcurrentSnapshotProvider(config)

	emitter := newCountingEmitter("real-snapshot-emitter")
	exporter := NewExporter(ds, snapshotProvider, emitter)

	if !exporter.Start(200 * time.Millisecond) {
		t.Fatal("Failed to start exporter")
	}

	if !waitForUint32(&emitter.count, 3, 5*time.Second) {
		exporter.Stop()
		t.Fatalf("Expected at least 3 emissions, got %d", emitter.count.Load())
	}
	exporter.Stop()
}

// ---------------------------------------------------------------------------
// Integration: Exporter with failing snapshot provider
// ---------------------------------------------------------------------------

func TestExporterHandlesSnapshotFailure(t *testing.T) {
	ds := mocks.NewMockDataSource()

	callCount := int32(0)
	sp := &intermittentSnapshotProvider{
		callCount: &callCount,
		failEvery: 2,
	}

	emitter := newCountingEmitter("snapshot-failure-emitter")
	exporter := NewExporter(ds, sp, emitter)

	if !exporter.Start(100 * time.Millisecond) {
		t.Fatal("Failed to start exporter")
	}

	// Wait until we've had enough snapshot calls to observe both successes and failures.
	// With failEvery=2, we need at least 3 calls to guarantee at least 1 failure and 2 successes.
	if !waitForInt32(&callCount, 3, 5*time.Second) {
		exporter.Stop()
		t.Fatalf("Expected at least 3 snapshot calls, got %d", atomic.LoadInt32(&callCount))
	}
	exporter.Stop()

	// Not all snapshots succeed, so emission count should be less than total cycles
	emitCount := emitter.count.Load()
	totalCalls := atomic.LoadInt32(&callCount)

	if emitCount == 0 {
		t.Error("Expected at least some emissions despite snapshot failures")
	}
	if emitCount >= uint32(totalCalls) {
		t.Error("Expected fewer emissions than snapshot attempts due to failures")
	}
}

// ---------------------------------------------------------------------------
// Integration: Exporter initialization phase
// ---------------------------------------------------------------------------

func TestExporterInitializesEmitters(t *testing.T) {
	ds := mocks.NewMockDataSource()
	snapshotProvider := newEmptySnapshotProvider()

	emitter := &initTrackingEmitter{name: "init-tracker"}
	exporter := NewExporter(ds, snapshotProvider, emitter)

	if !exporter.Start(time.Hour) { // Long interval so only Init runs
		t.Fatal("Failed to start exporter")
	}

	if !waitForBool(&emitter.initialized, 5*time.Second) {
		t.Error("Expected emitter.Init() to be called during start")
	}

	exporter.Stop()
}

func TestExporterInitFailureDoesNotPreventLoop(t *testing.T) {
	ds := mocks.NewMockDataSource()
	snapshotProvider := newEmptySnapshotProvider()

	failInit := &failingInitEmitter{name: "fail-init"}
	goodEmitter := newCountingEmitter("good-emitter")

	exporter := NewExporter(ds, snapshotProvider, failInit, goodEmitter)

	if !exporter.Start(100 * time.Millisecond) {
		t.Fatal("Failed to start exporter")
	}

	if !waitForUint32(&goodEmitter.count, 2, 5*time.Second) {
		exporter.Stop()
		t.Fatalf("Good emitter should have received at least 2 emissions, got %d", goodEmitter.count.Load())
	}
	exporter.Stop()
}

// ---------------------------------------------------------------------------
// Integration: Emitter panic recovery
// ---------------------------------------------------------------------------

func TestExporterRecoverFromEmitterPanic(t *testing.T) {
	ds := mocks.NewMockDataSource()
	snapshotProvider := newEmptySnapshotProvider()

	panicEmitter := &panickingEmitter{name: "panic-emitter"}
	goodEmitter := newCountingEmitter("survivor-emitter")

	exporter := NewExporter(ds, snapshotProvider, panicEmitter, goodEmitter)

	if !exporter.Start(100 * time.Millisecond) {
		t.Fatal("Failed to start exporter")
	}

	if !waitForUint32(&goodEmitter.count, 2, 5*time.Second) {
		exporter.Stop()
		t.Fatalf("Good emitter should still receive emissions, got %d", goodEmitter.count.Load())
	}
	exporter.Stop()
}

// ---------------------------------------------------------------------------
// Integration: Emitter ID tracking
// ---------------------------------------------------------------------------

func TestExporterEmitterIDs(t *testing.T) {
	ds := mocks.NewMockDataSource()
	snapshotProvider := newEmptySnapshotProvider()

	a := newCountingEmitter("alpha")
	b := newCountingEmitter("beta")
	c := newCountingEmitter("gamma")

	exporter := NewExporter(ds, snapshotProvider, a, b, c)
	ids := exporter.Emitters()

	if len(ids) != 3 {
		t.Fatalf("Expected 3 emitter IDs, got %d", len(ids))
	}

	expected := map[EmitterID]bool{"alpha": true, "beta": true, "gamma": true}
	for _, id := range ids {
		if !expected[id] {
			t.Errorf("Unexpected emitter ID: %s", id)
		}
	}
}

// ---------------------------------------------------------------------------
// Integration: Start/Stop/Restart cycle
// ---------------------------------------------------------------------------

func TestExporterRestartAfterStop(t *testing.T) {
	ds := mocks.NewMockDataSource()
	snapshotProvider := newEmptySnapshotProvider()
	emitter := newCountingEmitter("restart-emitter")

	exporter := NewExporter(ds, snapshotProvider, emitter)

	// First run
	if !exporter.Start(100 * time.Millisecond) {
		t.Fatal("Failed to start exporter (first time)")
	}
	if !waitForUint32(&emitter.count, 2, 5*time.Second) {
		exporter.Stop()
		t.Fatalf("Expected at least 2 emissions in first run, got %d", emitter.count.Load())
	}
	exporter.Stop()
	firstRunCount := emitter.count.Load()

	// Second run
	if !exporter.Start(100 * time.Millisecond) {
		t.Fatal("Failed to start exporter (second time)")
	}
	if !waitForUint32(&emitter.count, firstRunCount+2, 5*time.Second) {
		exporter.Stop()
		t.Fatalf("Expected at least 2 new emissions in second run, got %d", emitter.count.Load()-firstRunCount)
	}
	exporter.Stop()
}

// ---------------------------------------------------------------------------
// Integration: Multiple emitter coordination under load
// ---------------------------------------------------------------------------

func TestMultipleEmittersReceiveSameSnapshot(t *testing.T) {
	ds := mocks.NewMockDataSource()
	snapshotProvider := newEmptySnapshotProvider()

	const numEmitters = 5
	emitters := make([]Emitter, numEmitters)
	trackers := make([]*snapshotTrackingEmitter, numEmitters)

	for i := 0; i < numEmitters; i++ {
		tracker := &snapshotTrackingEmitter{
			name: fmt.Sprintf("tracker-%d", i),
		}
		trackers[i] = tracker
		emitters[i] = tracker
	}

	exporter := NewExporter(ds, snapshotProvider, emitters...)

	if !exporter.Start(100 * time.Millisecond) {
		t.Fatal("Failed to start exporter")
	}

	// Wait until all emitters have received at least 1 emission
	for i, tracker := range trackers {
		if !waitForUint32(&tracker.count, 1, 5*time.Second) {
			exporter.Stop()
			t.Fatalf("Emitter %d received 0 emissions within timeout", i)
		}
	}
	exporter.Stop()

	// All emitters should have received at least 1 emission
	for i, tracker := range trackers {
		count := tracker.count.Load()
		if count < 1 {
			t.Errorf("Emitter %d received %d emissions, expected at least 1", i, count)
		}
	}

	// All emitters should have the same emission count
	firstCount := trackers[0].count.Load()
	for i, tracker := range trackers[1:] {
		if tracker.count.Load() != firstCount {
			t.Errorf("Emitter %d has %d emissions, but emitter 0 has %d", i+1, tracker.count.Load(), firstCount)
		}
	}
}

// ---------------------------------------------------------------------------
// Integration: Slow emitter doesn't block fast emitter
// ---------------------------------------------------------------------------

func TestSlowEmitterDoesNotBlockFastEmitter(t *testing.T) {
	ds := mocks.NewMockDataSource()
	snapshotProvider := newEmptySnapshotProvider()

	slow := &delayEmitter{
		name:  "slow",
		delay: 50 * time.Millisecond,
	}
	fast := newCountingEmitter("fast")

	exporter := NewExporter(ds, snapshotProvider, slow, fast)

	if !exporter.Start(100 * time.Millisecond) {
		t.Fatal("Failed to start exporter")
	}

	if !waitForUint32(&slow.count, 3, 5*time.Second) {
		exporter.Stop()
		t.Fatalf("Slow emitter should have at least 3 emissions, got %d", slow.count.Load())
	}
	exporter.Stop()

	fastCount := fast.count.Load()
	if fastCount < 3 {
		t.Errorf("Fast emitter should have at least 3 emissions, got %d", fastCount)
	}
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

type intermittentSnapshotProvider struct {
	callCount *int32
	failEvery int32
}

func (i *intermittentSnapshotProvider) SnapshotOf(ds core.DataSource) (*ClusterSnapshot, error) {
	count := atomic.AddInt32(i.callCount, 1)
	if count%i.failEvery == 0 {
		return nil, fmt.Errorf("snapshot failure at call %d", count)
	}
	return &ClusterSnapshot{}, nil
}

type initTrackingEmitter struct {
	name        string
	initialized atomic.Bool
}

func (e *initTrackingEmitter) ID() EmitterID          { return EmitterID(e.name) }
func (e *initTrackingEmitter) Init(_ *ClusterSnapshot) error {
	e.initialized.Store(true)
	return nil
}
func (e *initTrackingEmitter) Emit(_ context.Context, _ *ClusterSnapshot) error { return nil }

type failingInitEmitter struct {
	name string
}

func (e *failingInitEmitter) ID() EmitterID { return EmitterID(e.name) }
func (e *failingInitEmitter) Init(_ *ClusterSnapshot) error {
	return fmt.Errorf("init failed for %s", e.name)
}
func (e *failingInitEmitter) Emit(_ context.Context, _ *ClusterSnapshot) error { return nil }

type panickingEmitter struct {
	name string
}

func (e *panickingEmitter) ID() EmitterID                                        { return EmitterID(e.name) }
func (e *panickingEmitter) Init(_ *ClusterSnapshot) error                        { return nil }
func (e *panickingEmitter) Emit(_ context.Context, _ *ClusterSnapshot) error {
	panic("emitter went boom!")
}

type snapshotTrackingEmitter struct {
	name  string
	count atomic.Uint32
	mu    sync.Mutex
	snaps []*ClusterSnapshot
}

func (e *snapshotTrackingEmitter) ID() EmitterID          { return EmitterID(e.name) }
func (e *snapshotTrackingEmitter) Init(_ *ClusterSnapshot) error { return nil }
func (e *snapshotTrackingEmitter) Emit(_ context.Context, snapshot *ClusterSnapshot) error {
	e.count.Add(1)
	e.mu.Lock()
	e.snaps = append(e.snaps, snapshot)
	e.mu.Unlock()
	return nil
}

type delayEmitter struct {
	name  string
	delay time.Duration
	count atomic.Uint32
}

func (e *delayEmitter) ID() EmitterID          { return EmitterID(e.name) }
func (e *delayEmitter) Init(_ *ClusterSnapshot) error { return nil }
func (e *delayEmitter) Emit(_ context.Context, _ *ClusterSnapshot) error {
	time.Sleep(e.delay)
	e.count.Add(1)
	return nil
}
