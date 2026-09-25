package emitter

// The exporter's measurements for metrics (docs/reliability/FINDINGS.md chunk 09):
// finops_agent_emit_total and finops_agent_snapshot_duration_seconds.

import (
	"sync"
	"testing"
	"time"
)

type recordingObserver struct {
	mu        sync.Mutex
	snapshots int
	calls     map[EmitterID]map[string]int
}

func (o *recordingObserver) SnapshotDuration(time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.snapshots++
}

func (o *recordingObserver) EmitterCall(id EmitterID, result string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.calls == nil {
		o.calls = map[EmitterID]map[string]int{}
	}
	if o.calls[id] == nil {
		o.calls[id] = map[string]int{}
	}
	o.calls[id][result]++
}

func (o *recordingObserver) SnapshotComponentFailed(string) {}

func (o *recordingObserver) count(id EmitterID, result string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.calls[id][result]
}

// Every Init and Emit call is observed once with its result, and a cycle that skips an emitter
// whose call is still running is observed as skipped; a call that outlives its deadline is
// observed once, as an error, and not again when it returns.
func TestExporterObservesEmitterCalls(t *testing.T) {
	obs := &recordingObserver{}
	steady := newFailingCountingEmitter("steady", 2) // every second Emit fails
	hung := &blockingEmitter{release: make(chan struct{})}

	exporter := NewExporterWithConfig(newEmptyDataSource(), newEmptySnapshotProvider(), ExporterConfig{
		EmitTimeout: 30 * time.Millisecond,
		StopGrace:   10 * time.Millisecond,
		Observer:    obs,
	}, steady, hung)
	if !exporter.Start(20 * time.Millisecond) {
		t.Fatal("failed to start exporter")
	}
	if !waitFor(3*time.Second, func() bool {
		return steady.count.Load() >= 6 && obs.count("blocking", EmitResultSkipped) >= 2
	}) {
		exporter.Stop()
		t.Fatalf("exporter didn't run: %d steady emits, observed %v", steady.count.Load(), obs.calls)
	}
	exporter.Stop()
	close(hung.release)

	emits := int(steady.count.Load())
	ok, failed := obs.count("steady", EmitResultOK), obs.count("steady", EmitResultError)
	// ok includes the one Init call; the observer may trail a call that returned during Stop
	if ok+failed < emits || ok+failed > emits+1 || failed < emits/2-1 || failed > emits/2+1 {
		t.Errorf("steady: %d emits (every second one failing) observed as %d ok and %d error", emits, ok, failed)
	}
	if got := obs.count("blocking", EmitResultError); got != 1 {
		t.Errorf("the hung Emit was observed as an error %d times; want once, at its deadline", got)
	}
	if obs.count("blocking", EmitResultOK) != 1 { // its Init
		t.Errorf("blocking: observed %v; want its Init once as ok", obs.calls["blocking"])
	}
	obs.mu.Lock()
	snapshots := obs.snapshots
	obs.mu.Unlock()
	if snapshots < emits {
		t.Errorf("observed %d snapshot durations for %d cycles", snapshots, emits)
	}
}
