package emitter

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ibm/finops-agent/pkg/cluster"
	"github.com/ibm/finops-agent/pkg/core"
	"github.com/ibm/finops-agent/pkg/nodes"
	"github.com/opencost/opencost/core/pkg/source"
)

func TestExporterProcess(t *testing.T) {
	ds := newEmptyDataSource()
	snapshotProvider := newEmptySnapshotProvider()
	a, b, c := newCountingEmitter("emitter-a"), newCountingEmitter("emitter-b"), newCountingEmitter("emitter-c")

	exporter := NewExporter(ds, snapshotProvider, a, b, c)
	if !exporter.Start(250 * time.Millisecond) {
		t.Fatal("Failed to start exporter")
	}

	time.Sleep(1400 * time.Millisecond) // wait for 5 emissions
	exporter.Stop()

	if a.count.Load() != 5 {
		t.Errorf("Expected emitter-a to be called 5 times, got %d", a.count.Load())
	}
	if b.count.Load() != 5 {
		t.Errorf("Expected emitter-b to be called 5 times, got %d", b.count.Load())
	}
	if c.count.Load() != 5 {
		t.Errorf("Expected emitter-c to be called 5 times, got %d", c.count.Load())
	}

	// delay a bit more to ensure that no more emissions are made after stopping
	time.Sleep(500 * time.Millisecond)
	if a.count.Load() != 5 {
		t.Errorf("Expected no more emissions after stop. Emitted additional %d times.", a.count.Load()-5)
	}
	if b.count.Load() != 5 {
		t.Errorf("Expected no more emissions after stop. Emitted additional %d times.", b.count.Load()-5)
	}
	if c.count.Load() != 5 {
		t.Errorf("Expected no more emissions after stop. Emitted additional %d times.", c.count.Load()-5)
	}
}

func TestExporterProcessWithFailures(t *testing.T) {
	ds := newEmptyDataSource()
	snapshotProvider := newEmptySnapshotProvider()
	a, b := newCountingEmitter("emitter-a"), newFailingCountingEmitter("emitter-b", 2)

	exporter := NewExporter(ds, snapshotProvider, a, b)
	if !exporter.Start(250 * time.Millisecond) {
		t.Fatal("Failed to start exporter")
	}

	time.Sleep(1400 * time.Millisecond) // wait for 5 emissions
	exporter.Stop()

	// We still check the counts to ensure we're running 5 emissions
	if a.count.Load() != 5 {
		t.Errorf("Expected emitter-a to be called 5 times, got %d", a.count.Load())
	}
	if b.count.Load() != 5 {
		t.Errorf("Expected emitter-b to be called 5 times, got %d", b.count.Load())
	}
}

func TestExporterProcessWithCancellation(t *testing.T) {
	ds := newEmptyDataSource()
	snapshotProvider := newEmptySnapshotProvider()
	a := newWorkSimulatingEmitter(750 * time.Millisecond)

	exporter := NewExporter(ds, snapshotProvider, a)

	if !exporter.Start(time.Second) {
		t.Fatal("Failed to start exporter")
	}

	time.Sleep((2 * time.Second) + (250 * time.Millisecond))
	fmt.Println("Stopping exporter...")
	exporter.Stop()

	// allow cancellation to propagate
	time.Sleep(50 * time.Millisecond)

	if !a.isCancelled.Load() {
		t.Errorf("Expected emitter to be cancelled, but it was not.")
	}
}

//--------------------------------------------------------------------------
//  helpers
//--------------------------------------------------------------------------

type countingEmitter struct {
	count  atomic.Uint32
	name   string
	failOn uint
}

func (ce *countingEmitter) ID() EmitterID {
	return EmitterID(ce.name)
}

func (ce *countingEmitter) Init(snapshot *ClusterSnapshot) error {
	return nil
}

func (ce *countingEmitter) Emit(ctx context.Context, snapshot *ClusterSnapshot) error {
	val := ce.count.Add(1)
	if ce.failOn > 0 && val%uint32(ce.failOn) == 0 {
		return fmt.Errorf("failed to emit data!")
	}
	return nil
}

type workSimulatingEmitter struct {
	workDuration time.Duration
	isCancelled  atomic.Bool
}

func (wse *workSimulatingEmitter) ID() EmitterID {
	return EmitterID("work-simulating-emitter")
}

func (wse *workSimulatingEmitter) Init(snapshot *ClusterSnapshot) error {
	return nil
}

func (wse *workSimulatingEmitter) Emit(ctx context.Context, snapshot *ClusterSnapshot) error {
	fmt.Println("Simulating work for emitter...")

	select {
	case <-ctx.Done():
		fmt.Println("Work simulation cancelled")
		wse.isCancelled.Store(true)
		return nil
	case <-time.After(wse.workDuration):
		fmt.Println("Work simulation complete")
		return nil
	}
}

type emptySnapshotProvider struct{}

func (e *emptySnapshotProvider) SnapshotOf(ds core.DataSource) (*ClusterSnapshot, error) {
	return &ClusterSnapshot{}, nil
}

type emptyDataSource struct{}

func (e *emptyDataSource) OpenCostSource() source.OpenCostDataSource {
	return nil
}

func (e *emptyDataSource) Metrics() source.MetricsQuerier {
	return nil
}

func (e *emptyDataSource) Cluster() cluster.ClusterCache {
	return nil
}

func (e *emptyDataSource) StatsSummary() nodes.NodeClient {
	return nil
}

func newCountingEmitter(name string) *countingEmitter {
	return &countingEmitter{
		name: name,
	}
}

func newFailingCountingEmitter(name string, failOn uint) *countingEmitter {
	return &countingEmitter{
		name:   name,
		failOn: failOn,
	}
}

func newWorkSimulatingEmitter(workDuration time.Duration) *workSimulatingEmitter {
	return &workSimulatingEmitter{
		workDuration: workDuration,
	}
}

func newEmptyDataSource() *emptyDataSource {
	return &emptyDataSource{}
}

func newEmptySnapshotProvider() *emptySnapshotProvider {
	return &emptySnapshotProvider{}
}
