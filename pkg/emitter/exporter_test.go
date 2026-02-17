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
	"github.com/opencost/opencost/pkg/cloud/models"

	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
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

// NOTE: This test explicitly tests the logging output for emit calls within the exporter.
// NOTE: It tests a very specific scenario just to conclude that the warning is being written.
func TestExporterEmissionBackPressure(t *testing.T) {
	initLogging(t, "debug", false)

	// use a single log writer so we can examine the logs after each query
	logWriter := new(SingleLogWriter)
	zerologger.Logger = zerologger.Output(zerolog.ConsoleWriter{
		Out:        logWriter,
		TimeFormat: "",
		NoColor:    true,
		PartsExclude: []string{
			zerolog.TimestampFieldName,
			zerolog.LevelFieldName,
			zerolog.CallerFieldName,
		},
	})

	// reinitialize logging when tests are complete
	defer initLogging(t, "debug", false)

	ds := newEmptyDataSource()
	snapshotProvider := newEmptySnapshotProvider()
	a := newWorkSimulatingEmitterWithID("slow-emitter", 3*time.Second)
	b := newWorkSimulatingEmitterWithID("normal-emitter", 250*time.Millisecond)

	exporter := NewExporter(ds, snapshotProvider, a, b)

	if !exporter.Start(500 * time.Millisecond) {
		t.Fatal("Failed to start exporter")
	}

	// wait for at least one emitter to generate a warning
	time.Sleep(3*time.Second + 500*time.Millisecond)

	// get log output from executing query
	output := logWriter.Log
	if len(output) == 0 {
		t.Errorf("Expected at least one warning log related to emission counts falling behind")
	}
	output = output[:len(output)-1]

	expectedWarning := "Number of concurrent emission tasks has reached 6 - We are still attempting to emit data with a snapshot of age: 3 seconds"
	if output != expectedWarning {
		t.Errorf("Output of:\n\"%s\"\nis not equivalent to:\n\"%s\"", output, expectedWarning)
	}
}

//--------------------------------------------------------------------------
//  helpers
//--------------------------------------------------------------------------

func initLogging(t *testing.T, logLevel string, colorEnabled bool) {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerologger.Logger = zerologger.Output(zerolog.ConsoleWriter{
		Out:        zerolog.NewTestWriter(t),
		TimeFormat: time.RFC3339Nano,
		NoColor:    !colorEnabled,
	})

	logLevelParsed, err := zerolog.ParseLevel(logLevel)
	if err != nil {
		logLevelParsed = zerolog.DebugLevel
	}

	zerolog.SetGlobalLevel(logLevelParsed)
}

type SingleLogWriter struct {
	Log string
}

// Write to testing.TB.
func (slw *SingleLogWriter) Write(p []byte) (n int, err error) {
	err = nil
	n = len(p)

	slw.Log = string(p)
	return
}

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

var idCount atomic.Uint64

type workSimulatingEmitter struct {
	id           EmitterID
	workDuration time.Duration
	isCancelled  atomic.Bool
}

func (wse *workSimulatingEmitter) ID() EmitterID {
	return wse.id
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

func (e *emptyDataSource) OpenCostCloudCostProvider() models.Provider {
	return nil
}

func (e *emptyDataSource) Metrics() source.MetricsQuerier {
	return nil
}

func (e *emptyDataSource) Cluster() cluster.ClusterCache {
	return nil
}

func (e *emptyDataSource) StatsSummary() nodes.StatSummaryClient {
	return nil
}

func (e *emptyDataSource) ClusterMetadata() cluster.Metadata {
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
		id:           EmitterID(fmt.Sprintf("work-simulating-emitter-%d", idCount.Add(1))),
		workDuration: workDuration,
	}
}

func newWorkSimulatingEmitterWithID(id string, workDuration time.Duration) *workSimulatingEmitter {
	return &workSimulatingEmitter{
		id:           EmitterID(id),
		workDuration: workDuration,
	}
}

func newEmptyDataSource() *emptyDataSource {
	return &emptyDataSource{}
}

func newEmptySnapshotProvider() *emptySnapshotProvider {
	return &emptySnapshotProvider{}
}
