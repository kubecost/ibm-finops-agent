package emitter

import (
	"time"

	"github.com/ibm/finops-agent/pkg/core"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/atomic"
)

// Exporter is an interface that defines a data emission management system and facilitates the
// snapshot and distribution of those cluster snapshots to the management emitters.
type Exporter interface {
	// Start begins the emission process, running the snapshot and emission processes on the
	// specified interval. If the process starts successfully, it returns true. Otherwise, if the
	// process is already started, it returns false.
	Start(interval time.Duration) bool

	// Stop halts the emission process.
	Stop()

	// Emitters returns a list of the `EmitterID`s registered within the exporter.
	Emitters() []EmitterID
}

// defaultExporter is the default implementation of the Exporter interface. It's a straight-forward
// snapshot and emission loop that runs on a specified interval.
type defaultExporter struct {
	runState         atomic.AtomicRunState
	ds               core.DataSource
	snapshotProvider SnapshotProvider
	emitters         []Emitter
}

func NewExporter(ds core.DataSource, snapshotProvider SnapshotProvider, emitters ...Emitter) Exporter {
	return &defaultExporter{
		ds:               ds,
		snapshotProvider: snapshotProvider,
		emitters:         emitters,
	}
}

// Start beings the emission process on the specified interval. It will run until Stop() is called.
func (de *defaultExporter) Start(interval time.Duration) bool {
	// Before we attempt to start, we must ensure we are not in a stopping state
	de.runState.WaitForReset()

	// This will atomically check the current state to ensure we can run, then advances the state.
	// If the state is already started, it will return false.
	if !de.runState.Start() {
		return false
	}

	// spawn a new goroutine which will loop and wait the interval each iteration
	go func() {
		for {
			// use a select statement to receive whichever channel receives data first
			select {
			// if our stop channel receives data, it means we have explicitly called
			// Stop(), and must reset our AtomicRunState to it's initial idle state
			case <-de.runState.OnStop():
				de.runState.Reset()
				return // exit go routine

			// After our interval elapses, fall through
			case <-time.After(interval):
			}

			// Kick off a new goroutine to snapshot and emit the data, this will allow the interval to reset
			// immediately and not wait for the snapshot and emission to complete
			go func() {
				// take a snapshot of the current cluster state
				snapshot, err := de.snapshotProvider.SnapshotOf(de.ds)
				if err != nil {
					log.Errorf("failed to take snapshot: %v", err)
					return
				}

				// emit the snapshot to all registered emitters
				for _, emitter := range de.emitters {
					if err := emitter.Emit(snapshot); err != nil {
						log.Errorf("failed to emit snapshot: %v", err)
					}
				}
			}()
		}
	}()

	return true
}

// Stop halts the emission process from running any further emissions, but may not halt
// any emissions that are currently in progress.
func (de *defaultExporter) Stop() {
	de.runState.Stop()
}

// Emitters returns a list of the `EmitterID`s registered within the exporter.
func (de *defaultExporter) Emitters() []EmitterID {
	ids := make([]EmitterID, 0, len(de.emitters))
	for _, emitter := range de.emitters {
		ids = append(ids, emitter.ID())
	}
	return ids
}
