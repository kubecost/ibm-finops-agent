package emitter

import (
	"context"
	"fmt"
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
		runContext, cancelRun := context.WithCancel(context.Background())
		defer cancelRun()
		defer de.runState.Reset()

		// take a snapshot of the current cluster state
		snapshot, err := de.snapshotProvider.SnapshotOf(de.ds)
		if err != nil {
			log.Errorf("failed to take snapshot for initialization phase: %v", err)
			return
		}

		for _, emitter := range de.emitters {
			if err := emitter.Init(snapshot); err != nil {
				log.Errorf("failed to initialize emitter %s: %v", emitter.ID(), err)
			}
		}

		for {
			// use a select statement to receive whichever channel receives data first
			select {
			// if our stop channel receives data, it means we have explicitly called
			// Stop(), and must reset our AtomicRunState to it's initial idle state
			case <-de.runState.OnStop():
				cancelRun()
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

				// emit the snapshot to all registered emitters -- note that the emitters run serially,
				// each blocking until emission is complete. This gives us a _little_ bit of flexibility
				// in the emission process at the cost of performance. We can definitely iterate on this.
				for _, emitter := range de.emitters {
					if err := emit(runContext, emitter, snapshot); err != nil {
						log.Errorf("[%s] failed to emit snapshot: %v", emitter.ID(), err)
					}
				}
			}()
		}
	}()

	return true
}

// emit is helper function that handles the emission of a snapshot to a specific emitter
// and captures and recovers from any panics that occur during the emission process.
func emit(ctx context.Context, emitter Emitter, snapshot *ClusterSnapshot) (err error) {
	// panics are recovered and propagated as errors
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = fmt.Errorf("unexpected panic: %w", e)
			} else if s, ok := r.(string); ok {
				err = fmt.Errorf("unexpected panic: %s", s)
			} else {
				err = fmt.Errorf("unexpected panic: %+v", r)
			}
		}
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Currently, each emitter will block until the emission is complete. This can easily
	// be changed later to allow for non-blocking emissions, but we'll have to ensure that
	// the shared ClusterSnapshot remains immutable during the emission process.
	err = emitter.Emit(ctx, snapshot)
	return
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
