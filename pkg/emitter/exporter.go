package emitter

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ibm/finops-agent/pkg/core"
	"github.com/opencost/opencost/core/pkg/log"
)

// Exporter is an interface that defines a data emission management system and facilitates the
// snapshot and distribution of those cluster snapshots to the management emitters.
type Exporter interface {
	// Start begins the emission process, running the snapshot and emission processes on the
	// specified interval. If the process starts successfully, it returns true. Otherwise, if the
	// process is already started or the interval isn't positive, it returns false.
	Start(interval time.Duration) bool

	// Stop halts the emission process and waits for the exporter loop to exit.
	Stop()

	// Emitters returns a list of the `EmitterID`s registered within the exporter.
	Emitters() []EmitterID

	Heartbeat
}

// Heartbeat exposes the exporter loop's liveness and per-emitter state to health checks,
// metrics and status reporting.
type Heartbeat interface {
	// Stalled reports whether the exporter loop has stopped making progress: a cycle started and
	// hasn't ended within its deadlines plus slack, or no cycle has started within two intervals
	// plus slack of the previous one ending (or of Start). Snapshot and emitter failures do not make the loop stalled; they are in Status.
	Stalled(now time.Time) bool

	// Status returns a copy of the exporter's current state.
	Status() ExporterStatus
}

// EmitterState is an emitter's lifecycle state within the exporter.
type EmitterState string

const (
	// EmitterUninitialised emitters have not yet had a successful Init. Init is retried every
	// cycle and Emit is not called.
	EmitterUninitialised EmitterState = "uninitialised"
	// EmitterReady emitters have had a successful Init, which is never called again. Emit is
	// called every cycle.
	EmitterReady EmitterState = "ready"
)

// EmitterStatus is the exporter's view of one emitter.
type EmitterStatus struct {
	ID    EmitterID
	State EmitterState
	// Busy is true while a call to Init or Emit is still running. A call that outlives its
	// deadline keeps the emitter busy, and the emitter is skipped until the call returns.
	Busy            bool
	LastInitError   error
	LastEmitSuccess time.Time
	// LastEmitError is the last failure of either call (Init, Emit, a deadline or a skip), and
	// ConsecutiveFailures counts failed cycles since the last success.
	LastEmitError       error
	LastEmitErrorTime   time.Time
	ConsecutiveFailures int
}

// ExporterStatus is a snapshot of the exporter's heartbeats and counters. The *Total counters
// only ever increase for the life of the process.
type ExporterStatus struct {
	Running                     bool
	Interval                    time.Duration
	SnapshotTimeout             time.Duration
	EmitTimeout                 time.Duration
	LastCycleStart              time.Time
	LastCycleEnd                time.Time
	LastSnapshotSuccess         time.Time
	LastSnapshotError           error
	ConsecutiveSnapshotFailures int

	CyclesTotal           uint64
	CycleOverrunsTotal    uint64
	SnapshotTimeoutsTotal uint64
	EmitTimeoutsTotal     uint64
	// AbandonedShortLivedPodsTotal counts short-lived pods drained into snapshots that completed
	// after their deadline and were discarded.
	AbandonedShortLivedPodsTotal uint64

	Emitters []EmitterStatus
}

// ExporterConfig configures the exporter's deadlines and retry behaviour. Zero values are
// replaced by defaults derived from the interval passed to Start.
type ExporterConfig struct {
	// SnapshotTimeout bounds one snapshot. Default DefaultTimeoutIntervals × interval.
	SnapshotTimeout time.Duration
	// EmitTimeout bounds one Init or Emit call on one emitter. Default
	// DefaultTimeoutIntervals × interval.
	EmitTimeout time.Duration
	// InitialBackoff is the first retry delay while no snapshot has succeeded yet. Default
	// min(1s, interval).
	InitialBackoff time.Duration
	// MaxBackoff caps the retry delay while no snapshot has succeeded yet. Default interval.
	MaxBackoff time.Duration
	// StallSlack is added to the Stalled thresholds. Default interval.
	StallSlack time.Duration
	// StopGrace is how long Stop waits for in-flight Init and Emit calls to return after their
	// context is cancelled. Default 5s.
	StopGrace time.Duration
	// Now is the clock for heartbeat timestamps. Default time.Now.
	Now func() time.Time
}

// DefaultTimeoutIntervals is the default snapshot and emit deadline, in exporter intervals.
// It's generous on purpose: the deadline exists to end hung cycles, not to cut slow ones short.
const DefaultTimeoutIntervals = 5

// maxSnapshotsInFlight bounds the snapshots running at once: the current one plus one that
// outlived its deadline. While both are running, new cycles fail without starting another.
const maxSnapshotsInFlight = 2

func (c ExporterConfig) withDefaults(interval time.Duration) ExporterConfig {
	if c.SnapshotTimeout <= 0 {
		c.SnapshotTimeout = DefaultTimeoutIntervals * interval
	}
	if c.EmitTimeout <= 0 {
		c.EmitTimeout = DefaultTimeoutIntervals * interval
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = interval
	}
	if c.InitialBackoff <= 0 {
		c.InitialBackoff = min(time.Second, c.MaxBackoff)
	}
	if c.StallSlack <= 0 {
		c.StallSlack = interval
	}
	if c.StopGrace <= 0 {
		c.StopGrace = 5 * time.Second
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return c
}

// ContextSnapshotProvider is a SnapshotProvider that honours a context. The exporter uses it
// when the provider implements it, so components that support cancellation stop at the
// snapshot deadline.
type ContextSnapshotProvider interface {
	SnapshotProvider
	SnapshotOfContext(context.Context, core.DataSource) (*ClusterSnapshot, error)
}

// defaultExporter is the default implementation of the Exporter interface: a supervised
// snapshot and emission loop that runs on a specified interval until Stop is called.
//
// The loop never exits on a failure. Snapshots and emitter calls run under deadlines; a call
// that outlives its deadline is abandoned (it keeps running in its own goroutine and is waited
// for before the same work is started again), so one hung component can't stop the loop or
// the other emitters.
type defaultExporter struct {
	ds               core.DataSource
	snapshotProvider SnapshotProvider
	config           ExporterConfig
	slots            []*emitterSlot

	// lifecycle serialises Start and Stop.
	lifecycle sync.Mutex
	cancel    context.CancelFunc
	done      chan struct{}

	snapshotsInFlight atomic.Int32

	// mu guards the heartbeat fields below and every slot's status.
	mu                          sync.Mutex
	running                     bool
	inCycle                     bool
	interval                    time.Duration
	effective                   ExporterConfig
	startedAt                   time.Time
	lastCycleStart              time.Time
	lastCycleEnd                time.Time
	lastSnapshotSuccess         time.Time
	lastSnapshotError           error
	consecutiveSnapshotFailures int

	cycles, overruns, snapshotTimeouts, emitTimeouts, abandonedShortLivedPods atomic.Uint64
}

// emitterSlot is the exporter's per-emitter state. busy is set while a call is running; status
// is guarded by the exporter's mu.
type emitterSlot struct {
	emitter Emitter
	busy    atomic.Bool
	status  EmitterStatus
}

// NewExporter creates an exporter with default deadlines.
func NewExporter(ds core.DataSource, snapshotProvider SnapshotProvider, emitters ...Emitter) Exporter {
	return NewExporterWithConfig(ds, snapshotProvider, ExporterConfig{}, emitters...)
}

// NewExporterWithConfig creates an exporter with the given deadlines and retry settings.
func NewExporterWithConfig(ds core.DataSource, snapshotProvider SnapshotProvider, config ExporterConfig, emitters ...Emitter) Exporter {
	slots := make([]*emitterSlot, 0, len(emitters))
	for _, e := range emitters {
		slots = append(slots, &emitterSlot{
			emitter: e,
			status:  EmitterStatus{ID: e.ID(), State: EmitterUninitialised},
		})
	}
	return &defaultExporter{
		ds:               ds,
		snapshotProvider: snapshotProvider,
		config:           config,
		slots:            slots,
	}
}

// Start begins the emission process on the specified interval. It will run until Stop() is called.
func (de *defaultExporter) Start(interval time.Duration) bool {
	de.lifecycle.Lock()
	defer de.lifecycle.Unlock()

	if de.done != nil {
		return false
	}
	if interval <= 0 {
		log.Errorf("exporter interval must be positive, got %s", interval)
		return false
	}

	cfg := de.config.withDefaults(interval)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	de.cancel, de.done = cancel, done

	de.mu.Lock()
	de.running = true
	de.interval = interval
	de.effective = cfg
	de.startedAt = cfg.Now()
	de.mu.Unlock()

	go func() {
		defer close(done)
		de.run(ctx, interval, cfg)
	}()

	return true
}

// Stop halts the emission process and waits for the loop to exit. Init and Emit calls in flight
// have their context cancelled and are waited for up to StopGrace.
func (de *defaultExporter) Stop() {
	de.lifecycle.Lock()
	defer de.lifecycle.Unlock()

	if de.done == nil {
		return
	}
	de.cancel()
	<-de.done
	de.cancel, de.done = nil, nil

	de.mu.Lock()
	de.running = false
	de.mu.Unlock()
}

// Emitters returns a list of the `EmitterID`s registered within the exporter.
func (de *defaultExporter) Emitters() []EmitterID {
	ids := make([]EmitterID, 0, len(de.slots))
	for _, slot := range de.slots {
		ids = append(ids, slot.emitter.ID())
	}
	return ids
}

// run is the exporter loop. Until the first snapshot succeeds, cycles are retried with capped
// exponential backoff and jitter; after that they run on the interval. It returns only when ctx
// is cancelled.
func (de *defaultExporter) run(ctx context.Context, interval time.Duration, cfg ExporterConfig) {
	backoff := cfg.InitialBackoff
	for !de.cycle(ctx, cfg) {
		// full jitter over [backoff/2, backoff)
		delay := backoff/2 + rand.N(backoff/2+1)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		backoff = min(2*backoff, cfg.MaxBackoff)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		start := time.Now()
		de.cycle(ctx, cfg)

		// On overrun, skip the ticks that were missed rather than running them back to back.
		if elapsed := time.Since(start); elapsed > interval && ctx.Err() == nil {
			missed := uint64(elapsed / interval)
			de.overruns.Add(missed)
			select {
			case <-ticker.C:
			default:
			}
			log.Warnf("exporter cycle took %s, longer than the %s interval; skipped %d tick(s)", elapsed, interval, missed)
		}
	}
}

// cycle takes one snapshot and makes one call on each emitter: Init if it isn't ready yet,
// otherwise Emit. It reports whether the snapshot succeeded.
func (de *defaultExporter) cycle(ctx context.Context, cfg ExporterConfig) bool {
	de.cycles.Add(1)
	de.mu.Lock()
	de.lastCycleStart = cfg.Now()
	de.inCycle = true
	de.mu.Unlock()
	defer func() {
		de.mu.Lock()
		de.lastCycleEnd = cfg.Now()
		de.inCycle = false
		de.mu.Unlock()
	}()

	snapshot, err := de.takeSnapshot(ctx, cfg)
	if ctx.Err() != nil {
		return false
	}

	de.mu.Lock()
	if err != nil {
		de.lastSnapshotError = err
		de.consecutiveSnapshotFailures++
		failures := de.consecutiveSnapshotFailures
		de.mu.Unlock()
		log.Errorf("failed to take snapshot (%d consecutive failures): %v", failures, err)
		return false
	}
	if de.consecutiveSnapshotFailures > 0 {
		log.Infof("snapshot succeeded after %d consecutive failures", de.consecutiveSnapshotFailures)
	}
	de.lastSnapshotSuccess = cfg.Now()
	de.lastSnapshotError = nil
	de.consecutiveSnapshotFailures = 0
	de.mu.Unlock()

	var calls sync.WaitGroup
	for _, slot := range de.slots {
		if !slot.busy.CompareAndSwap(false, true) {
			de.recordEmitterFailure(slot, cfg, errors.New("previous Init or Emit is still running past its deadline; skipped this cycle"))
			continue
		}
		calls.Go(func() { de.callEmitter(ctx, cfg, slot, snapshot) })
	}
	calls.Wait()

	return true
}

// snapshotCall is one snapshot attempt. The goroutine running it and the cycle waiting on it
// agree through mu on whether the result was delivered or abandoned.
type snapshotCall struct {
	mu        sync.Mutex
	finished  bool
	abandoned bool
	snapshot  *ClusterSnapshot
	err       error
	done      chan struct{}
}

// takeSnapshot runs one snapshot under the snapshot deadline. A snapshot that outlives its
// deadline is abandoned: the cycle fails, and the snapshot's goroutine discards its result
// when it eventually returns.
func (de *defaultExporter) takeSnapshot(ctx context.Context, cfg ExporterConfig) (*ClusterSnapshot, error) {
	if n := de.snapshotsInFlight.Load(); n >= maxSnapshotsInFlight {
		return nil, fmt.Errorf("%d snapshots are still running past their deadline; not starting another", n)
	}

	sctx, cancel := context.WithTimeout(ctx, cfg.SnapshotTimeout)
	defer cancel()

	call := &snapshotCall{done: make(chan struct{})}
	de.snapshotsInFlight.Add(1)
	go func() {
		defer de.snapshotsInFlight.Add(-1)
		snapshot, err := de.safeSnapshot(sctx)

		call.mu.Lock()
		defer call.mu.Unlock()
		call.finished = true
		call.snapshot, call.err = snapshot, err
		close(call.done)
		if call.abandoned && snapshot != nil && snapshot.Kubernetes != nil {
			if dropped := len(snapshot.Kubernetes.ShortLivedPods); dropped > 0 {
				de.abandonedShortLivedPods.Add(uint64(dropped))
				log.Errorf("discarded %d short-lived pods from a snapshot that completed after its deadline", dropped)
			}
		}
	}()

	select {
	case <-call.done:
	case <-sctx.Done():
	}

	call.mu.Lock()
	defer call.mu.Unlock()
	if call.finished {
		return call.snapshot, call.err
	}
	call.abandoned = true
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	de.snapshotTimeouts.Add(1)
	return nil, fmt.Errorf("snapshot exceeded its %s deadline: %w", cfg.SnapshotTimeout, context.DeadlineExceeded)
}

// safeSnapshot takes a snapshot, converting a panic into an error.
func (de *defaultExporter) safeSnapshot(ctx context.Context) (snapshot *ClusterSnapshot, err error) {
	defer func() {
		if r := recover(); r != nil {
			snapshot, err = nil, panicError(r)
		}
	}()
	if cp, ok := de.snapshotProvider.(ContextSnapshotProvider); ok {
		return cp.SnapshotOfContext(ctx, de.ds)
	}
	return de.snapshotProvider.SnapshotOf(de.ds)
}

// callEmitter makes one Init or Emit call on slot's emitter under the emit deadline. The
// caller has set slot.busy; the call's goroutine clears it when the call returns, however late.
func (de *defaultExporter) callEmitter(ctx context.Context, cfg ExporterConfig, slot *emitterSlot, snapshot *ClusterSnapshot) {
	de.mu.Lock()
	ready := slot.status.State == EmitterReady
	de.mu.Unlock()

	ectx, cancel := context.WithTimeout(ctx, cfg.EmitTimeout)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer slot.busy.Store(false)
		defer close(done)
		if ready {
			de.recordEmitResult(slot, cfg, emit(ectx, slot.emitter, snapshot))
		} else {
			de.recordInitResult(slot, cfg, initialise(slot.emitter, snapshot))
		}
	}()

	select {
	case <-done:
		return
	case <-ectx.Done():
	}

	if ctx.Err() != nil {
		// Stopping: give the call a bounded chance to observe cancellation and return.
		timer := time.NewTimer(cfg.StopGrace)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			log.Warnf("[%s] still running %s after Stop", slot.emitter.ID(), cfg.StopGrace)
		}
		return
	}

	de.emitTimeouts.Add(1)
	op := "Init"
	if ready {
		op = "Emit"
	}
	de.recordEmitterFailure(slot, cfg, fmt.Errorf("%s exceeded its %s deadline: %w", op, cfg.EmitTimeout, context.DeadlineExceeded))
}

func (de *defaultExporter) recordInitResult(slot *emitterSlot, cfg ExporterConfig, err error) {
	if err != nil {
		de.mu.Lock()
		slot.status.LastInitError = err
		de.mu.Unlock()
		de.recordEmitterFailure(slot, cfg, fmt.Errorf("failed to initialize emitter: %w", err))
		return
	}
	de.mu.Lock()
	slot.status.State = EmitterReady
	slot.status.LastInitError = nil
	slot.status.ConsecutiveFailures = 0
	de.mu.Unlock()
	log.Infof("[%s] emitter initialised", slot.emitter.ID())
}

func (de *defaultExporter) recordEmitResult(slot *emitterSlot, cfg ExporterConfig, err error) {
	if err != nil {
		de.recordEmitterFailure(slot, cfg, fmt.Errorf("failed to emit snapshot: %w", err))
		return
	}
	de.mu.Lock()
	slot.status.LastEmitSuccess = cfg.Now()
	slot.status.ConsecutiveFailures = 0
	de.mu.Unlock()
}

func (de *defaultExporter) recordEmitterFailure(slot *emitterSlot, cfg ExporterConfig, err error) {
	de.mu.Lock()
	slot.status.LastEmitError = err
	slot.status.LastEmitErrorTime = cfg.Now()
	slot.status.ConsecutiveFailures++
	de.mu.Unlock()
	log.Errorf("[%s] %v", slot.emitter.ID(), err)
}

// initialise calls emitter.Init, converting a panic into an error.
func initialise(emitter Emitter, snapshot *ClusterSnapshot) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = panicError(r)
		}
	}()
	return emitter.Init(snapshot)
}

// emit is helper function that handles the emission of a snapshot to a specific emitter
// and captures and recovers from any panics that occur during the emission process.
func emit(ctx context.Context, emitter Emitter, snapshot *ClusterSnapshot) (err error) {
	// panics are recovered and propagated as errors
	defer func() {
		if r := recover(); r != nil {
			err = panicError(r)
		}
	}()

	// Currently, each emitter will block until the emission is complete. This can easily
	// be changed later to allow for non-blocking emissions, but we'll have to ensure that
	// the shared ClusterSnapshot remains immutable during the emission process.
	return emitter.Emit(ctx, snapshot)
}

func panicError(r any) error {
	if e, ok := r.(error); ok {
		return fmt.Errorf("unexpected panic: %w\n%s", e, debug.Stack())
	} else if s, ok := r.(string); ok {
		return fmt.Errorf("unexpected panic: %s\n%s", s, debug.Stack())
	}
	return fmt.Errorf("unexpected panic: %+v\n%s", r, debug.Stack())
}

// Stalled implements Heartbeat.
func (de *defaultExporter) Stalled(now time.Time) bool {
	de.mu.Lock()
	defer de.mu.Unlock()

	if !de.running {
		return false
	}
	cfg := de.effective
	if de.inCycle {
		return now.Sub(de.lastCycleStart) > cfg.SnapshotTimeout+cfg.EmitTimeout+cfg.StallSlack
	}
	// Measured from the end of the last cycle, so a long cycle that did end isn't mistaken for a
	// missing one: after a cycle ends, the next one starts within one interval (overrun ticks are
	// skipped, never queued) or one MaxBackoff (≤ interval) while retrying.
	last := de.lastCycleEnd
	if last.IsZero() || last.Before(de.startedAt) {
		last = de.startedAt
	}
	return now.Sub(last) > 2*de.interval+cfg.StallSlack
}

// Status implements Heartbeat.
func (de *defaultExporter) Status() ExporterStatus {
	de.mu.Lock()
	defer de.mu.Unlock()

	status := ExporterStatus{
		Running:                      de.running,
		Interval:                     de.interval,
		SnapshotTimeout:              de.effective.SnapshotTimeout,
		EmitTimeout:                  de.effective.EmitTimeout,
		LastCycleStart:               de.lastCycleStart,
		LastCycleEnd:                 de.lastCycleEnd,
		LastSnapshotSuccess:          de.lastSnapshotSuccess,
		LastSnapshotError:            de.lastSnapshotError,
		ConsecutiveSnapshotFailures:  de.consecutiveSnapshotFailures,
		CyclesTotal:                  de.cycles.Load(),
		CycleOverrunsTotal:           de.overruns.Load(),
		SnapshotTimeoutsTotal:        de.snapshotTimeouts.Load(),
		EmitTimeoutsTotal:            de.emitTimeouts.Load(),
		AbandonedShortLivedPodsTotal: de.abandonedShortLivedPods.Load(),
		Emitters:                     make([]EmitterStatus, 0, len(de.slots)),
	}
	for _, slot := range de.slots {
		es := slot.status
		es.Busy = slot.busy.Load()
		status.Emitters = append(status.Emitters, es)
	}
	return status
}
