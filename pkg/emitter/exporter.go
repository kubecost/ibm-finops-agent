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
	// Stalled reports whether the exporter has stopped making progress in a way a restart can
	// fix: a cycle started and hasn't ended within its deadlines plus slack, no cycle has started
	// within two intervals plus slack of the previous one ending (or of Start), or an Init or
	// Emit call has run StuckCallCycles intervals past its deadline. Snapshot and emitter failures do not make the loop stalled; they are in Status.
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
	// StopGrace is how long Stop waits for in-flight snapshots and Init and Emit calls to return
	// after their context is cancelled. Default 5s.
	StopGrace time.Duration
	// StuckCallCycles is how many intervals past EmitTimeout an Init or Emit call may keep
	// running before Stalled reports the exporter stalled. Default 5.
	StuckCallCycles int
	// Now is the clock for heartbeat timestamps. Default time.Now.
	Now func() time.Time
}

// DefaultTimeoutIntervals is the default snapshot and emit deadline, in exporter intervals.
// It's generous on purpose: the deadline exists to end hung cycles, not to cut slow ones short.
const DefaultTimeoutIntervals = 5

// maxSnapshotsInFlight bounds the snapshots running at once. A snapshot that outlives its
// deadline keeps running and its result is used when it completes; a second one is started
// only if the first has been running for more than two snapshot deadlines (it looks hung).
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
	if c.StuckCallCycles <= 0 {
		c.StuckCallCycles = 5
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
// The loop never exits on a failure. Snapshots and emitter calls run under deadlines, which end
// the cycle but not the call: a call that outlives its deadline keeps running in its own
// goroutine. A late snapshot's result is used by the cycle in which it completes, so a slow
// source still gets through; a late Init or Emit keeps its emitter busy (and skipped) until it
// returns, so one hung component can't stop the loop or the other emitters.
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

	// Owned by the loop goroutine: snapshots started and not yet collected, the sequence number
	// of the last one started, and of the last one whose result was used.
	pending     []*snapshotCall
	snapshotSeq uint64
	lastUsedSeq uint64

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
	// callStarted is when the running call started (zero when idle), and skipLogged whether
	// the skip for a call past its deadline has been logged. Both guarded by mu.
	callStarted time.Time
	skipLogged  bool
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

// Stop halts the emission process and waits for the loop to exit. Snapshots and Init and Emit
// calls in flight have their context cancelled and are waited for up to StopGrace. A call that
// ignores cancellation for longer is left running (bounded: at most maxSnapshotsInFlight
// snapshots and one call per emitter); a snapshot that completes afterwards is discarded and
// its short-lived pods counted, and a busy emitter is skipped after a restart until its call
// returns.
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
	defer de.releasePending(cfg)

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
		if err == nil {
			discardSnapshot(de, snapshot, "the exporter stopped before it was emitted")
		}
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
			de.recordSkip(slot, cfg)
			continue
		}
		de.mu.Lock()
		slot.callStarted = cfg.Now()
		de.mu.Unlock()
		calls.Go(func() { de.callEmitter(ctx, cfg, slot, snapshot) })
	}
	calls.Wait()

	return true
}

// snapshotCall is one snapshot, run in its own goroutine. snapshot and err are written before
// done is closed. orphaned (under mu) is set when the loop exits without collecting the call;
// the goroutine then discards the result itself.
type snapshotCall struct {
	seq      uint64
	started  time.Time
	done     chan struct{}
	snapshot *ClusterSnapshot
	err      error

	mu       sync.Mutex
	orphaned bool
}

func (c *snapshotCall) finished() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// takeSnapshot returns a snapshot for this cycle. It first uses any snapshot that completed
// since the last cycle, then starts one if none is running (or the running one looks hung),
// and waits for a result up to the snapshot deadline. A snapshot still running at the deadline
// fails this cycle but is not abandoned: its result is used when it completes.
func (de *defaultExporter) takeSnapshot(ctx context.Context, cfg ExporterConfig) (*ClusterSnapshot, error) {
	if snapshot, _ := de.collectSnapshots(); snapshot != nil {
		return snapshot, nil
	}

	if de.shouldStartSnapshot(cfg) {
		de.startSnapshot(ctx, cfg)
	}
	if len(de.pending) == 0 {
		return nil, fmt.Errorf("%d snapshots are still running after the exporter restarted; not starting another", de.snapshotsInFlight.Load())
	}

	timer := time.NewTimer(cfg.SnapshotTimeout)
	defer timer.Stop()
	for {
		var first, second <-chan struct{}
		first = de.pending[0].done
		if len(de.pending) > 1 {
			second = de.pending[1].done
		}
		select {
		case <-first:
		case <-second:
		case <-timer.C:
			de.snapshotTimeouts.Add(1)
			oldest := de.pending[0]
			return nil, fmt.Errorf("no snapshot within the %s deadline; one has been running for %s and its result will be used when it completes: %w",
				cfg.SnapshotTimeout, time.Since(oldest.started).Round(time.Second), context.DeadlineExceeded)
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		snapshot, err := de.collectSnapshots()
		if snapshot != nil {
			return snapshot, nil
		}
		if err != nil && len(de.pending) == 0 {
			return nil, err
		}
		if len(de.pending) == 0 {
			// only superseded snapshots completed; start a fresh one
			if !de.shouldStartSnapshot(cfg) {
				return nil, errors.New("only superseded snapshots completed and the in-flight bound is reached")
			}
			de.startSnapshot(ctx, cfg)
		}
	}
}

// collectSnapshots removes completed snapshots from pending. It returns the newest successful
// one not older than the last snapshot used, discarding (and counting) any others, or else the
// newest error.
func (de *defaultExporter) collectSnapshots() (*ClusterSnapshot, error) {
	var best *snapshotCall
	var lastErr error
	remaining := de.pending[:0]
	for _, call := range de.pending {
		if !call.finished() {
			remaining = append(remaining, call)
			continue
		}
		switch {
		case call.err != nil:
			lastErr = call.err
		case call.seq < de.lastUsedSeq:
			discardSnapshot(de, call.snapshot, "a newer snapshot was already emitted")
		case best == nil || call.seq > best.seq:
			if best != nil {
				discardSnapshot(de, best.snapshot, "a newer snapshot completed in the same cycle")
			}
			best = call
		default:
			discardSnapshot(de, call.snapshot, "a newer snapshot completed in the same cycle")
		}
	}
	clear(de.pending[len(remaining):])
	de.pending = remaining

	if best != nil {
		de.lastUsedSeq = best.seq
		return best.snapshot, nil
	}
	return nil, lastErr
}

// shouldStartSnapshot reports whether to start a snapshot: none is running, or every running
// one has taken more than two snapshot deadlines, and the in-flight bound allows it.
func (de *defaultExporter) shouldStartSnapshot(cfg ExporterConfig) bool {
	if de.snapshotsInFlight.Load() >= maxSnapshotsInFlight {
		return false
	}
	for _, call := range de.pending {
		if time.Since(call.started) <= 2*cfg.SnapshotTimeout {
			return false
		}
	}
	return true
}

// startSnapshot starts a snapshot bounded by the snapshot deadline and the run context.
func (de *defaultExporter) startSnapshot(ctx context.Context, cfg ExporterConfig) {
	de.snapshotSeq++
	call := &snapshotCall{seq: de.snapshotSeq, started: time.Now(), done: make(chan struct{})}
	de.pending = append(de.pending, call)
	de.snapshotsInFlight.Add(1)

	go func() {
		defer de.snapshotsInFlight.Add(-1)
		sctx, cancel := context.WithTimeout(ctx, cfg.SnapshotTimeout)
		defer cancel()

		call.snapshot, call.err = de.safeSnapshot(sctx)
		close(call.done)

		call.mu.Lock()
		orphaned := call.orphaned
		call.mu.Unlock()
		if orphaned && call.err == nil {
			discardSnapshot(de, call.snapshot, "it completed after the exporter stopped")
		}
	}()
}

// releasePending runs when the loop exits. It waits up to StopGrace for running snapshots
// (their context is cancelled), discards completed ones, and hands the rest to their own
// goroutines to discard when they complete.
func (de *defaultExporter) releasePending(cfg ExporterConfig) {
	grace, cancel := context.WithTimeout(context.Background(), cfg.StopGrace)
	defer cancel()
	for _, call := range de.pending {
		select {
		case <-call.done:
		case <-grace.Done():
		}
		call.mu.Lock()
		if call.finished() {
			if call.err == nil {
				discardSnapshot(de, call.snapshot, "the exporter stopped before it was emitted")
			}
		} else {
			call.orphaned = true
			log.Warnf("snapshot still running %s after Stop; it will be discarded when it completes", cfg.StopGrace)
		}
		call.mu.Unlock()
	}
	de.pending = nil
}

// discardSnapshot counts and logs the short-lived pods in a successful snapshot that will
// never be emitted. The pods were drained from the cluster cache, so they are lost (I1).
func discardSnapshot(de *defaultExporter, snapshot *ClusterSnapshot, reason string) {
	if snapshot == nil || snapshot.Kubernetes == nil {
		return
	}
	if dropped := len(snapshot.Kubernetes.ShortLivedPods); dropped > 0 {
		de.abandonedShortLivedPods.Add(uint64(dropped))
		log.Errorf("discarded a snapshot because %s, dropping %d short-lived pods", reason, dropped)
	}
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
		defer de.callReturned(slot, cfg)
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
	// both may have been ready; a call that did finish is not a timeout
	select {
	case <-done:
		return
	default:
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

// callReturned marks slot idle when its call returns, however late, and logs the end of a skip
// episode.
func (de *defaultExporter) callReturned(slot *emitterSlot, cfg ExporterConfig) {
	de.mu.Lock()
	started := slot.callStarted
	slot.callStarted = time.Time{}
	logged := slot.skipLogged
	slot.skipLogged = false
	de.mu.Unlock()
	slot.busy.Store(false)
	if logged {
		log.Infof("[%s] call that overran its deadline returned after %s; resuming", slot.emitter.ID(), cfg.Now().Sub(started).Round(time.Second))
	}
}

// recordSkip records a cycle skipped because slot's previous call is still running. It logs
// once per stuck call.
func (de *defaultExporter) recordSkip(slot *emitterSlot, cfg ExporterConfig) {
	de.mu.Lock()
	started := slot.callStarted
	first := !slot.skipLogged
	slot.skipLogged = true
	slot.status.LastEmitError = errors.New("previous Init or Emit is still running past its deadline; skipped this cycle")
	slot.status.LastEmitErrorTime = cfg.Now()
	slot.status.ConsecutiveFailures++
	de.mu.Unlock()
	if first {
		log.Errorf("[%s] Init or Emit still running %s after it started; skipping this emitter until it returns",
			slot.emitter.ID(), cfg.Now().Sub(started).Round(time.Second))
	}
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
	// An Init or Emit stuck well past its deadline is a local wedge that a restart can fix.
	stuckAfter := cfg.EmitTimeout + time.Duration(cfg.StuckCallCycles)*de.interval
	for _, slot := range de.slots {
		if !slot.callStarted.IsZero() && now.Sub(slot.callStarted) > stuckAfter {
			return true
		}
	}
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
