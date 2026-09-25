package adapters

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/log"
)

// maxSwapDeferral bounds how long a snapshot waits for pinned computations to finish before it
// is published anyway. Computations normally take seconds; one pinned for longer is likely hung,
// and the adapters must not serve stale data behind it indefinitely.
const maxSwapDeferral = 5 * time.Minute

// adapterState is one generation of the data the adapters serve. It is never modified after it
// is published.
type adapterState struct {
	info       *clusters.ClusterInfo
	kubernetes *emitter.KubernetesSnapshot
	metrics    *metricsState
}

// metricsState is an immutable set of per-resolution metrics snapshots.
type metricsState struct {
	tenMinute, hourly, daily *MetricsResolution
}

func newMetricsState(summary *emitter.MetricsSummary) *metricsState {
	if summary == nil {
		summary = &emitter.MetricsSummary{}
	}
	return &metricsState{
		tenMinute: NewMetricsResolution(10*time.Minute, summary.Minutely),
		hourly:    NewMetricsResolution(time.Hour, summary.Hourly),
		daily:     NewMetricsResolution(24*time.Hour, summary.Daily),
	}
}

// with returns a copy of ms updated with summary.
func (ms *metricsState) with(summary *emitter.MetricsSummary) *metricsState {
	if summary == nil {
		return ms
	}
	return &metricsState{
		tenMinute: ms.tenMinute.with(summary.Minutely),
		hourly:    ms.hourly.with(summary.Hourly),
		daily:     ms.daily.with(summary.Daily),
	}
}

// stateHolder publishes adapter state generations. Adapters sharing a holder always read one
// generation per call, and a computation that pins the holder reads one generation for its whole
// duration: a new generation published while anything is pinned is held back until the last pin
// is released (or maxSwapDeferral passes). Publishing never blocks.
type stateHolder struct {
	current atomic.Pointer[adapterState]

	mu           sync.Mutex
	pins         int
	pending      *adapterState
	pendingSince time.Time
	maxDeferral  time.Duration
	now          func() time.Time

	forcedSwaps atomic.Uint64
}

func newStateHolder(s *adapterState) *stateHolder {
	h := &stateHolder{maxDeferral: maxSwapDeferral, now: time.Now}
	h.current.Store(s)
	return h
}

func (h *stateHolder) load() *adapterState {
	return h.current.Load()
}

// update publishes next(latest), where latest is the newest generation, published or pending.
func (h *stateHolder) update(next func(*adapterState) *adapterState) {
	h.mu.Lock()
	defer h.mu.Unlock()

	base := h.pending
	if base == nil {
		base = h.current.Load()
	}
	s := next(base)
	if h.pins == 0 {
		h.current.Store(s)
		h.pending = nil
		return
	}

	if h.pending == nil {
		h.pendingSince = h.now()
	}
	h.pending = s
	if waited := h.now().Sub(h.pendingSince); waited >= h.maxDeferral {
		h.current.Store(s)
		h.pending = nil
		h.forcedSwaps.Add(1)
		log.Warnf("Kubecost adapters: published a snapshot under %d pinned computations after waiting %s; they may read mixed snapshots", h.pins, waited.Round(time.Second))
	}
}

// pin holds the current generation until release is called.
func (h *stateHolder) pin() (release func()) {
	h.mu.Lock()
	h.pins++
	h.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.pins--
			if h.pins == 0 && h.pending != nil {
				h.current.Store(h.pending)
				h.pending = nil
			}
		})
	}
}
