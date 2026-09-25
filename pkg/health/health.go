// Package health is the agent's health model (docs/reliability/FINDINGS.md chunk 08).
//
// Components register with a Registry and are checked on every probe:
//
//   - Liveness (/healthz) fails only when a restart is likely to fix the fault: a loop whose
//     heartbeat is stale, or a check that can't complete within its timeout (a deadlock). It never
//     fails for remote, credential, disk, RBAC, bucket, region or config problems, or for snapshot
//     failures caused by an upstream component (I4).
//   - Readiness (/readyz) is the degraded signal: false while any component is not live, has an
//     active condition, or startup hasn't finished. The agent serves no traffic, so NotReady has no
//     functional cost; it is alertable through kube-state-metrics.
//   - Startup (/startupz) is true once startup has finished, whether or not the agent is ready.
//   - /status is JSON of every component's conditions and progress, with no secrets.
package health

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ibm/finops-agent/pkg/condition"
	"github.com/opencost/opencost/core/pkg/log"
)

// Report is one component's health.
type Report struct {
	// Live is false only for a fault a restart is likely to fix (I4).
	Live bool
	// NotLiveReason says why Live is false.
	NotLiveReason string
	// Conditions are the component's active degraded conditions. Any one makes it not ready.
	// A zero Since is filled in with when the registry first saw the condition.
	Conditions []condition.Condition
	// Status is component-specific progress for /status: last-success times and backlog counts.
	// It must marshal to JSON and must not hold secrets or configuration values.
	Status any
}

// Component is checked by the registry on every probe. HealthCheck must be cheap and must not
// block on I/O: it reads heartbeats and conditions the component keeps up to date. The registry
// bounds it with a timeout, and a check that doesn't return in time counts as not live.
type Component interface {
	HealthCheck(ctx context.Context, now time.Time) Report
}

// ComponentFunc adapts a function to Component.
type ComponentFunc func(ctx context.Context, now time.Time) Report

func (f ComponentFunc) HealthCheck(ctx context.Context, now time.Time) Report { return f(ctx, now) }

// ConditionReporter is a component whose health is its active conditions: it is always live, and
// ready when it has none. The Kubecost emitter and the collector WAL implement it.
type ConditionReporter interface {
	Conditions() []condition.Condition
}

// Startup phases, in order. /readyz reports the current one until startup has finished.
const (
	PhaseStarting   = "starting"
	PhaseDataSource = "data_source" // informer sync and the collector WAL restore
	PhaseEmitters   = "emitters"
	PhaseRunning    = "running"
)

// DefaultCheckTimeout bounds one probe's checks. Keep the probes' timeoutSeconds above it.
const DefaultCheckTimeout = 2 * time.Second

// Registry holds the agent's health components. It is safe for concurrent use.
type Registry struct {
	now          func() time.Time
	checkTimeout time.Duration

	mu         sync.Mutex
	phase      string
	components []*entry
}

// entry is one registered component and the state the registry keeps for it.
type entry struct {
	name      string
	component Component

	// mu guards the fields below.
	mu sync.Mutex
	// inflight is the check running now, nil when none is. At most one check per component runs
	// at a time: concurrent probes wait for the same one, and a hung check doesn't pile up
	// goroutines.
	inflight *call
	// logged is the last state logged, for edge-triggered logging; seen is when each condition
	// (type and reason) was first seen, for conditions with no Since.
	logged *loggedState
	seen   map[string]time.Time
}

// call is one run of a component's check. report is written before done is closed.
type call struct {
	started time.Time
	done    chan struct{}
	report  Report
}

type loggedState struct {
	live       bool
	ready      bool
	conditions string
}

// NewRegistry returns a registry in PhaseStarting with no components: live, and not ready.
func NewRegistry() *Registry {
	return &Registry{now: time.Now, checkTimeout: DefaultCheckTimeout, phase: PhaseStarting}
}

// SetCheckTimeout sets how long a probe waits for the components' checks. It must be called
// before the registry is used.
func (r *Registry) SetCheckTimeout(d time.Duration) {
	if d > 0 {
		r.checkTimeout = d
	}
}

// SetClock sets the registry's clock, for tests. It must be called before the registry is used.
func (r *Registry) SetClock(now func() time.Time) {
	r.now = now
}

// SetPhase records the startup phase. PhaseRunning ends startup.
func (r *Registry) SetPhase(phase string) {
	r.mu.Lock()
	prev := r.phase
	r.phase = phase
	r.mu.Unlock()
	if prev != phase {
		log.Infof("startup phase: %s", phase)
	}
}

// Phase returns the startup phase.
func (r *Registry) Phase() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.phase
}

// Register adds a component under name. Names should be unique.
func (r *Registry) Register(name string, c Component) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.components = append(r.components, &entry{name: name, component: c, seen: map[string]time.Time{}})
}

// RegisterConditions adds a component that is always live and is ready when it has no active
// conditions.
func (r *Registry) RegisterConditions(name string, c ConditionReporter) {
	r.Register(name, ComponentFunc(func(context.Context, time.Time) Report {
		return Report{Live: true, Conditions: c.Conditions()}
	}))
}

// RegisterAny registers v if it implements Component or, failing that, ConditionReporter, and
// reports whether it did. It lets any emitter take part in health checks by implementing one of
// them.
func (r *Registry) RegisterAny(name string, v any) bool {
	switch c := v.(type) {
	case Component:
		r.Register(name, c)
	case ConditionReporter:
		r.RegisterConditions(name, c)
	default:
		return false
	}
	return true
}

// ComponentResult is one component's report from one evaluation.
type ComponentResult struct {
	Name string
	Report
}

// Ready reports whether the component is live and has no active conditions.
func (c ComponentResult) Ready() bool {
	return c.Live && len(c.Conditions) == 0
}

// Result is one evaluation of every component.
type Result struct {
	Phase      string
	Live       bool
	Ready      bool
	Components []ComponentResult
}

// Check evaluates every component and never blocks longer than the check timeout. Concurrent
// probes share a component's running check. A component whose check doesn't return in time is
// not live; if it is hung, later probes wait for the same check and time out too. Each change in
// a component's liveness, readiness or conditions is logged once.
func (r *Registry) Check(ctx context.Context) Result {
	r.mu.Lock()
	phase := r.phase
	entries := slices.Clone(r.components)
	r.mu.Unlock()

	now := r.now()
	ctx, cancel := context.WithTimeout(ctx, r.checkTimeout)
	defer cancel()

	calls := make([]*call, len(entries))
	for i, e := range entries {
		calls[i] = e.start(now, r.checkTimeout)
	}
	results := make([]ComponentResult, len(entries))
	for i, e := range entries {
		results[i] = ComponentResult{Name: e.name}
		select {
		case <-calls[i].done:
		case <-ctx.Done():
		}
		// a check that has returned is used even if the deadline passed too
		select {
		case <-calls[i].done:
			results[i].Report = calls[i].report
		default:
			results[i].NotLiveReason = fmt.Sprintf("its health check did not return within %s (running for %s)",
				r.checkTimeout, r.now().Sub(calls[i].started).Round(time.Millisecond))
		}
	}

	result := Result{Phase: phase, Live: true, Ready: phase == PhaseRunning, Components: results}
	for i, e := range entries {
		c := &results[i]
		e.fillSince(c, now)
		e.logTransition(*c)
		if !c.Live {
			result.Live = false
		}
		if !c.Ready() {
			result.Ready = false
		}
	}
	return result
}

// start returns the component's running check, starting one if none is running. The check's
// context is bounded by timeout.
func (e *entry) start(now time.Time, timeout time.Duration) *call {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.inflight != nil {
		return e.inflight
	}
	c := &call{started: now, done: make(chan struct{})}
	e.inflight = c
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		c.report = e.check(ctx, now)
		e.mu.Lock()
		e.inflight = nil
		e.mu.Unlock()
		close(c.done)
	}()
	return c
}

// check runs the component's check, converting a panic into a not-live report.
func (e *entry) check(ctx context.Context, now time.Time) (report Report) {
	defer func() {
		if p := recover(); p != nil {
			report = Report{NotLiveReason: fmt.Sprintf("its health check panicked: %v", p)}
		}
	}()
	return e.component.HealthCheck(ctx, now)
}

// fillSince sets a zero Since to when the condition was first seen, and forgets conditions that
// are no longer active.
func (e *entry) fillSince(c *ComponentResult, now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	active := make(map[string]bool, len(c.Conditions))
	conditions := slices.Clone(c.Conditions)
	for i := range conditions {
		key := conditions[i].Type + "/" + conditions[i].Reason
		active[key] = true
		first, ok := e.seen[key]
		if !ok {
			first = now
			e.seen[key] = now
		}
		if conditions[i].Since.IsZero() {
			conditions[i].Since = first
		}
	}
	for key := range e.seen {
		if !active[key] {
			delete(e.seen, key)
		}
	}
	c.Conditions = conditions
}

// logTransition logs a change in the component's liveness, readiness or set of conditions.
func (e *entry) logTransition(c ComponentResult) {
	types := make([]string, 0, len(c.Conditions))
	for _, cond := range c.Conditions {
		types = append(types, cond.Type)
	}
	state := loggedState{live: c.Live, ready: c.Ready(), conditions: strings.Join(types, ",")}

	e.mu.Lock()
	prev := e.logged
	e.logged = &state
	e.mu.Unlock()

	if prev != nil && *prev == state {
		return
	}
	if prev == nil && state.live && state.ready {
		return
	}
	switch {
	case !state.live:
		log.Errorf("health: component %s is not live: %s", c.Name, c.NotLiveReason)
	case prev != nil && !prev.live:
		log.Infof("health: component %s is live again", c.Name)
	}
	switch {
	case !state.ready && state.conditions != "":
		log.Warnf("health: component %s is not ready: %s", c.Name, state.conditions)
	case state.ready && prev != nil && !prev.ready:
		log.Infof("health: component %s is ready again", c.Name)
	}
}
