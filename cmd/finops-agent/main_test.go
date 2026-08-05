package main

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/ibm/finops-agent/pkg/emitter"
)

// fakeHealthChecker is a minimal emitter.HealthChecker for exercising the health endpoint.
type fakeHealthChecker struct{ healthy bool }

func (f fakeHealthChecker) Healthy() bool { return f.healthy }

// The following minimal emitters exercise publishHealthCheckers' interface filtering.
type plainEmitter struct{}

func (plainEmitter) ID() emitter.EmitterID                                { return "plain" }
func (plainEmitter) Init(*emitter.ClusterSnapshot) error                  { return nil }
func (plainEmitter) Emit(context.Context, *emitter.ClusterSnapshot) error { return nil }

type healthyEmitter struct{ plainEmitter }

func (healthyEmitter) Healthy() bool { return true }

type unhealthyEmitter struct{ plainEmitter }

func (unhealthyEmitter) Healthy() bool { return false }

// TestEmitterHealthCheck verifies the health-check aggregation logic across publication states.
func TestEmitterHealthCheck(t *testing.T) {
	var checkers atomic.Pointer[[]emitter.HealthChecker]
	healthCheck := newEmitterHealthCheck(&checkers)

	// Before publication: startup grace, always healthy.
	if !healthCheck() {
		t.Fatalf("expected healthy before checkers published")
	}

	// All healthy.
	allHealthy := []emitter.HealthChecker{fakeHealthChecker{healthy: true}, fakeHealthChecker{healthy: true}}
	checkers.Store(&allHealthy)
	if !healthCheck() {
		t.Fatalf("expected healthy when all checkers report healthy")
	}

	// One unhealthy flips the aggregate.
	oneUnhealthy := []emitter.HealthChecker{fakeHealthChecker{healthy: true}, fakeHealthChecker{healthy: false}}
	checkers.Store(&oneUnhealthy)
	if healthCheck() {
		t.Fatalf("expected unhealthy when a checker reports unhealthy")
	}

	// Empty published list is healthy (no checkers to fail).
	empty := []emitter.HealthChecker{}
	checkers.Store(&empty)
	if !healthCheck() {
		t.Fatalf("expected healthy for empty checker list")
	}
}

// TestPublishHealthCheckers ensures only HealthChecker-implementing emitters are published.
func TestPublishHealthCheckers(t *testing.T) {
	var checkers atomic.Pointer[[]emitter.HealthChecker]

	emitters := []emitter.Emitter{
		healthyEmitter{},   // implements HealthChecker
		plainEmitter{},     // does not implement HealthChecker
		unhealthyEmitter{}, // implements HealthChecker
	}

	publishHealthCheckers(&checkers, emitters)

	published := checkers.Load()
	if published == nil {
		t.Fatalf("expected checkers to be published")
	}
	if len(*published) != 2 {
		t.Fatalf("expected 2 health checkers, got %d", len(*published))
	}
}
