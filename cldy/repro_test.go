package cldy_test

// F-38, liveness half (docs/reliability/FINDINGS.md): a metrics outage is a remote fault, so it
// must make the agent not ready but never fail liveness, whose restart would do nothing for the
// outage. This was a reliability_repro reproduction; chunk 08 fixed it. The exporter half
// (emission resumes after a restart during the outage) is
// TestReproF38ExporterResumesAfterOutageRestart in exporter_resume_test.go.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ibm/finops-agent/cldy"
	"github.com/ibm/finops-agent/pkg/condition"
	"github.com/ibm/finops-agent/pkg/core"
	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/ibm/finops-agent/pkg/health"
)

// switchableProvider returns a fixed snapshot, or an error while failing is set.
type switchableProvider struct {
	snap    *emitter.ClusterSnapshot
	failing atomic.Bool
}

func (p *switchableProvider) SnapshotOf(core.DataSource) (*emitter.ClusterSnapshot, error) {
	if p.failing.Load() {
		return nil, errors.New("metrics source unavailable")
	}
	return p.snap, nil
}

// countingUploader counts samples; safe for use from the exporter goroutine.
type countingUploader struct{ samples atomic.Int32 }

func (u *countingUploader) AddSample(string)    { u.samples.Add(1) }
func (u *countingUploader) SetClusterID(string) {}

func TestReproF38MetricsOutageKeepsLiveness(t *testing.T) {
	data := loadTestSnapshot(t)
	clock := newFakeClock(time.Now())
	config := cldy.EmitterConfig{
		ScratchDir:       t.TempDir(),
		EmitAsJson:       true,
		EmissionInterval: 3 * time.Minute,
	}
	provider := &switchableProvider{snap: data}
	const tick = 5 * time.Millisecond

	up := &countingUploader{}
	ce := cldy.NewEmitterForTest(config, up, clock.Now)
	// Slack and a generous emit deadline keep a slow write or a scheduling delay on a loaded test
	// machine (-race) from reading as a stall at this 5ms interval.
	exp := emitter.NewExporterWithConfig(nil, provider, emitter.ExporterConfig{StallSlack: time.Second, EmitTimeout: time.Second}, ce)
	exp.Start(tick)
	defer exp.Stop()

	// The agent's health model, wired as main does.
	registry := health.NewRegistry()
	// The outage's 45 min pass on the emitter's fake clock, which the registry uses; the exporter
	// runs its 5ms ticks on the real clock, so its component is checked against that.
	registry.SetClock(clock.Now)
	exporterHealth := emitter.HealthComponent(exp)
	registry.Register("exporter", health.ComponentFunc(func(ctx context.Context, _ time.Time) health.Report {
		return exporterHealth.HealthCheck(ctx, time.Now())
	}))
	if !registry.RegisterAny(string(ce.ID()), ce) {
		t.Fatal("the Cloudability emitter doesn't implement a health interface")
	}
	registry.SetPhase(health.PhaseRunning)
	healthz := httptest.NewServer(registry.LivenessHandler())
	defer healthz.Close()

	// Healthy operation until the first sample is emitted.
	for deadline := time.Now().Add(5 * time.Second); up.samples.Load() == 0; {
		if time.Now().After(deadline) {
			t.Fatalf("setup: no sample emitted before the outage")
		}
		clock.Advance(time.Minute)
		time.Sleep(4 * tick)
	}

	provider.failing.Store(true)
	for range 45 { // a 45 min outage, one exporter tick per minute
		clock.Advance(time.Minute)
		time.Sleep(tick)
		if res := registry.Check(context.Background()); !res.Live {
			t.Fatalf("F-38: liveness failed during a metrics outage that a restart cannot fix: %v", res.NotReadyLines())
		}
	}
	resp, err := http.Get(healthz.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("F-38: /healthz = %d after a 45 min metrics outage that a restart cannot fix; "+
			"the kubelet restart would interrupt collection for nothing", resp.StatusCode)
	}
	res := registry.Check(context.Background())
	if res.Ready || !hasCondition(res, emitter.ConditionSnapshotFailing) {
		t.Errorf("a 45 min metrics outage should make the agent not ready with %s: ready=%v %v",
			emitter.ConditionSnapshotFailing, res.Ready, res.NotReadyLines())
	}

	// The outage ends: emission resumes and the agent is ready again, with no restart.
	provider.failing.Store(false)
	before := up.samples.Load()
	for deadline := time.Now().Add(5 * time.Second); up.samples.Load() == before; {
		if time.Now().After(deadline) {
			t.Fatalf("emission did not resume after the outage")
		}
		clock.Advance(time.Minute)
		time.Sleep(4 * tick)
	}
	if res := registry.Check(context.Background()); !res.Live || !res.Ready {
		t.Errorf("after the outage: live=%v ready=%v %v", res.Live, res.Ready, res.NotReadyLines())
	}
}

func hasCondition(res health.Result, typ string) bool {
	for _, c := range res.Components {
		if condition.Has(c.Conditions, typ) {
			return true
		}
	}
	return false
}
