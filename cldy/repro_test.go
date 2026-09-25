//go:build reliability_repro

package cldy_test

// Reproductions of the Cloudability findings in docs/reliability/FINDINGS.md. Each test asserts
// the invariant the finding breaks and fails today with a message naming the finding. The chunk
// that fixes a finding removes its test from behind the reliability_repro tag.

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ibm/finops-agent/cldy"
	"github.com/ibm/finops-agent/pkg/core"
	"github.com/ibm/finops-agent/pkg/emitter"
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

// F-38: node stats are recorded only from successful snapshots, so a metrics outage of over
// 30 min fails liveness. The restart deletes pending data (F-01, F-36), and if the outage is
// still on, the new exporter's first snapshot fails and it never emits again (F-09).
func TestReproF38MetricsOutageKeepsLivenessAndResumes(t *testing.T) {
	data := loadTestSnapshot(t)
	clock := newFakeClock(time.Now())
	config := cldy.EmitterConfig{
		UploaderConfig:   cldy.UploaderConfig{ScratchDir: t.TempDir()},
		EmitAsJson:       true,
		EmissionInterval: 3 * time.Minute,
	}
	provider := &switchableProvider{snap: data}
	const tick = 5 * time.Millisecond

	up := &countingUploader{}
	ce := cldy.NewEmitterForTest(config, up, clock.Now)
	exp := emitter.NewExporter(nil, provider, ce)
	exp.Start(tick)
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
	}
	if !ce.Healthy() {
		t.Errorf("F-38: liveness (/healthz) failed after a 45 min metrics outage that a restart cannot fix; " +
			"the kubelet restart deletes pending samples and uploads (F-01, F-36)")
	}

	// The kubelet restarts the pod while the outage is still on.
	exp.Stop()
	up2 := &countingUploader{}
	ce2 := cldy.NewEmitterForTest(config, up2, clock.Now)
	exp2 := emitter.NewExporter(nil, provider, ce2)
	exp2.Start(tick)
	defer exp2.Stop()
	time.Sleep(20 * tick)

	provider.failing.Store(false)
	for range 10 {
		clock.Advance(time.Minute)
		time.Sleep(10 * tick)
	}
	if up2.samples.Load() == 0 {
		t.Errorf("F-38: after a restart during the outage, emission never resumed once the metrics source recovered "+
			"(the exporter exited on its first failed snapshot, F-09), while liveness reports healthy=%v", ce2.Healthy())
	}
}
