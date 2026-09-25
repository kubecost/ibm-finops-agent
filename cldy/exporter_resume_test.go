package cldy_test

// F-38, exporter half (docs/reliability/FINDINGS.md): after a restart during a metrics outage,
// Cloudability emission resumes once the source recovers. The liveness half is chunk 08's
// TestReproF38MetricsOutageKeepsLiveness.

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ibm/finops-agent/cldy"
	"github.com/ibm/finops-agent/pkg/core"
	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/onsi/gomega"
)

// resumeProvider returns snap, or fails while failing is set.
type resumeProvider struct {
	snap    *emitter.ClusterSnapshot
	failing atomic.Bool
}

func (p *resumeProvider) SnapshotOf(core.DataSource) (*emitter.ClusterSnapshot, error) {
	if p.failing.Load() {
		return nil, errors.New("metrics source unavailable")
	}
	return p.snap, nil
}

// resumeUploader counts samples; safe for use from the exporter goroutine.
type resumeUploader struct{ samples atomic.Int32 }

func (u *resumeUploader) AddSample(string)    { u.samples.Add(1) }
func (u *resumeUploader) RemoveSample(string) {}
func (u *resumeUploader) SetClusterID(string) {}

func TestReproF38ExporterResumesAfterOutageRestart(t *testing.T) {
	gomega.RegisterTestingT(t)
	data, err := buildTestData()
	if err != nil {
		t.Fatalf("buildTestData: %v", err)
	}
	clock := newFakeClock(time.Now())
	config := cldy.EmitterConfig{
		UploaderConfig:   cldy.UploaderConfig{ScratchDir: t.TempDir()},
		EmitAsJson:       true,
		EmissionInterval: 3 * time.Minute,
	}
	provider := &resumeProvider{snap: data}
	provider.failing.Store(true)
	const tick = 5 * time.Millisecond

	// The pod (re)starts while the outage is still on.
	up := &resumeUploader{}
	ce := cldy.NewEmitterForTest(config, up, clock.Now)
	exp := emitter.NewExporter(nil, provider, ce)
	exp.Start(tick)
	defer exp.Stop()
	for range 10 {
		clock.Advance(time.Minute)
		time.Sleep(2 * tick)
	}

	provider.failing.Store(false)
	for deadline := time.Now().Add(5 * time.Second); up.samples.Load() == 0 && time.Now().Before(deadline); {
		clock.Advance(time.Minute)
		time.Sleep(4 * tick)
	}
	if up.samples.Load() == 0 {
		t.Errorf("F-38: after a restart during a metrics outage, emission never resumed once the source recovered " +
			"(the exporter exited on its first failed snapshot, F-09)")
	}
}
