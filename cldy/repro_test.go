//go:build reliability_repro

package cldy_test

// Reproductions of the Cloudability findings in docs/reliability/FINDINGS.md. Each test asserts
// the invariant the finding breaks and fails today with a message naming the finding. The chunk
// that fixes a finding removes its test from behind the reliability_repro tag.

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ibm/finops-agent/cldy"
	"github.com/ibm/finops-agent/pkg/core"
	"github.com/ibm/finops-agent/pkg/emitter"
)

// reproRecoveryPeriod stands in for a configured recovery period (decision D8's proposed 72h
// default) where a test needs to look past F-36.
const reproRecoveryPeriod = 72 * time.Hour

// ghostUploads returns the queued payloads whose files no longer exist.
func ghostUploads(cu *cldy.CldyUploader) []string {
	var ghosts []string
	for _, p := range cu.QueuedUploadsForTest() {
		if _, err := os.Stat(p); errors.Is(err, os.ErrNotExist) {
			ghosts = append(ghosts, filepath.Base(p))
		}
	}
	return ghosts
}

// F-02: with no storage service, uploadData loops over nothing, returns nil and deletes the payload.
func TestReproF02ZeroServicesKeepsPayload(t *testing.T) {
	scratch := newProdScratch(t, t.TempDir(), "cid-f02")
	config := scratch.UploaderConfig(t)
	config.RecoveryPeriod = reproRecoveryPeriod

	cu := cldy.NewUploaderForTest(config, nil, nil) // e.g. the service's startup connectivity test failed
	cu.SetClusterID(scratch.ClusterID)
	cu.AddSample(scratch.AddCompleteSample(t, time.Now(), 0))
	cu.UploadCycleForTest()

	if len(scratch.Uploads(t)) == 0 {
		t.Fatalf("F-02: with zero storage services the upload cycle deleted the payload as if delivered; "+
			"the sample is gone (%d sample files left) and nothing was uploaded or counted", scratch.ScratchFileCount())
	}
}

// F-03: operateAndRemove only removes keys when every upload succeeds, but uploadData deletes each
// delivered file. A partial failure leaves ghosts that fail every later cycle.
func TestReproF03PartialFailureLeavesNoGhosts(t *testing.T) {
	clock := newFakeClock(time.Now().Truncate(time.Second))
	scratch := newProdScratch(t, t.TempDir(), "cid-f03")
	for i := range 3 {
		scratch.AddUpload(t, clock.Now().Add(-time.Duration(30-10*i)*time.Minute))
	}
	config := scratch.UploaderConfig(t)
	config.RecoveryPeriod = reproRecoveryPeriod // look past F-36 so the backlog is queued
	svc := &fakeStorage{failOn: func(call int, _ cldy.UploadPayload) error {
		if call == 3 {
			return errors.New("injected upload failure")
		}
		return nil
	}}
	cu := cldy.NewUploaderForTest(config, []cldy.StorageService{svc}, clock.Now)
	cu.SetClusterID(scratch.ClusterID)
	if cu.RecoveredUploads != 3 {
		t.Fatalf("setup: expected 3 queued payloads, got %d", cu.RecoveredUploads)
	}

	// Cycle 1 packages a fresh sample (4 queued) and the 3rd upload fails.
	cu.AddSample(scratch.AddCompleteSample(t, clock.Now(), 0))
	cu.UploadCycleForTest()
	// Cycle 2: the service has recovered.
	clock.Advance(10 * time.Minute)
	cu.AddSample(scratch.AddCompleteSample(t, clock.Now(), 1))
	cu.UploadCycleForTest()

	if ghosts := ghostUploads(cu); len(ghosts) > 0 {
		t.Errorf("F-03: after one partial upload failure the queue holds %d ghost entries (delivered and deleted, still "+
			"queued) %v; every later cycle stops on ENOENT and the queue is never pruned again", len(ghosts), ghosts)
	}
	if left := scratch.Uploads(t); len(left) > 0 {
		t.Errorf("F-03: %d payloads still undelivered after the service recovered: %v", len(left), left)
	}
}

// F-03 (b): disk-pressure cleanup deletes payload files that are still queued.
func TestReproF03DiskPressureLeavesNoGhosts(t *testing.T) {
	clock := newFakeClock(time.Now())
	scratch := newProdScratch(t, t.TempDir(), "cid-f03b")
	scratch.AddUpload(t, clock.Now().Add(-20*time.Minute))
	scratch.AddUpload(t, clock.Now().Add(-10*time.Minute))
	config := scratch.UploaderConfig(t)
	config.RecoveryPeriod = reproRecoveryPeriod
	cu := cldy.NewUploaderForTest(config, nil, clock.Now)

	clock.Advance(reproRecoveryPeriod) // the files are now older than recoveryPeriod/2
	if err := cu.ClearOldUploadSamples(); err != nil {
		t.Fatalf("ClearOldUploadSamples: %v", err)
	}

	if ghosts := ghostUploads(cu); len(ghosts) > 0 {
		t.Fatalf("F-03: disk-pressure cleanup deleted %d queued payloads but left them in the upload queue as ghosts %v",
			len(ghosts), ghosts)
	}
}

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
func (u *countingUploader) RemoveSample(string) {}
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
