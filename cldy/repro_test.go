//go:build reliability_repro

package cldy_test

// Reproductions of the Cloudability findings in docs/reliability/FINDINGS.md. Each test asserts
// the invariant the finding breaks and fails today with a message naming the finding. The chunk
// that fixes a finding removes its test from behind the reliability_repro tag.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ibm/finops-agent/cldy"
	"github.com/ibm/finops-agent/internal/mocks"
	"github.com/ibm/finops-agent/pkg/cluster"
	"github.com/ibm/finops-agent/pkg/core"
	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// reproRecoveryPeriod stands in for a configured recovery period (decision D8's proposed 72h
// default) where a test needs to look past F-36.
const reproRecoveryPeriod = 72 * time.Hour

// loadTestSnapshot returns the testdata snapshot. buildTestData asserts with gomega, so its fail
// handler is pointed at t first.
func loadTestSnapshot(t *testing.T) *emitter.ClusterSnapshot {
	t.Helper()
	gomega.RegisterTestingT(t)
	data, err := buildTestData()
	if err != nil {
		t.Fatalf("buildTestData: %v", err)
	}
	return data
}

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

// F-01: startup recovery walks scratch/ and treats scratch/<clusterID>/ as the first sample; on
// reaching the first real sample it removes the whole cluster directory.
func TestReproF01RecoveryKeepsPendingSamples(t *testing.T) {
	scratch := newProdScratch(t, t.TempDir(), "cid-f01")
	now := time.Now()
	for i := range 3 {
		scratch.AddCompleteSample(t, now.Add(-time.Duration(30-10*i)*time.Minute), i)
	}
	config := scratch.UploaderConfig(t)
	config.RecoveryPeriod = reproRecoveryPeriod // look past F-36

	cu := cldy.NewUploaderForTest(config, nil, nil)

	if got := len(scratch.Uploads(t)); got != 3 {
		t.Fatalf("F-01: startup recovery on the production layout turned %d of 3 complete samples into payloads "+
			"(RecoveredSamples=%d, %d sample files left in scratch/); the rest were deleted without being counted",
			got, cu.RecoveredSamples, scratch.ScratchFileCount())
	}
}

// F-36: RecoveryPeriod is never set from configuration, so it is 0 in production and every
// payload in upload/ is deleted at startup.
func TestReproF36RestartKeepsUploadBacklog(t *testing.T) {
	scratch := newProdScratch(t, t.TempDir(), "cid-f36")
	path := scratch.AddUpload(t, time.Now().Add(-5*time.Minute))
	config := scratch.UploaderConfig(t) // exactly what production builds from the environment

	cu := cldy.NewUploaderForTest(config, nil, nil)

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("F-36: a 5-minute-old payload in upload/ was deleted at startup (production RecoveryPeriod=%v, "+
			"RecoveredUploads=%d): %v", config.RecoveryPeriod, cu.RecoveredUploads, err)
	}
	if cu.RecoveredUploads != 1 {
		t.Fatalf("F-36: payload survived but was not queued for upload (RecoveredUploads=%d)", cu.RecoveredUploads)
	}
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

// F-04: payloads are written in place. A crash mid-tar leaves a truncated .tgz under its final
// name, which recovery queues and uploads once F-36 is fixed (modelled with a real RecoveryPeriod).
func TestReproF04CrashMidTarNotUploaded(t *testing.T) {
	clock := newFakeClock(time.Now().Truncate(time.Second))
	scratch := newProdScratch(t, t.TempDir(), "cid-f04")
	config := scratch.UploaderConfig(t)
	config.RecoveryPeriod = reproRecoveryPeriod

	// First process: killed while writing the payload.
	cu := cldy.NewUploaderForTest(config, nil, clock.Now)
	cu.SetClusterID(scratch.ClusterID)
	cu.AddSample(scratch.AddCompleteSample(t, clock.Now(), 0))
	onDisk := map[string][]byte{}
	headers := 0
	restore := cldy.SetTestHook(func(point string) error {
		if point != cldy.CrashMidTar {
			return nil
		}
		if headers++; headers < 5 {
			return nil
		}
		// Capture what a kill -9 at this point leaves on disk.
		matches, _ := filepath.Glob(filepath.Join(scratch.UploadDir(), "*.tgz"))
		for _, m := range matches {
			b, err := os.ReadFile(m)
			if err != nil {
				t.Fatalf("capturing %s: %v", m, err)
			}
			onDisk[m] = b
		}
		return errors.New("killed mid-tar")
	})
	cu.UploadCycleForTest()
	restore()
	if len(onDisk) == 0 {
		t.Fatalf("setup: crash point %q was not reached with a payload on disk", cldy.CrashMidTar)
	}
	for path, b := range onDisk {
		if err := os.WriteFile(path, b, 0o644); err != nil {
			t.Fatalf("restoring crash state: %v", err)
		}
	}

	// Second process: restarts, recovers, and uploads on its first cycle.
	clock.Advance(10 * time.Minute)
	svc := &fakeStorage{}
	cu2 := cldy.NewUploaderForTest(config, []cldy.StorageService{svc}, clock.Now)
	cu2.SetClusterID(scratch.ClusterID)
	cu2.AddSample(scratch.AddCompleteSample(t, clock.Now(), 1))
	cu2.UploadCycleForTest()

	for _, u := range svc.Uploaded() {
		if u.Err != nil {
			t.Fatalf("F-04: a payload truncated by a crash mid-tar was recovered and uploaded: %s: %v", u.FileName, u.Err)
		}
	}
}

// F-05: recovery runs in NewCldyUploader before Emitter.Init sets the cluster ID, so recovered
// payloads are named "_<ts>.tgz" and their tar entries lack the <clusterID>/ segment.
func TestReproF05RecoveredPayloadHasClusterID(t *testing.T) {
	scratch := newProdScratch(t, t.TempDir(), "cid-f05")
	scratch.AddCompleteSample(t, time.Now().Add(-10*time.Minute), 0)
	config := scratch.UploaderConfig(t)
	config.RecoveryPeriod = reproRecoveryPeriod

	cldy.NewUploaderForTest(config, nil, nil)

	uploads := scratch.Uploads(t)
	if len(uploads) == 0 {
		t.Fatalf("F-05: no recovered payload to check; want one named %s_<ts>.tgz (masked here by F-01, "+
			"which deletes the sample first)", scratch.ClusterID)
	}
	for _, name := range uploads {
		if !strings.HasPrefix(name, scratch.ClusterID+"_") {
			t.Errorf("F-05: recovered payload %q is not named <clusterID>_<ts>.tgz", name)
		}
		entries, err := readTGZ(filepath.Join(scratch.UploadDir(), name))
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}
		for _, e := range entries {
			if !strings.Contains(e, "/"+scratch.ClusterID+"/") {
				t.Errorf("F-05: tar entry %q in recovered payload %s lacks the <clusterID>/ segment", e, name)
				break
			}
		}
	}
}

// F-08: getClusterID falls back to "" when the default namespace is missing, and Init accepts it.
func TestReproF08InitRejectsMissingClusterID(t *testing.T) {
	dir := t.TempDir()
	config := cldy.EmitterConfig{
		UploaderConfig:   cldy.UploaderConfig{ScratchDir: dir},
		EmitAsJson:       true,
		EmissionInterval: 3 * time.Minute,
	}
	ce := cldy.NewEmitterForTest(config, &mockUploader{}, nil)
	data := loadTestSnapshot(t)
	var namespaces []*v1.Namespace
	for _, ns := range data.Kubernetes.Namespaces {
		if ns.Name != "default" {
			namespaces = append(namespaces, ns)
		}
	}
	data.Kubernetes.Namespaces = namespaces

	err := ce.Init(data)

	written := 0
	_ = filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			written++
		}
		return nil
	})
	if err == nil {
		t.Errorf("F-08: Init accepted a snapshot without the default namespace and used cluster ID %q", *ce.ClusterID)
	}
	if written > 0 {
		t.Errorf("F-08: Init wrote %d files under %s with an empty cluster ID", written, dir)
	}
}

// drainingClusterCache buffers short-lived pods and drains them on GetAllShortLivedPods, as
// DynamicClusterCache does.
type drainingClusterCache struct {
	*mocks.MockClusterCache
	mu  sync.Mutex
	slp []*v1.Pod
}

func (c *drainingClusterCache) deleted(pod *v1.Pod) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.slp = append(c.slp, pod)
}

func (c *drainingClusterCache) GetAllShortLivedPods() []*v1.Pod {
	c.mu.Lock()
	defer c.mu.Unlock()
	pods := c.slp
	c.slp = nil
	return pods
}

type drainingDataSource struct {
	*mocks.MockDataSource
	cache *drainingClusterCache
}

func (ds *drainingDataSource) Cluster() cluster.ClusterCache { return ds.cache }

var _ core.DataSource = (*drainingDataSource)(nil)

func shortLivedPod(name string) *v1.Pod {
	now := metav1.Now()
	return &v1.Pod{
		TypeMeta:   metav1.TypeMeta{Kind: "Pod", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID("uid-" + name)},
		Status: v1.PodStatus{
			Phase:     v1.PodSucceeded,
			StartTime: &now,
			ContainerStatuses: []v1.ContainerStatus{{
				Name:  "main",
				State: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{FinishedAt: now}},
			}},
		},
	}
}

// F-37: the snapshot drains short-lived pods every exporter tick (1 min), but Cloudability writes
// them only on emitting ticks (every 3 min), so the pods drained on the other ticks are lost.
func TestReproF37ShortLivedPodsKeptAcrossTicks(t *testing.T) {
	data := loadTestSnapshot(t)
	clock := newFakeClock(time.Now().Truncate(time.Minute))
	cache := &drainingClusterCache{MockClusterCache: mocks.NewMockClusterCache()}
	cache.Namespaces = data.Kubernetes.Namespaces
	ds := &drainingDataSource{MockDataSource: mocks.NewMockDataSource(), cache: cache}
	snapshotConfig := emitter.DefaultSnapshotConfig()
	snapshotConfig.Now = clock.Now
	provider := emitter.NewConcurrentSnapshotProvider(snapshotConfig)

	up := &mockUploader{}
	ce := cldy.NewEmitterForTest(cldy.EmitterConfig{
		UploaderConfig:   cldy.UploaderConfig{ScratchDir: t.TempDir()},
		EmitAsJson:       true,
		EmissionInterval: 3 * time.Minute, // production default EMISSION_INTERVAL
	}, up, clock.Now)

	// The exporter's cycle: Init on the first snapshot, then snapshot + Emit every minute.
	snap, err := provider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if err := ce.Init(snap); err != nil {
		t.Fatalf("Init: %v", err)
	}
	want := []string{"slp-1", "slp-2", "slp-3"}
	for _, name := range want {
		cache.deleted(shortLivedPod(name)) // one short-lived pod ends during each tick
		clock.Advance(time.Minute)
		snap, err := provider.SnapshotOf(ds)
		if err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		if err := ce.Emit(context.Background(), snap); err != nil {
			t.Fatalf("Emit: %v", err)
		}
	}

	if len(up.data) != 1 {
		t.Fatalf("setup: expected exactly one sample after 3 one-minute ticks, got %d", len(up.data))
	}
	written, err := readPodNames(filepath.Join(up.data[0], "pods.jsonl"))
	if err != nil {
		t.Fatalf("reading pods.jsonl: %v", err)
	}
	if missing := missingFrom(written, want...); len(missing) > 0 {
		t.Fatalf("F-37: short-lived pods %v drained on non-emitting ticks never reached a sample (sample has %v)",
			missing, written)
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

// F-38 (liveness half, chunk 08): node stats are recorded only from successful snapshots, so a
// metrics outage of over 30 min fails liveness, and the restart deletes pending data (F-01,
// F-36). The exporter half (emission resumes after a restart during the outage) is
// TestReproF38ExporterResumesAfterOutageRestart in exporter_resume_test.go.
func TestReproF38MetricsOutageKeepsLiveness(t *testing.T) {
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
	defer exp.Stop()
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
}
