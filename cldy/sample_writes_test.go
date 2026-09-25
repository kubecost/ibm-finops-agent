package cldy_test

// Sample writes, Init, short-lived pods and disk pressure (docs/reliability/FINDINGS.md chunk 04:
// F-08, F-18, F-22, F-32, F-37, F-45, F-49). The F-08 and F-37 tests were reliability_repro
// reproductions.

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

// F-08: getClusterID falls back to "" when the default namespace is missing, and Init accepts it.
func TestReproF08InitRejectsMissingClusterID(t *testing.T) {
	dir := t.TempDir()
	config := cldy.EmitterConfig{
		ScratchDir:       dir,
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
		Kind: "Pod", APIVersion: "v1",
		Name: name, Namespace: "default", UID: types.UID("uid-" + name),
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
		ScratchDir:       t.TempDir(),
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

// defaultNamespaceUID is the default namespace's UID in testdata, which becomes the cluster ID.
const defaultNamespaceUID = "8604469a-1368-44ee-9f1c-c5cc8c2121c1"

// productionInterval is the default EMISSION_INTERVAL; the exporter ticks every minute.
const productionInterval = 3 * time.Minute

func newTestEmitter(dir string, up cldy.Uploader, clock *fakeClock) *cldy.Emitter {
	return cldy.NewEmitterForTest(cldy.EmitterConfig{
		ScratchDir:       dir,
		EmitAsJson:       true,
		EmissionInterval: productionInterval,
	}, up, clock.Now)
}

func clusterScratchDir(dir string) string {
	return filepath.Join(dir, "scratch", defaultNamespaceUID)
}

// withShortLivedPods returns a copy of snap whose Kubernetes snapshot carries pods as its
// short-lived pods.
func withShortLivedPods(snap *emitter.ClusterSnapshot, pods ...*v1.Pod) *emitter.ClusterSnapshot {
	k := *snap.Kubernetes
	k.ShortLivedPods = pods
	c := *snap
	c.Kubernetes = &k
	return &c
}

// checkOnlyFinalisedSamples fails if any sample directory without the staging prefix lacks a
// valid manifest.
func checkOnlyFinalisedSamples(t *testing.T, clusterDir, when string) {
	t.Helper()
	entries, err := os.ReadDir(clusterDir)
	if err != nil {
		t.Fatalf("%s: reading %s: %v", when, clusterDir, err)
	}
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), cldy.StagingPrefix) {
			continue
		}
		if err := cldy.ValidateSampleForTest(filepath.Join(clusterDir, e.Name())); err != nil {
			t.Errorf("F-22: %s: %s is not staging but is not a valid finalised sample: %v", when, e.Name(), err)
		}
	}
}

func stagingDirs(t *testing.T, clusterDir string) []string {
	t.Helper()
	entries, err := os.ReadDir(clusterDir)
	if err != nil {
		t.Fatalf("reading %s: %v", clusterDir, err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), cldy.StagingPrefix) {
			names = append(names, e.Name())
		}
	}
	return names
}

func sampleFiles(t *testing.T, sample string, pattern string) []string {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(sample, pattern))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	return m
}

// F-08: once the default namespace appears, Init succeeds on a later Emit and emission starts
// without a restart. Nothing is written while uninitialised.
func TestF08InitRetriedUntilDefaultNamespaceAppears(t *testing.T) {
	data := loadTestSnapshot(t)
	noDefault := withShortLivedPods(data)
	var namespaces []*v1.Namespace
	for _, ns := range data.Kubernetes.Namespaces {
		if ns.Name != "default" {
			namespaces = append(namespaces, ns)
		}
	}
	noDefault.Kubernetes.Namespaces = namespaces

	dir := t.TempDir()
	clock := newFakeClock(time.Now().Truncate(time.Minute))
	up := &mockUploader{}
	ce := newTestEmitter(dir, up, clock)

	if err := ce.Init(noDefault); err == nil {
		t.Fatalf("F-08: Init accepted a snapshot without the default namespace")
	}
	for range 5 {
		clock.Advance(time.Minute)
		_ = ce.Emit(context.Background(), noDefault)
	}
	if entries, _ := os.ReadDir(dir); len(entries) > 0 {
		t.Fatalf("F-08: the uninitialised emitter wrote %d entries under the scratch dir", len(entries))
	}
	if ev := ce.EventsForTest(); !ev.Conditions[cldy.ConditionUninitialised] {
		t.Errorf("F-08: condition %s not set while Init fails: %+v", cldy.ConditionUninitialised, ev.Conditions)
	}

	// The default namespace appears. Init is retried within its backoff cap.
	clock.Advance(10 * time.Minute)
	if err := ce.Emit(context.Background(), data); err != nil {
		t.Fatalf("F-08: Emit did not initialise once the default namespace appeared: %v", err)
	}
	for range 3 {
		clock.Advance(time.Minute)
		if err := ce.Emit(context.Background(), data); err != nil {
			t.Fatalf("Emit: %v", err)
		}
	}
	if len(up.data) != 1 {
		t.Fatalf("F-08: %d samples after initialising and one emission interval, want 1", len(up.data))
	}
	if up.clusterID != defaultNamespaceUID || !strings.Contains(up.data[0], defaultNamespaceUID) {
		t.Errorf("F-08: sample %s / uploader cluster ID %q not under cluster ID %s", up.data[0], up.clusterID, defaultNamespaceUID)
	}
	if ev := ce.EventsForTest(); ev.Conditions[cldy.ConditionUninitialised] {
		t.Errorf("condition %s still set after Init succeeded", cldy.ConditionUninitialised)
	}
}

// F-37: every short-lived pod lands in exactly one sample, across non-emitting ticks, repeated
// reports of the same pod and a failed emission.
func TestF37ShortLivedPodsInExactlyOneSample(t *testing.T) {
	data := loadTestSnapshot(t)
	dir := t.TempDir()
	clock := newFakeClock(time.Now().Truncate(time.Minute))
	up := &mockUploader{}
	ce := newTestEmitter(dir, up, clock)
	if err := ce.Init(withShortLivedPods(data, shortLivedPod("slp-0"))); err != nil {
		t.Fatalf("Init: %v", err)
	}

	var want []string
	want = append(want, "slp-0")
	// Emissions at t3, t6 (fails, retried at t7) and t9; the pod of every tick is written.
	for i := 1; i <= 9; i++ {
		clock.Advance(time.Minute)
		name := fmt.Sprintf("slp-%d", i)
		want = append(want, name)
		pods := []*v1.Pod{shortLivedPod(name)}
		if i == 2 {
			pods = append(pods, shortLivedPod("slp-1")) // reported again before it was written
		}
		var restore func()
		if i == 6 { // an emitting tick whose write fails
			restore = cldy.SetTestHook(func(point string) error {
				if point == cldy.CrashSampleBeforeRename {
					return errors.New("injected write failure")
				}
				return nil
			})
		}
		err := ce.Emit(context.Background(), withShortLivedPods(data, pods...))
		if restore != nil {
			restore()
			if err == nil {
				t.Fatalf("setup: the injected write failure at tick %d was not reported", i)
			}
		} else if err != nil {
			t.Fatalf("Emit at tick %d: %v", i, err)
		}
	}

	seen := map[string]int{}
	for _, sample := range up.data {
		names, err := readPodNames(filepath.Join(sample, "pods.jsonl"))
		if err != nil {
			t.Fatalf("reading %s: %v", sample, err)
		}
		for _, n := range names {
			if strings.HasPrefix(n, "slp-") {
				seen[n]++
			}
		}
	}
	if len(up.data) != 3 {
		t.Errorf("setup: %d samples, want 3 (t3, t7, t9)", len(up.data))
	}
	for _, n := range want {
		if seen[n] != 1 {
			t.Errorf("F-37: short-lived pod %s appears in %d samples, want exactly 1 (%d samples, seen %v)",
				n, seen[n], len(up.data), seen)
		}
	}
}

// F-37: overflow of the pending short-lived pods drops the oldest and counts them.
func TestF37ShortLivedPodOverflowCounted(t *testing.T) {
	data := loadTestSnapshot(t)
	clock := newFakeClock(time.Now().Truncate(time.Minute))
	up := &mockUploader{}
	ce := newTestEmitter(t.TempDir(), up, clock)
	if err := ce.Init(data); err != nil {
		t.Fatalf("Init: %v", err)
	}
	const over = 5
	pods := make([]*v1.Pod, 0, cldy.MaxPendingShortLivedPods+over)
	for i := range cldy.MaxPendingShortLivedPods + over {
		pods = append(pods, shortLivedPod(fmt.Sprintf("slp-%d", i)))
	}
	clock.Advance(time.Minute)
	if err := ce.Emit(context.Background(), withShortLivedPods(data, pods...)); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if got := ce.EventsForTest().Dropped[cldy.DropReasonShortLivedPodOverflow]; got != over {
		t.Fatalf("I1: %d short-lived pods over the cap, %d counted as %s", over, got, cldy.DropReasonShortLivedPodOverflow)
	}
	clock.Advance(2 * time.Minute)
	if err := ce.Emit(context.Background(), data); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if len(up.data) != 1 {
		t.Fatalf("setup: %d samples, want 1", len(up.data))
	}
	names, err := readPodNames(filepath.Join(up.data[0], "pods.jsonl"))
	if err != nil {
		t.Fatalf("reading pods: %v", err)
	}
	if missing := missingFrom(names, "slp-5", fmt.Sprintf("slp-%d", cldy.MaxPendingShortLivedPods+over-1)); len(missing) > 0 {
		t.Errorf("the newest short-lived pods %v are missing after overflow", missing)
	}
	if missing := missingFrom(names, "slp-0"); len(missing) == 0 {
		t.Errorf("the oldest short-lived pod was kept, want it dropped")
	}
}

// F-22: a kill at any step of writing a sample leaves no directory without the staging prefix
// that lacks a valid manifest, and the emitter carries on after a failed step.
func TestF22CrashAtEveryWriteStepLeavesOnlyFinalisedSamples(t *testing.T) {
	data := loadTestSnapshot(t)

	// run drives Init and 6 one-minute ticks (2 emissions), failing at the failAt'th sample crash
	// point (0 means never) and checking the on-disk invariant at every crash point, which is
	// what a kill -9 there would leave. It returns the number of crash points reached.
	run := func(t *testing.T, failAt int) int {
		dir := t.TempDir()
		clock := newFakeClock(time.Now().Truncate(time.Minute))
		up := &mockUploader{}
		ce := newTestEmitter(dir, up, clock)
		points := 0
		restore := cldy.SetTestHook(func(point string) error {
			switch point {
			case cldy.CrashSampleFileWritten, cldy.CrashSampleBeforeRename, cldy.CrashSampleAfterRename:
			default:
				return nil
			}
			points++
			checkOnlyFinalisedSamples(t, clusterScratchDir(dir), fmt.Sprintf("kill at %s #%d", point, points))
			if points == failAt {
				return fmt.Errorf("killed at %s", point)
			}
			return nil
		})
		defer restore()
		if err := ce.Init(data); err != nil && failAt == 0 {
			t.Fatalf("Init: %v", err)
		}
		for range 6 {
			clock.Advance(time.Minute)
			_ = ce.Emit(context.Background(), data)
		}
		checkOnlyFinalisedSamples(t, clusterScratchDir(dir), "after the run")
		if failAt == 0 && len(up.data) != 2 {
			t.Fatalf("clean run emitted %d samples, want 2", len(up.data))
		}
		if len(up.data) == 0 {
			t.Errorf("F-45: no sample emitted in 6 ticks after a failure at crash point %d", failAt)
		}
		for _, s := range up.data {
			if err := cldy.ValidateSampleForTest(s); err != nil {
				t.Errorf("F-22: emitted sample %s is not a valid finalised sample: %v", s, err)
			}
		}
		// A new process on the same directory: whatever the kill left is recovered or swept, the
		// invariant holds throughout, and emission resumes.
		clock.Advance(time.Minute)
		up2 := &mockUploader{}
		ce2 := newTestEmitter(dir, up2, clock)
		restore()
		restore = cldy.SetTestHook(func(point string) error {
			checkOnlyFinalisedSamples(t, clusterScratchDir(dir), "after restart at "+point)
			return nil
		})
		if err := ce2.Init(data); err != nil {
			t.Fatalf("Init after restart: %v", err)
		}
		for range 9 {
			clock.Advance(time.Minute)
			if err := ce2.Emit(context.Background(), data); err != nil {
				t.Fatalf("Emit after restart: %v", err)
			}
		}
		if len(up2.data) != 3 {
			t.Errorf("after a restart following a kill at crash point %d: %d samples in 9 ticks, want 3", failAt, len(up2.data))
		}
		checkOnlyFinalisedSamples(t, clusterScratchDir(dir), "after the restarted run")
		if staging := stagingDirs(t, clusterScratchDir(dir)); len(staging) != 1 {
			t.Errorf("staging directories %v after the restarted run, want only the live one", staging)
		}
		return points
	}

	total := run(t, 0)
	if total == 0 {
		t.Fatalf("F-22: no sample crash points reached: samples are written in place under their final names")
	}
	for k := 1; k <= total; k++ {
		t.Run(fmt.Sprintf("fail-at-%d", k), func(t *testing.T) { run(t, k) })
	}
}

// F-45: a failed write doesn't consume the emission slot: the next tick retries, the retried
// sample is intact with its baseline, and the cadence is unchanged.
func TestF45WriteFailureKeepsSlot(t *testing.T) {
	data := loadTestSnapshot(t)
	dir := t.TempDir()
	clock := newFakeClock(time.Now().Truncate(time.Minute))
	up := &mockUploader{}
	ce := newTestEmitter(dir, up, clock)
	if err := ce.Init(data); err != nil {
		t.Fatalf("Init: %v", err)
	}
	tick := func() error {
		clock.Advance(time.Minute)
		return ce.Emit(context.Background(), data)
	}
	for range 3 { // t1..t3: sample 0
		if err := tick(); err != nil {
			t.Fatalf("Emit: %v", err)
		}
	}
	if len(up.data) != 1 {
		t.Fatalf("setup: %d samples at t3, want 1", len(up.data))
	}
	for range 2 { // t4, t5
		if err := tick(); err != nil {
			t.Fatalf("Emit: %v", err)
		}
	}

	// t6: the scratch directory is read-only, so the next sample's directory can't be created.
	cd := clusterScratchDir(dir)
	if err := os.Chmod(cd, 0o555); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	err := tick()
	if cErr := os.Chmod(cd, 0o755); cErr != nil {
		t.Fatalf("chmod: %v", cErr)
	}
	if err == nil {
		t.Fatalf("setup: the write failure at t6 was not reported")
	}

	// t7: retried, not deferred to t9.
	if err := tick(); err != nil {
		t.Fatalf("F-45: Emit after the failure: %v", err)
	}
	if len(up.data) != 2 {
		t.Fatalf("F-45: the write failure at t6 consumed the emission slot: %d samples at t7, want 2", len(up.data))
	}
	retried := up.data[1]
	if err := cldy.ValidateSampleForTest(retried); err != nil {
		t.Errorf("F-45: the retried sample is not intact: %v", err)
	}
	if got := len(sampleFiles(t, retried, "baseline-summary-*")); got != 4 {
		t.Errorf("F-45: the retried sample has %d baseline files, want 4", got)
	}
	if got := len(sampleFiles(t, retried, "stats-summary-*")); got != 4 {
		t.Errorf("F-45: the retried sample has %d stats files, want 4", got)
	}
	if staging := stagingDirs(t, cd); len(staging) != 1 {
		t.Errorf("F-45: staging directories %v left after the failure, want only the live one", staging)
	}
	ev := ce.EventsForTest()
	if ev.EmitResults["error"] != 1 {
		t.Errorf("emit errors counted %d, want 1", ev.EmitResults["error"])
	}

	// The cadence is kept: t8 doesn't emit, t9 does.
	if err := tick(); err != nil || len(up.data) != 2 {
		t.Fatalf("t8: err=%v samples=%d, want no new sample", err, len(up.data))
	}
	if err := tick(); err != nil || len(up.data) != 3 {
		t.Fatalf("t9: err=%v samples=%d, want a new sample", err, len(up.data))
	}
}

// F-32: after a stall, missed slots are skipped and counted rather than emitted as a burst.
func TestF32StallSkipsMissedSlots(t *testing.T) {
	data := loadTestSnapshot(t)
	clock := newFakeClock(time.Now().Truncate(time.Minute))
	up := &mockUploader{}
	ce := newTestEmitter(t.TempDir(), up, clock)
	if err := ce.Init(data); err != nil {
		t.Fatalf("Init: %v", err)
	}
	clock.Advance(3 * time.Minute)
	if err := ce.Emit(context.Background(), data); err != nil || len(up.data) != 1 {
		t.Fatalf("setup: t3 err=%v samples=%d", err, len(up.data))
	}

	clock.Advance(10 * time.Minute) // the exporter stalled from t3 to t13
	if err := ce.Emit(context.Background(), data); err != nil || len(up.data) != 2 {
		t.Fatalf("t13: err=%v samples=%d, want 2", err, len(up.data))
	}
	clock.Advance(time.Minute)
	if err := ce.Emit(context.Background(), data); err != nil {
		t.Fatalf("t14: %v", err)
	}
	if len(up.data) != 2 {
		t.Errorf("F-32: a catch-up burst: %d samples by t14, want 2", len(up.data))
	}
	if got := ce.EventsForTest().EmissionSlotsSkipped; got != 2 {
		t.Errorf("F-32: %d emission slots counted as skipped, want 2 (t6, t9)", got)
	}
	clock.Advance(time.Minute)
	if err := ce.Emit(context.Background(), data); err != nil || len(up.data) != 3 {
		t.Errorf("t15: err=%v samples=%d, want the next slot (t15) emitted", err, len(up.data))
	}
}

// diskFullUnless reports no free space while more than keep finalised-name sample directories
// are in the cluster scratch directory, and plenty otherwise.
func diskFullUnless(t *testing.T, clusterDir string, keep int) func(string) (uint64, error) {
	return func(string) (uint64, error) {
		n := 0
		entries, err := os.ReadDir(clusterDir)
		if err != nil {
			return 1 << 40, nil
		}
		for _, e := range entries {
			if e.IsDir() && !strings.HasPrefix(e.Name(), cldy.StagingPrefix) {
				n++
			}
		}
		if n > keep {
			return 0, nil
		}
		return 1 << 40, nil
	}
}

// F-18: under disk pressure the oldest finalised samples are evicted, each counted as a drop,
// and the live staging directory is never touched.
func TestF18DiskPressureEvictsOldestFinalisedSamples(t *testing.T) {
	data := loadTestSnapshot(t)
	dir := t.TempDir()
	scratch := newProdScratch(t, dir, defaultNamespaceUID)
	now := time.Now().Truncate(time.Minute)
	old := []string{
		scratch.AddCompleteSample(t, now.Add(-30*time.Minute), 0),
		scratch.AddCompleteSample(t, now.Add(-20*time.Minute), 1),
		scratch.AddCompleteSample(t, now.Add(-10*time.Minute), 2),
	}
	clock := newFakeClock(now)
	up := &mockUploader{}
	ce := newTestEmitter(dir, up, clock)
	if err := ce.Init(data); err != nil {
		t.Fatalf("Init: %v", err)
	}
	live := stagingDirs(t, scratch.ClusterScratchDir())

	restore := cldy.SetDiskAvailableForTest(diskFullUnless(t, scratch.ClusterScratchDir(), 1))
	defer restore()
	clock.Advance(3 * time.Minute)
	if err := ce.Emit(context.Background(), data); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	for i, s := range old {
		_, err := os.Stat(s)
		if gone := errors.Is(err, os.ErrNotExist); gone != (i < 2) {
			t.Errorf("sample %d (%s): removed=%v, want the two oldest removed", i, filepath.Base(s), gone)
		}
	}
	if len(up.data) != 1 {
		t.Errorf("no sample written after eviction freed space (%d)", len(up.data))
	}
	ev := ce.EventsForTest()
	if got := ev.Dropped[cldy.DropReasonDiskPressure]; got != 2 {
		t.Errorf("F-18: 2 samples evicted under disk pressure, %d counted as %s", got, cldy.DropReasonDiskPressure)
	}
	if len(up.removed) != 2 {
		t.Errorf("evicted samples not removed from the upload queue: %v", up.removed)
	}
	if !ev.Conditions[cldy.ConditionDiskPressure] {
		t.Errorf("F-18: condition %s not set", cldy.ConditionDiskPressure)
	}
	for _, l := range live {
		if _, err := os.Stat(filepath.Join(scratch.ClusterScratchDir(), strings.TrimPrefix(l, cldy.StagingPrefix))); err != nil {
			t.Errorf("the live sample %s was not finalised: %v", l, err)
		}
	}
}

// F-18: when eviction can't free enough space the sample is skipped and counted; the live
// staging directory (holding the next sample's baseline) survives, and the next sample after the
// pressure clears is intact.
func TestF18DiskPressureSkipsSampleAndKeepsLiveStaging(t *testing.T) {
	data := loadTestSnapshot(t)
	dir := t.TempDir()
	clock := newFakeClock(time.Now().Truncate(time.Minute))
	up := &mockUploader{}
	ce := newTestEmitter(dir, up, clock)
	if err := ce.Init(data); err != nil {
		t.Fatalf("Init: %v", err)
	}
	clock.Advance(3 * time.Minute)
	if err := ce.Emit(context.Background(), data); err != nil || len(up.data) != 1 {
		t.Fatalf("setup: t3 err=%v samples=%d", err, len(up.data))
	}
	cd := clusterScratchDir(dir)
	live := stagingDirs(t, cd)
	// Make the live staging directory look old, as it does after a long stall.
	for _, l := range live {
		old := clock.Now().Add(-2 * time.Hour)
		if err := os.Chtimes(filepath.Join(cd, l), old, old); err != nil {
			t.Fatalf("chtimes: %v", err)
		}
	}

	restore := cldy.SetDiskAvailableForTest(func(string) (uint64, error) { return 0, nil })
	clock.Advance(3 * time.Minute)
	err := ce.Emit(context.Background(), data)
	restore()
	if err != nil {
		t.Fatalf("Emit under disk pressure: %v", err)
	}
	if len(up.data) != 1 {
		t.Errorf("F-18: a sample was written with no disk space (%d samples)", len(up.data))
	}
	ev := ce.EventsForTest()
	if got := ev.Dropped[cldy.DropReasonDiskPressureSkipped]; got != 1 {
		t.Errorf("F-18: skipped sample counted %d times as %s, want 1", got, cldy.DropReasonDiskPressureSkipped)
	}
	for _, l := range live {
		if _, err := os.Stat(filepath.Join(cd, l)); err != nil {
			t.Errorf("the live staging directory %s was removed under disk pressure: %v", l, err)
		}
	}

	clock.Advance(3 * time.Minute)
	if err := ce.Emit(context.Background(), data); err != nil {
		t.Fatalf("Emit after pressure cleared: %v", err)
	}
	if len(up.data) != 2 {
		t.Fatalf("no sample after the pressure cleared (%d)", len(up.data))
	}
	if err := cldy.ValidateSampleForTest(up.data[1]); err != nil {
		t.Errorf("sample after pressure is not intact: %v", err)
	}
	if got := len(sampleFiles(t, up.data[1], "baseline-summary-*")); got != 4 {
		t.Errorf("sample after pressure has %d baseline files, want 4", got)
	}
	if ev := ce.EventsForTest(); ev.Conditions[cldy.ConditionDiskPressure] {
		t.Errorf("condition %s still set after the pressure cleared", cldy.ConditionDiskPressure)
	}
}

// F-49: a statfs failure is not disk pressure: nothing is deleted and the sample is written.
func TestF49StatfsErrorDeletesNothing(t *testing.T) {
	data := loadTestSnapshot(t)
	dir := t.TempDir()
	scratch := newProdScratch(t, dir, defaultNamespaceUID)
	now := time.Now().Truncate(time.Minute)
	oldTS := now.Add(-2 * time.Hour)
	pending := scratch.AddCompleteSample(t, oldTS, 0)
	if err := os.Chtimes(pending, oldTS, oldTS); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	clock := newFakeClock(now)
	up := &mockUploader{}
	ce := newTestEmitter(dir, up, clock)

	restore := cldy.SetDiskAvailableForTest(func(string) (uint64, error) { return 0, errors.New("statfs: input/output error") })
	defer restore()
	if err := ce.Init(data); err != nil {
		t.Fatalf("Init: %v", err)
	}
	clock.Advance(3 * time.Minute)
	if err := ce.Emit(context.Background(), data); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if _, err := os.Stat(pending); err != nil {
		t.Errorf("F-49: a statfs failure deleted a pending sample: %v", err)
	}
	if len(up.data) != 1 {
		t.Errorf("F-49: no sample written when free space was unknown (%d)", len(up.data))
	}
	ev := ce.EventsForTest()
	if !ev.Conditions[cldy.ConditionDiskSpaceUnknown] {
		t.Errorf("F-49: condition %s not set", cldy.ConditionDiskSpaceUnknown)
	}
	if len(ev.Dropped) != 0 {
		t.Errorf("F-49: drops counted when free space was unknown: %v", ev.Dropped)
	}
}

// Staging directories left by a crash or a failed Emit are swept once older than twice the
// emission interval and counted as unfinalised discards, never the live one.
func TestOrphanStagingSweptButNotLive(t *testing.T) {
	data := loadTestSnapshot(t)
	dir := t.TempDir()
	now := time.Now().Truncate(time.Minute)
	cd := clusterScratchDir(dir)
	orphan := filepath.Join(cd, cldy.StagingPrefix+fmt.Sprintf("%d_7", now.Add(-time.Hour).UnixMilli()))
	young := filepath.Join(cd, cldy.StagingPrefix+fmt.Sprintf("%d_0", now.Add(-time.Minute).UnixMilli()))
	for _, d := range []string{orphan, young} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(d, "pods.jsonl"), []byte("{}\n"), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	old := now.Add(-time.Hour)
	if err := os.Chtimes(orphan, old, old); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	clock := newFakeClock(now)
	up := &mockUploader{}
	ce := newTestEmitter(dir, up, clock)
	if err := ce.Init(data); err != nil {
		t.Fatalf("Init: %v", err)
	}
	// The live staging directory looks old too.
	for _, l := range stagingDirs(t, cd) {
		if p := filepath.Join(cd, l); p != orphan && p != young {
			if err := os.Chtimes(p, old, old); err != nil {
				t.Fatalf("chtimes: %v", err)
			}
		}
	}
	clock.Advance(3 * time.Minute)
	if err := ce.Emit(context.Background(), data); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if _, err := os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("orphaned staging directory not swept: %v", err)
	}
	if _, err := os.Stat(young); err != nil {
		t.Errorf("a staging directory younger than 2 x the emission interval was swept: %v", err)
	}
	if len(up.data) != 1 || cldy.ValidateSampleForTest(up.data[0]) != nil {
		t.Errorf("the live sample was not finalised: %v", up.data)
	}
	ev := ce.EventsForTest()
	if ev.UnfinalizedDiscarded != 1 {
		t.Errorf("unfinalised discards counted %d, want 1", ev.UnfinalizedDiscarded)
	}
	if len(ev.Dropped) != 0 {
		t.Errorf("sweeping staging directories counted drops: %v", ev.Dropped)
	}
}

// The finalised-sample contract: listing sees only finalised samples with valid manifests, and
// validation catches a torn file.
func TestFinalisedSampleContract(t *testing.T) {
	scratch := newProdScratch(t, t.TempDir(), "cid-contract")
	now := time.Now().Truncate(time.Second)
	good := scratch.AddCompleteSample(t, now.Add(-2*time.Minute), 0)
	legacy := scratch.AddIncompleteSample(t, now.Add(-3*time.Minute), 1)
	staging := filepath.Join(scratch.ClusterScratchDir(), cldy.StagingPrefix+fmt.Sprintf("%d_2", now.UnixMilli()))
	if err := os.CopyFS(staging, os.DirFS("testdata")); err != nil {
		t.Fatalf("copy: %v", err)
	}
	torn := scratch.AddCompleteSample(t, now.Add(-time.Minute), 3)
	if err := os.WriteFile(filepath.Join(torn, "pods.jsonl"), []byte("{"), 0o644); err != nil {
		t.Fatalf("tear: %v", err)
	}

	paths, invalid, err := cldy.FinalisedSamplesForTest(scratch.ClusterScratchDir())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(paths) != 1 || paths[0] != good {
		t.Errorf("finalised samples = %v, want [%s]", paths, good)
	}
	wantInvalid := []string{filepath.Base(legacy), filepath.Base(torn)}
	if strings.Join(invalid, ",") != strings.Join(wantInvalid, ",") {
		t.Errorf("invalid = %v, want %v (oldest first)", invalid, wantInvalid)
	}
	if err := cldy.ValidateSampleForTest(good); err != nil {
		t.Errorf("valid sample rejected: %v", err)
	}
	// A same-size corruption is caught only by the hash check.
	b, err := os.ReadFile(filepath.Join(good, "nodes.jsonl"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	b[0] ^= 0xff
	if err := os.WriteFile(filepath.Join(good, "nodes.jsonl"), b, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := cldy.ValidateSampleForTest(good); err == nil {
		t.Errorf("a corrupted file passed hash validation")
	}
}

// The uploaded tar layout is unchanged by the manifest: MANIFEST.json is not packaged.
func TestPayloadLeavesOutManifest(t *testing.T) {
	scratch := newProdScratch(t, t.TempDir(), "cid-tar")
	config := scratch.UploaderConfig(t)
	cu := cldy.NewUploaderForTest(config, nil, nil)
	cu.SetClusterID(scratch.ClusterID)
	sample := scratch.AddCompleteSample(t, time.Now(), 0)
	cu.AddSample(sample)
	path, err := cu.ConstructPayload(time.Now())
	if err != nil {
		t.Fatalf("ConstructPayload: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = f.Close() }()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("gzip: %v", err)
	}
	tr := tar.NewReader(gz)
	prefix := filepath.Base(sample) + "/" + scratch.ClusterID + "/"
	n := 0
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar: %v", err)
		}
		n++
		if !strings.HasPrefix(hdr.Name, prefix) {
			t.Errorf("tar entry %s not under %s", hdr.Name, prefix)
		}
		if filepath.Base(hdr.Name) == cldy.ManifestFileName {
			t.Errorf("tar layout changed: %s is packaged", hdr.Name)
		}
	}
	if n == 0 {
		t.Fatalf("empty payload")
	}
}

// Baselines live in the next sample's staging directory, which a restart orphans: the first
// sample after a restart has no baselines and the orphan is swept as an unfinalised discard. The
// sample after that has its baselines again.
func TestFirstSampleAfterRestartHasNoBaselines(t *testing.T) {
	data := loadTestSnapshot(t)
	dir := t.TempDir()
	clock := newFakeClock(time.Now().Truncate(time.Minute))
	ce := newTestEmitter(dir, &mockUploader{}, clock)
	if err := ce.Init(data); err != nil {
		t.Fatalf("Init: %v", err)
	}
	clock.Advance(3 * time.Minute)
	if err := ce.Emit(context.Background(), data); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	// Restart.
	clock.Advance(time.Minute)
	up := &mockUploader{}
	ce2 := newTestEmitter(dir, up, clock)
	if err := ce2.Init(data); err != nil {
		t.Fatalf("Init: %v", err)
	}
	for range 6 {
		clock.Advance(time.Minute)
		if err := ce2.Emit(context.Background(), data); err != nil {
			t.Fatalf("Emit: %v", err)
		}
	}
	if len(up.data) != 2 {
		t.Fatalf("%d samples after restart, want 2", len(up.data))
	}
	if got := len(sampleFiles(t, up.data[0], "baseline-summary-*")); got != 0 {
		t.Errorf("first sample after restart has %d baseline files, want 0", got)
	}
	if got := len(sampleFiles(t, up.data[1], "baseline-summary-*")); got != 4 {
		t.Errorf("second sample after restart has %d baseline files, want 4", got)
	}
	if got := ce2.EventsForTest().UnfinalizedDiscarded; got != 1 {
		t.Errorf("the previous process's staging directory: %d unfinalised discards, want 1", got)
	}
}

// F-37: a pending short-lived pod is written however long emission is delayed; the one-hour age
// filter applies when the pod is drained, not when the sample is finally written.
func TestF37PendingShortLivedPodsSurviveLongDelay(t *testing.T) {
	data := loadTestSnapshot(t)
	dir := t.TempDir()
	start := time.Now().Truncate(time.Minute)
	clock := newFakeClock(start)
	up := &mockUploader{}
	ce := newTestEmitter(dir, up, clock)
	if err := ce.Init(data); err != nil {
		t.Fatalf("Init: %v", err)
	}
	finishedAt := func(name string, at time.Time) *v1.Pod {
		p := shortLivedPod(name)
		p.Status.ContainerStatuses[0].State.Terminated.FinishedAt = metav1.NewTime(at)
		return p
	}
	clock.Advance(time.Minute)
	if err := ce.Emit(context.Background(), withShortLivedPods(data,
		finishedAt("slp-recent", start.Add(-30*time.Minute)),
		finishedAt("slp-stale", start.Add(-2*time.Hour)), // filtered when drained, as before
	)); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	// Every write fails for over an hour.
	cd := clusterScratchDir(dir)
	if err := os.Chmod(cd, 0o555); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	for range 65 {
		clock.Advance(time.Minute)
		_ = ce.Emit(context.Background(), data)
	}
	if err := os.Chmod(cd, 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	clock.Advance(time.Minute)
	if err := ce.Emit(context.Background(), data); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if len(up.data) != 1 {
		t.Fatalf("setup: %d samples, want 1", len(up.data))
	}
	names, err := readPodNames(filepath.Join(up.data[0], "pods.jsonl"))
	if err != nil {
		t.Fatalf("reading pods: %v", err)
	}
	if missing := missingFrom(names, "slp-recent"); len(missing) > 0 {
		t.Errorf("F-37: a pending short-lived pod was filtered out and cleared after a delay of over an hour")
	}
	if missing := missingFrom(names, "slp-stale"); len(missing) == 0 {
		t.Errorf("a pod that finished over an hour before it was drained was written; the age filter changed")
	}
}
