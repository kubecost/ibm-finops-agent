package cldy_test

// F-07 with F-37 (docs/reliability/FINDINGS.md): through the exporter, short-lived pods stay in
// the cluster cache's buffer until Cloudability writes them into a sample, so pods seen on
// non-emitting ticks, or while node stats fail, still reach a sample.

import (
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ibm/finops-agent/cldy"
	"github.com/ibm/finops-agent/internal/mocks"
	"github.com/ibm/finops-agent/pkg/cluster"
	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/ibm/finops-agent/pkg/nodes"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

// peekableClusterCache buffers short-lived pods like DynamicClusterCache, with peek and commit.
type peekableClusterCache struct {
	*mocks.MockClusterCache
	mu  sync.Mutex
	slp []*v1.Pod
}

func (c *peekableClusterCache) deleted(pod *v1.Pod) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.slp = append(c.slp, pod)
}

func (c *peekableClusterCache) GetAllShortLivedPods() []*v1.Pod {
	c.mu.Lock()
	defer c.mu.Unlock()
	pods := c.slp
	c.slp = nil
	return pods
}

func (c *peekableClusterCache) PeekShortLivedPods() []*v1.Pod {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.slp)
}

func (c *peekableClusterCache) CommitShortLivedPods(uids []types.UID) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	before := len(c.slp)
	c.slp = slices.DeleteFunc(c.slp, func(p *v1.Pod) bool { return slices.Contains(uids, p.UID) })
	return before - len(c.slp)
}

func (c *peekableClusterCache) buffered() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.slp)
}

// switchableStats fails node-stats collection while failing is set.
type switchableStats struct {
	mu      sync.Mutex
	failing bool
}

func (s *switchableStats) set(failing bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failing = failing
}

func (s *switchableStats) GetNodeData() ([]*stats.Summary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failing {
		return nil, errNodeStats
	}
	return nil, nil
}

var errNodeStats = &nodeStatsError{}

type nodeStatsError struct{}

func (*nodeStatsError) Error() string { return "node stats unavailable" }

type peekableDataSource struct {
	*mocks.MockDataSource
	cache *peekableClusterCache
	stats *switchableStats
}

func (ds *peekableDataSource) Cluster() cluster.ClusterCache         { return ds.cache }
func (ds *peekableDataSource) StatsSummary() nodes.StatSummaryClient { return ds.stats }

// samplesUploader records the sample directories; safe for use from the exporter goroutine.
type samplesUploader struct {
	mu      sync.Mutex
	samples []string
}

func (u *samplesUploader) AddSample(s string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.samples = append(u.samples, s)
}
func (u *samplesUploader) RemoveSample(string) {}
func (u *samplesUploader) SetClusterID(string) {}

func (u *samplesUploader) podNames(t *testing.T) []string {
	u.mu.Lock()
	samples := slices.Clone(u.samples)
	u.mu.Unlock()
	var names []string
	for _, s := range samples {
		written, err := readPodNames(filepath.Join(s, "pods.jsonl"))
		if err != nil {
			t.Fatalf("reading %s: %v", s, err)
		}
		names = append(names, written...)
	}
	return names
}

func TestShortLivedPodsReachASampleThroughTheExporter(t *testing.T) {
	gomega.RegisterTestingT(t)
	data, err := buildTestData()
	if err != nil {
		t.Fatalf("buildTestData: %v", err)
	}
	clock := newFakeClock(time.Now().Truncate(time.Minute))
	cache := &peekableClusterCache{MockClusterCache: mocks.NewMockClusterCache()}
	cache.Namespaces = data.Kubernetes.Namespaces
	ds := &peekableDataSource{MockDataSource: mocks.NewMockDataSource(), cache: cache, stats: &switchableStats{}}
	snapshotConfig := emitter.DefaultSnapshotConfig()
	snapshotConfig.Now = clock.Now
	provider := emitter.NewConcurrentSnapshotProvider(snapshotConfig)

	up := &samplesUploader{}
	ce := cldy.NewEmitterForTest(cldy.EmitterConfig{
		ScratchDir:       t.TempDir(),
		EmitAsJson:       true,
		EmissionInterval: 3 * time.Minute, // production default EMISSION_INTERVAL
	}, up, clock.Now)

	const tick = 5 * time.Millisecond
	exp := emitter.NewExporter(ds, provider, ce)
	exp.Start(tick)
	defer exp.Stop()
	waitReady := func() bool {
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(tick) {
			if s := exp.Status(); s.Emitters[0].State == emitter.EmitterReady {
				return true
			}
		}
		return false
	}
	if !waitReady() {
		t.Fatal("cldy emitter never initialised")
	}

	// One pod ends per simulated minute; node stats fail for two of those minutes.
	want := []string{"slp-1", "slp-2", "slp-3", "slp-4"}
	for i, name := range want {
		ds.stats.set(i == 1 || i == 2)
		cache.deleted(endedPod(name))
		clock.Advance(time.Minute)
		time.Sleep(4 * tick)
	}
	ds.stats.set(false)

	deadline := time.Now().Add(5 * time.Second)
	for len(missingFrom(up.podNames(t), want...)) > 0 && time.Now().Before(deadline) {
		clock.Advance(time.Minute)
		time.Sleep(4 * tick)
	}
	if missing := missingFrom(up.podNames(t), want...); len(missing) > 0 {
		t.Fatalf("F-07/F-37: short-lived pods %v never reached a sample (samples have %v)", missing, up.podNames(t))
	}
	for deadline := time.Now().Add(2 * time.Second); cache.buffered() > 0 && time.Now().Before(deadline); {
		time.Sleep(tick)
	}
	if n := cache.buffered(); n > 0 {
		t.Errorf("%d short-lived pods still buffered after they were written", n)
	}
}

// endedPod is a short-lived pod that has just finished.
func endedPod(name string) *v1.Pod {
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
