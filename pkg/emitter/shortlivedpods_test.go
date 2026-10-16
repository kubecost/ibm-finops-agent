package emitter

// F-07 (docs/reliability/FINDINGS.md): short-lived pods leave the cluster cache's buffer only
// after the emitter that writes them reports them written, so a failed snapshot or emit loses
// none.

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ibm/finops-agent/internal/mocks"
	"github.com/ibm/finops-agent/pkg/cluster"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// slpCache buffers short-lived pods like DynamicClusterCache: GetAllShortLivedPods drains,
// PeekShortLivedPods reads, and CommitShortLivedPods removes the given UIDs.
type slpCache struct {
	*mocks.MockClusterCache
	mu  sync.Mutex
	slp []*corev1.Pod
}

func (c *slpCache) deleted(pod *corev1.Pod) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.slp = append(c.slp, pod)
}

func (c *slpCache) GetAllShortLivedPods() []*corev1.Pod {
	c.mu.Lock()
	defer c.mu.Unlock()
	pods := c.slp
	c.slp = nil
	return pods
}

func (c *slpCache) PeekShortLivedPods() []*corev1.Pod {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.slp)
}

func (c *slpCache) CommitShortLivedPods(uids []types.UID) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	before := len(c.slp)
	c.slp = slices.DeleteFunc(c.slp, func(p *corev1.Pod) bool { return slices.Contains(uids, p.UID) })
	return before - len(c.slp)
}

func (c *slpCache) buffered() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.slp)
}

// slpDataSource serves slpCache and node stats that fail while statsFail is set.
type slpDataSource struct {
	*reproDataSource
	cache *slpCache
}

func (ds *slpDataSource) Cluster() cluster.ClusterCache { return ds.cache }

func newSLPDataSource() *slpDataSource {
	return &slpDataSource{
		reproDataSource: newReproDataSource(),
		cache:           &slpCache{MockClusterCache: mocks.NewMockClusterCache()},
	}
}

// slpWriter is a Cloudability-like emitter: it writes the snapshot's short-lived pods on Emit
// unless failing is set, and reports the UIDs it wrote.
type slpWriter struct {
	failing atomic.Bool

	mu      sync.Mutex
	written []string
	pending []types.UID
}

func (w *slpWriter) ID() EmitterID               { return CldyEmitterID }
func (w *slpWriter) Init(*ClusterSnapshot) error { return nil }
func (w *slpWriter) Emit(_ context.Context, cs *ClusterSnapshot) error {
	if w.failing.Load() {
		return errors.New("sample write failed")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, p := range cs.Kubernetes.ShortLivedPods {
		w.written = append(w.written, p.Name)
		w.pending = append(w.pending, p.UID)
	}
	return nil
}

func (w *slpWriter) WrittenShortLivedPods() []types.UID {
	w.mu.Lock()
	defer w.mu.Unlock()
	uids := w.pending
	w.pending = nil
	return uids
}

func (w *slpWriter) names() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.written)
}

func slp(name string) *corev1.Pod {
	return &corev1.Pod{Name: name, Namespace: "default", UID: types.UID("uid-" + name)}
}

func TestShortLivedPodsSurviveFailedEmitAndSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name string
		fail func(ds *slpDataSource, w *slpWriter, failing bool)
	}{
		{"emit fails", func(_ *slpDataSource, w *slpWriter, failing bool) { w.failing.Store(failing) }},
		{"node stats fail", func(ds *slpDataSource, _ *slpWriter, failing bool) { ds.statsFail.Store(failing) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ds := newSLPDataSource()
			w := &slpWriter{}
			provider := NewConcurrentSnapshotProvider(DefaultSnapshotConfig())
			exp := NewExporter(ds, provider, w)

			// Init with a healthy snapshot first, then fail with a pod buffered.
			if !exp.Start(10 * time.Millisecond) {
				t.Fatal("failed to start exporter")
			}
			defer exp.Stop()
			if !waitFor(2*time.Second, func() bool {
				s := exp.Status()
				return len(s.Emitters) == 1 && s.Emitters[0].State == EmitterReady
			}) {
				t.Fatal("emitter never initialised")
			}
			tc.fail(ds, w, true)
			ds.cache.deleted(slp("slp-1"))
			if !waitFor(2*time.Second, func() bool { return exp.Status().CyclesTotal > 5 }) {
				t.Fatal("exporter stopped cycling")
			}
			tc.fail(ds, w, false)

			if !waitFor(2*time.Second, func() bool { return slices.Contains(w.names(), "slp-1") }) {
				t.Fatalf("F-07: short-lived pod slp-1 was drained by a snapshot whose emission failed (%s) and never written", tc.name)
			}
			if !waitFor(2*time.Second, func() bool { return ds.cache.buffered() == 0 }) {
				t.Errorf("slp-1 still buffered after it was written; the drain was never committed")
			}
		})
	}
}
