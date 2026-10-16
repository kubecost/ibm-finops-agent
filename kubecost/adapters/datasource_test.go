package adapters

// F-19 (docs/reliability/FINDINGS.md): a cost computation reads several adapters in turn. If a
// snapshot Update lands between those reads, the computation mixes cluster cache N+1 with
// metrics N, and a closed window exported at rollover keeps the torn result for good.

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
	v1 "k8s.io/api/core/v1"
)

// generation returns a snapshot in which every adapter's data names generation gen.
func generation(gen int, hour time.Time) *emitter.ClusterSnapshot {
	name := fmt.Sprintf("gen-%d", gen)
	return &emitter.ClusterSnapshot{
		ClusterInfo: &clusters.ClusterInfo{ID: "cluster", Name: name},
		Kubernetes: &emitter.KubernetesSnapshot{
			Nodes: []*v1.Node{{Name: name, UID: "node-uid"}},
		},
		Metrics: &emitter.MetricsSummary{
			Hourly: []*emitter.MetricsSnapshot{{
				Window:            opencost.NewClosedWindow(hour, hour.Add(time.Hour)),
				NodeActiveMinutes: []*source.NodeActiveMinutesResult{{Cluster: "cluster", Node: name}},
			}},
		},
	}
}

func newGenerationAdapter(hour time.Time) (*OpenCostDataSourceAdapter, *ClusterCacheAdapter) {
	s := generation(0, hour)
	cache := NewClusterCacheAdapter(s.Kubernetes)
	ds := NewOpenCostDataSourceAdapter(
		NewClusterInfoProviderAdapter(s.ClusterInfo),
		NewClusterMapAdapter(s.ClusterInfo),
		cache,
		NewMetricsQuerierAdapter(s.Metrics),
		time.Minute,
	)
	return ds, cache
}

// A pinned computation sees one generation across all four adapters while Update runs
// concurrently. Run with -race.
func TestPinnedReadsSeeOneGeneration(t *testing.T) {
	hour := time.Now().UTC().Truncate(time.Hour)
	ds, cache := newGenerationAdapter(hour)

	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Go(func() {
		for gen := 1; !stop.Load(); gen++ {
			ds.Update(generation(gen, hour))
		}
	})

	var reads, torn int
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		release := ds.Pin()
		info := ds.ClusterInfo().GetClusterInfo()[clusters.ClusterInfoNameKey]
		nodes := cache.GetAllNodes()
		mapName := ds.ClusterMap().NameFor("cluster")
		metrics, err := ds.Metrics().QueryNodeActiveMinutes(hour, hour.Add(time.Hour)).Await()
		release()
		if err != nil || len(nodes) != 1 || len(metrics) != 1 {
			t.Fatalf("unexpected read: nodes=%d metrics=%d err=%v", len(nodes), len(metrics), err)
		}
		reads++
		if info != nodes[0].Name || info != mapName || info != metrics[0].Node {
			torn++
		}
	}
	stop.Store(true)
	wg.Wait()

	if torn > 0 {
		t.Errorf("F-19: %d of %d pinned computations mixed adapter generations", torn, reads)
	}
}

// Update never waits for a pinned computation, and a computation that stays pinned for too long
// doesn't hold back fresh data indefinitely.
func TestUpdateDoesNotBlockOnPin(t *testing.T) {
	hour := time.Now().UTC().Truncate(time.Hour)
	ds, cache := newGenerationAdapter(hour)

	release := ds.Pin()
	done := make(chan struct{})
	go func() {
		ds.Update(generation(1, hour))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Update blocked on a pinned computation")
	}
	release()

	if got := cache.GetAllNodes()[0].Name; got != "gen-1" {
		t.Errorf("after the last pin was released the adapters serve %s; want gen-1", got)
	}
}

// A computation pinned for longer than maxDeferral (likely hung) doesn't hold fresh data back
// for good: the next Update publishes anyway and is counted.
func TestLongPinIsBounded(t *testing.T) {
	hour := time.Now().UTC().Truncate(time.Hour)
	ds, cache := newGenerationAdapter(hour)
	ds.src.maxDeferral = 20 * time.Millisecond

	release := ds.Pin()
	defer release()
	ds.Update(generation(1, hour))
	if got := cache.GetAllNodes()[0].Name; got != "gen-0" {
		t.Fatalf("pinned adapters serve %s; want gen-0 until the pin is released", got)
	}
	time.Sleep(30 * time.Millisecond)
	ds.Update(generation(2, hour))
	if got := cache.GetAllNodes()[0].Name; got != "gen-2" {
		t.Errorf("adapters serve %s after the pin outlived maxDeferral; want gen-2", got)
	}
	if got := ds.ForcedSwapsTotal(); got != 1 {
		t.Errorf("ForcedSwapsTotal = %d; want 1", got)
	}
}
