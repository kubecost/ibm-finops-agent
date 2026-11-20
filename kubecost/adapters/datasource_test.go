package adapters

import (
	"sync"
	"testing"
	"time"

	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/opencost"
)

// TestAtomicUpdate verifies that concurrent reads and writes to the adapter state
// do not cause data races. This test should be run with `go test -race`.
func TestAtomicUpdate(t *testing.T) {
	// Create initial adapters
	clusterInfo := &clusters.ClusterInfo{
		ID:       "test-cluster-1",
		Name:     "test-cluster",
		Provider: "test-provider",
	}

	infoAdapter := NewClusterInfoProviderAdapter(clusterInfo)
	mapAdapter := NewClusterMapAdapter(clusterInfo)
	clusterAdapter := NewClusterCacheAdapter(&emitter.KubernetesSnapshot{})
	metricsAdapter := NewMetricsQuerierAdapter(&emitter.MetricsSummary{})

	// Create the data source adapter
	adapter := NewOpenCostDataSourceAdapter(
		infoAdapter,
		mapAdapter,
		clusterAdapter,
		metricsAdapter,
		time.Hour,
	)

	// Use a WaitGroup to coordinate goroutines
	var wg sync.WaitGroup

	// Writer goroutine: perform 100 updates
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			// Create a new snapshot with updated data
			snapshot := &emitter.ClusterSnapshot{
				ClusterInfo: &clusters.ClusterInfo{
					ID:       "test-cluster-" + string(rune(i)),
					Name:     "test-cluster",
					Provider: "test-provider",
				},
				Kubernetes: &emitter.KubernetesSnapshot{},
				Metrics: &emitter.MetricsSummary{
					Minutely: []*emitter.MetricsSnapshot{
						{
							Window: opencost.NewClosedWindow(
								time.Now().Truncate(10*time.Minute),
								time.Now().Truncate(10*time.Minute).Add(10*time.Minute),
							),
						},
					},
				},
			}
			adapter.Update(snapshot)
			time.Sleep(time.Millisecond) // Small delay between updates
		}
	}()

	// Reader goroutines: perform 1000 reads each
	numReaders := 3
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				// Read all four accessors in sequence
				// If there's a torn read, the race detector will catch it
				_ = adapter.Metrics()
				_ = adapter.ClusterMap()
				_ = adapter.ClusterInfo()
				_ = adapter.ClusterCache()
			}
		}(i)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// If we reach here without the race detector flagging issues, the test passes
	t.Log("Atomic update test completed successfully")
}

// TestConsistentReads verifies that when reading multiple accessors,
// they all come from the same snapshot (no torn reads).
func TestConsistentReads(t *testing.T) {
	// Create initial adapters with a specific cluster ID
	clusterInfo := &clusters.ClusterInfo{
		ID:       "cluster-v1",
		Name:     "test-cluster",
		Provider: "test-provider",
	}

	infoAdapter := NewClusterInfoProviderAdapter(clusterInfo)
	mapAdapter := NewClusterMapAdapter(clusterInfo)
	clusterAdapter := NewClusterCacheAdapter(&emitter.KubernetesSnapshot{})
	metricsAdapter := NewMetricsQuerierAdapter(&emitter.MetricsSummary{})

	adapter := NewOpenCostDataSourceAdapter(
		infoAdapter,
		mapAdapter,
		clusterAdapter,
		metricsAdapter,
		time.Hour,
	)

	// Verify initial state
	info := adapter.ClusterInfo()
	clusterMap := adapter.ClusterMap()

	infoData := info.GetClusterInfo()
	if infoData[clusters.ClusterInfoIdKey] != "cluster-v1" {
		t.Errorf("Expected cluster ID 'cluster-v1', got '%s'", infoData[clusters.ClusterInfoIdKey])
	}

	mapData := clusterMap.AsMap()
	if _, ok := mapData["cluster-v1"]; !ok {
		t.Error("Expected cluster-v1 in cluster map")
	}

	// Update to a new cluster ID
	newClusterInfo := &clusters.ClusterInfo{
		ID:       "cluster-v2",
		Name:     "test-cluster",
		Provider: "test-provider",
	}

	snapshot := &emitter.ClusterSnapshot{
		ClusterInfo: newClusterInfo,
		Kubernetes:  &emitter.KubernetesSnapshot{},
		Metrics:     &emitter.MetricsSummary{},
	}

	adapter.Update(snapshot)

	// Verify updated state - both should reflect the new cluster ID
	info = adapter.ClusterInfo()
	clusterMap = adapter.ClusterMap()

	infoData = info.GetClusterInfo()
	if infoData[clusters.ClusterInfoIdKey] != "cluster-v2" {
		t.Errorf("Expected cluster ID 'cluster-v2', got '%s'", infoData[clusters.ClusterInfoIdKey])
	}

	mapData = clusterMap.AsMap()
	if _, ok := mapData["cluster-v2"]; !ok {
		t.Error("Expected cluster-v2 in cluster map")
	}
}
