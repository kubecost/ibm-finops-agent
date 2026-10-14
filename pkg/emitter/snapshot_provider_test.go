package emitter

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ibm/finops-agent/internal/mocks"
	"github.com/ibm/finops-agent/pkg/core"
	"github.com/ibm/finops-agent/pkg/nodes"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSnapshotWithPopulatedClusterCache(t *testing.T) {
	ds := mocks.NewMockDataSource()

	ds.ClusterCache.Nodes = []*v1.Node{
		{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "node-2"}},
	}
	ds.ClusterCache.Pods = []*v1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "pod-2", Namespace: "kube-system"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "pod-3", Namespace: "default"}},
	}
	ds.ClusterCache.Namespaces = []*v1.Namespace{
		{ObjectMeta: metav1.ObjectMeta{Name: "default"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "kube-system"}},
	}
	ds.ClusterCache.Services = []*v1.Service{
		{ObjectMeta: metav1.ObjectMeta{Name: "svc-1", Namespace: "default"}},
	}

	config := DefaultSnapshotConfig()
	provider := NewConcurrentSnapshotProvider(config)

	snapshot, err := provider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	if len(snapshot.Kubernetes.Nodes) != 2 {
		t.Errorf("Expected 2 nodes, got %d", len(snapshot.Kubernetes.Nodes))
	}
	if len(snapshot.Kubernetes.Pods) != 3 {
		t.Errorf("Expected 3 pods, got %d", len(snapshot.Kubernetes.Pods))
	}
	if len(snapshot.Kubernetes.Namespaces) != 2 {
		t.Errorf("Expected 2 namespaces, got %d", len(snapshot.Kubernetes.Namespaces))
	}
	if len(snapshot.Kubernetes.Services) != 1 {
		t.Errorf("Expected 1 service, got %d", len(snapshot.Kubernetes.Services))
	}
	if snapshot.ClusterInfo == nil {
		t.Fatal("Expected ClusterInfo to be populated")
	}
	if snapshot.ClusterInfo.ID != "mock-cluster-id" {
		t.Errorf("Expected cluster ID 'mock-cluster-id', got '%s'", snapshot.ClusterInfo.ID)
	}
	if snapshot.Metrics == nil {
		t.Fatal("Expected Metrics to be non-nil")
	}
}

func TestSnapshotWithSelectiveKubernetesResources(t *testing.T) {
	ds := mocks.NewMockDataSource()
	ds.ClusterCache.Nodes = []*v1.Node{
		{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}},
	}
	ds.ClusterCache.Pods = []*v1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"}},
	}

	config := DefaultSnapshotConfig()
	config.KubernetesSnapshot = NewKubernetesSnapshotConfig()
	config.KubernetesSnapshot.Nodes = true
	config.KubernetesSnapshot.Pods = false

	provider := NewConcurrentSnapshotProvider(config)
	snapshot, err := provider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	if len(snapshot.Kubernetes.Nodes) != 1 {
		t.Errorf("Expected 1 node, got %d", len(snapshot.Kubernetes.Nodes))
	}
	if len(snapshot.Kubernetes.Pods) != 0 {
		t.Errorf("Expected 0 pods (disabled), got %d", len(snapshot.Kubernetes.Pods))
	}
	if ds.ClusterCache.Calls["GetAllPods"] != 0 {
		t.Errorf("Expected GetAllPods to not be called, but it was called %d times", ds.ClusterCache.Calls["GetAllPods"])
	}
	if ds.ClusterCache.Calls["GetAllNodes"] != 1 {
		t.Errorf("Expected GetAllNodes to be called once, got %d", ds.ClusterCache.Calls["GetAllNodes"])
	}
}

func TestSnapshotNodeStatsPartialFailure(t *testing.T) {
	ds := &partialFailureDataSource{
		MockDataSource: mocks.NewMockDataSource(),
		statsClient: &partialFailureStatsClient{
			successNodes: []*stats.Summary{
				{Node: stats.NodeStats{NodeName: "healthy-node"}},
			},
		},
	}

	config := DefaultSnapshotConfig()
	provider := NewConcurrentSnapshotProvider(config)

	snapshot, err := provider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("Snapshot should succeed with partial data: %v", err)
	}

	if snapshot.NodeStats == nil {
		t.Fatal("Expected NodeStats to be non-nil")
	}
	if len(snapshot.NodeStats.Stats) != 1 {
		t.Errorf("Expected 1 node stat, got %d", len(snapshot.NodeStats.Stats))
	}
	if snapshot.NodeStats.Stats[0].Node.NodeName != "healthy-node" {
		t.Errorf("Expected 'healthy-node', got '%s'", snapshot.NodeStats.Stats[0].Node.NodeName)
	}
}

func TestSnapshotNodeStatsCompleteFailure(t *testing.T) {
	ds := &partialFailureDataSource{
		MockDataSource: mocks.NewMockDataSource(),
		statsClient:    &completeFailureStatsClient{},
	}

	config := DefaultSnapshotConfig()
	provider := NewConcurrentSnapshotProvider(config)

	_, err := provider.SnapshotOf(ds)
	if err == nil {
		t.Fatal("Snapshot should fail when node stats completely fail")
	}
}

func TestMetricsCachePopulatesAndExpires(t *testing.T) {
	previousCacheDuration := metricsSummaryCacheDuration
	metricsSummaryCacheDuration = 200 * time.Millisecond
	defer func() {
		metricsSummaryCacheDuration = previousCacheDuration
	}()

	ds := mocks.NewMockDataSource()
	config := DefaultSnapshotConfig()
	config.UseMetricsCache = true

	provider := NewConcurrentSnapshotProvider(config)

	snap1, err := provider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("First snapshot failed: %v", err)
	}

	snap2, err := provider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("Second snapshot failed: %v", err)
	}

	if snap1.Metrics != snap2.Metrics {
		t.Fatal("Expected cached metrics (same pointer) for second snapshot")
	}

	time.Sleep(300 * time.Millisecond)

	snap3, err := provider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("Third snapshot failed: %v", err)
	}

	if snap1.Metrics == snap3.Metrics {
		t.Fatal("Expected new metrics (different pointer) after cache expiry")
	}
}

func TestMetricsCacheDisabled(t *testing.T) {
	ds := mocks.NewMockDataSource()
	config := DefaultSnapshotConfig()
	config.UseMetricsCache = false

	provider := NewConcurrentSnapshotProvider(config)

	snap1, err := provider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("First snapshot failed: %v", err)
	}

	snap2, err := provider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("Second snapshot failed: %v", err)
	}

	if snap1.Metrics == snap2.Metrics {
		t.Fatal("Expected different metrics pointers when cache is disabled")
	}
}

func TestConcurrentSnapshotProviderUnderLoad(t *testing.T) {
	config := DefaultSnapshotConfig()
	provider := NewConcurrentSnapshotProvider(config)

	const concurrency = 10
	var wg sync.WaitGroup
	errors := make(chan error, concurrency)
	snapshots := make(chan *ClusterSnapshot, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ds := mocks.NewMockDataSource()
			ds.ClusterCache.Nodes = []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}},
			}
			snap, err := provider.SnapshotOf(ds)
			if err != nil {
				errors <- err
				return
			}
			snapshots <- snap
		}()
	}

	wg.Wait()
	close(errors)
	close(snapshots)

	for err := range errors {
		t.Errorf("Concurrent snapshot failed: %v", err)
	}

	count := 0
	for snap := range snapshots {
		count++
		if snap.Kubernetes == nil {
			t.Error("Expected Kubernetes snapshot to be populated")
		}
	}

	if count != concurrency {
		t.Errorf("Expected %d successful snapshots, got %d", concurrency, count)
	}
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

type partialFailureDataSource struct {
	*mocks.MockDataSource
	statsClient nodes.StatSummaryClient
}

func (d *partialFailureDataSource) StatsSummary() nodes.StatSummaryClient {
	return d.statsClient
}

type partialFailureStatsClient struct {
	successNodes []*stats.Summary
}

func (p *partialFailureStatsClient) GetNodeData() ([]*stats.Summary, error) {
	return p.successNodes, fmt.Errorf("some nodes failed: node-bad-1, node-bad-2")
}

type completeFailureStatsClient struct{}

func (c *completeFailureStatsClient) GetNodeData() ([]*stats.Summary, error) {
	return nil, fmt.Errorf("all nodes unreachable")
}

var _ core.DataSource = (*mocks.MockDataSource)(nil)
