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

// ---------------------------------------------------------------------------
// Integration: ConcurrentSnapshotProvider with populated mock data
// ---------------------------------------------------------------------------

func TestSnapshotWithPopulatedClusterCache(t *testing.T) {
	ds := mocks.NewMockDataSource()

	// Populate the mock cluster cache with representative data
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

	// Verify Kubernetes snapshot contains populated data
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

	// Verify cluster info is populated
	if snapshot.ClusterInfo == nil {
		t.Fatal("Expected ClusterInfo to be populated")
	}
	if snapshot.ClusterInfo.ID != "mock-cluster-id" {
		t.Errorf("Expected cluster ID 'mock-cluster-id', got '%s'", snapshot.ClusterInfo.ID)
	}

	// Verify metrics summary exists (even if empty)
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
	// Disable some resources
	config.KubernetesSnapshot = NewKubernetesSnapshotConfig()
	config.KubernetesSnapshot.Nodes = true
	config.KubernetesSnapshot.Pods = false

	provider := NewConcurrentSnapshotProvider(config)
	snapshot, err := provider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// Nodes should be snapshotted
	if len(snapshot.Kubernetes.Nodes) != 1 {
		t.Errorf("Expected 1 node, got %d", len(snapshot.Kubernetes.Nodes))
	}

	// Pods should be empty (disabled)
	if len(snapshot.Kubernetes.Pods) != 0 {
		t.Errorf("Expected 0 pods (disabled), got %d", len(snapshot.Kubernetes.Pods))
	}

	// Verify that the mock's GetAllPods was not called (disabled resource)
	if ds.ClusterCache.Calls["GetAllPods"] != 0 {
		t.Errorf("Expected GetAllPods to not be called, but it was called %d times", ds.ClusterCache.Calls["GetAllPods"])
	}

	// But GetAllNodes should have been called
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
	// Should succeed because we have partial data
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

// ---------------------------------------------------------------------------
// Integration: ConcurrentSnapshotProvider metrics caching behavior
// ---------------------------------------------------------------------------

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

	// First snapshot
	snap1, err := provider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("First snapshot failed: %v", err)
	}

	// Second snapshot should use cache
	snap2, err := provider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("Second snapshot failed: %v", err)
	}

	if snap1.Metrics != snap2.Metrics {
		t.Fatal("Expected cached metrics (same pointer) for second snapshot")
	}

	// Wait for cache to expire
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

	// Without cache, every snapshot should produce a new MetricsSummary
	if snap1.Metrics == snap2.Metrics {
		t.Fatal("Expected different metrics pointers when cache is disabled")
	}
}

// ---------------------------------------------------------------------------
// Integration: Concurrent snapshot provider under parallel load
// ---------------------------------------------------------------------------

func TestConcurrentSnapshotProviderUnderLoad(t *testing.T) {
	config := DefaultSnapshotConfig()
	provider := NewConcurrentSnapshotProvider(config)

	// Run multiple concurrent snapshots, each with its own MockDataSource
	// (MockDataSource maps are not safe for concurrent writes)
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
// Integration: Window calculation edge cases
// ---------------------------------------------------------------------------

func TestSnapshotWindowsForZeroLastSnapshot(t *testing.T) {
	now := time.Date(2025, 1, 15, 9, 5, 30, 0, time.UTC)
	windows := snapshotWindowsFor(now, time.Time{}, 10*time.Minute)

	if len(windows) != 1 {
		t.Fatalf("Expected 1 window for zero lastSnapshot, got %d", len(windows))
	}

	start := windows[0].Start()
	end := windows[0].End()
	if !start.Equal(time.Date(2025, 1, 15, 9, 0, 0, 0, time.UTC)) {
		t.Errorf("Unexpected window start: %s", *start)
	}
	if !end.Equal(time.Date(2025, 1, 15, 9, 10, 0, 0, time.UTC)) {
		t.Errorf("Unexpected window end: %s", *end)
	}
}

func TestSnapshotWindowsForSameWindow(t *testing.T) {
	now := time.Date(2025, 1, 15, 9, 7, 30, 0, time.UTC)
	last := time.Date(2025, 1, 15, 9, 3, 0, 0, time.UTC)
	windows := snapshotWindowsFor(now, last, 10*time.Minute)

	if len(windows) != 1 {
		t.Fatalf("Expected 1 window (same boundary), got %d", len(windows))
	}
}

func TestSnapshotWindowsForBoundaryCrossing(t *testing.T) {
	// Last snapshot at 9:09, current at 9:11 -> crosses the 10m boundary
	now := time.Date(2025, 1, 15, 9, 11, 0, 0, time.UTC)
	last := time.Date(2025, 1, 15, 9, 9, 0, 0, time.UTC)
	windows := snapshotWindowsFor(now, last, 10*time.Minute)

	if len(windows) != 2 {
		t.Fatalf("Expected 2 windows (boundary crossing), got %d", len(windows))
	}

	// First window should be 9:00-9:10 (previous full window)
	s0 := windows[0].Start()
	e0 := windows[0].End()
	if !s0.Equal(time.Date(2025, 1, 15, 9, 0, 0, 0, time.UTC)) {
		t.Errorf("Window[0] unexpected start: %s", *s0)
	}
	if !e0.Equal(time.Date(2025, 1, 15, 9, 10, 0, 0, time.UTC)) {
		t.Errorf("Window[0] unexpected end: %s", *e0)
	}

	// Second window should be 9:10-9:20 (current window)
	s1 := windows[1].Start()
	e1 := windows[1].End()
	if !s1.Equal(time.Date(2025, 1, 15, 9, 10, 0, 0, time.UTC)) {
		t.Errorf("Window[1] unexpected start: %s", *s1)
	}
	if !e1.Equal(time.Date(2025, 1, 15, 9, 20, 0, 0, time.UTC)) {
		t.Errorf("Window[1] unexpected end: %s", *e1)
	}
}

func TestSnapshotWindowsHourlyBoundaryCrossing(t *testing.T) {
	now := time.Date(2025, 1, 15, 16, 0, 30, 0, time.UTC)
	last := time.Date(2025, 1, 15, 15, 59, 0, 0, time.UTC)
	windows := snapshotWindowsFor(now, last, time.Hour)

	if len(windows) != 2 {
		t.Fatalf("Expected 2 windows (hourly boundary crossing), got %d", len(windows))
	}
}

func TestSnapshotWindowsDailyBoundaryCrossing(t *testing.T) {
	now := time.Date(2025, 1, 16, 0, 0, 30, 0, time.UTC)
	last := time.Date(2025, 1, 15, 23, 59, 0, 0, time.UTC)
	windows := snapshotWindowsFor(now, last, 24*time.Hour)

	if len(windows) != 2 {
		t.Fatalf("Expected 2 windows (daily boundary crossing), got %d", len(windows))
	}
}

// ---------------------------------------------------------------------------
// Integration: Snapshot with minutely metrics enabled/disabled
// ---------------------------------------------------------------------------

func TestSnapshotMinutelyMetricsEnabled(t *testing.T) {
	t.Setenv("MINUTE_METRICS_ENABLED", "true")

	ds := mocks.NewMockDataSource()
	config := NewSnapshotConfigFromEnv()

	provider := NewConcurrentSnapshotProvider(config)
	snapshot, err := provider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	if snapshot.Metrics.Minutely == nil {
		t.Error("Expected minutely metrics to be populated when enabled")
	}
}

func TestSnapshotMinutelyMetricsDisabled(t *testing.T) {
	ds := mocks.NewMockDataSource()
	config := DefaultSnapshotConfig()
	config.MinutelyMetricsEnabled = false

	provider := NewConcurrentSnapshotProvider(config)
	snapshot, err := provider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	if len(snapshot.Metrics.Minutely) != 0 {
		t.Error("Expected minutely metrics to be empty when disabled")
	}
}

// ---------------------------------------------------------------------------
// Integration: Multiple sequential snapshots track window progression
// ---------------------------------------------------------------------------

func TestSequentialSnapshotsTrackWindows(t *testing.T) {
	ds := mocks.NewMockDataSource()
	bender := newTimeBender()

	config := DefaultSnapshotConfig()
	config.MinutelyMetricsEnabled = true
	config.Now = bender.now

	provider := NewConcurrentSnapshotProvider(config)

	// First snapshot at 15:05
	bender.current = time.Date(2025, 6, 1, 15, 5, 0, 0, time.UTC)
	snap1, err := provider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("First snapshot failed: %v", err)
	}

	// Should have 1 window for each resolution
	if len(snap1.Metrics.Hourly) != 1 {
		t.Errorf("First snapshot: expected 1 hourly window, got %d", len(snap1.Metrics.Hourly))
	}

	// Second snapshot at 16:01 (crosses hourly boundary)
	bender.current = time.Date(2025, 6, 1, 16, 1, 0, 0, time.UTC)
	snap2, err := provider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("Second snapshot failed: %v", err)
	}

	// Should have 2 hourly windows (previous full + current)
	if len(snap2.Metrics.Hourly) != 2 {
		t.Errorf("Second snapshot: expected 2 hourly windows, got %d", len(snap2.Metrics.Hourly))
	}
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// partialFailureDataSource wraps MockDataSource but overrides StatsSummary()
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

// verifyDataSource ensures the mock data source satisfies the interface
var _ core.DataSource = (*mocks.MockDataSource)(nil)
