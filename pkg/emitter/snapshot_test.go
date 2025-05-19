package emitter

import (
	"testing"
	"time"
	"unsafe"

	"github.com/ibm/finops-agent/internal/mocks"
)

// NOTE: When the metrics caching is removed, this test can also be removed!
func TestSnapshottingTemporaryCache(t *testing.T) {
	previousCacheDuration := metricsSummaryCacheDuration
	metricsSummaryCacheDuration = time.Second

	// reinstate the previous value after the test
	defer func() {
		metricsSummaryCacheDuration = previousCacheDuration
	}()

	ds := mocks.NewMockDataSource()
	config := DefaultSnapshotConfig()
	config.UseMetricsCache = true

	snapshotProvider := NewConcurrentSnapshotProvider(config)

	snapshot, err := snapshotProvider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// get the snapshot of metrics data
	metricsSnapshot := snapshot.Metrics

	// wait a bit
	time.Sleep(250 * time.Millisecond)

	newSnapshot, err := snapshotProvider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	newMetricsSnapshot := newSnapshot.Metrics

	// compare the two snapshots (should be cached)
	// does go perform reference equality checks for non-comparable types??
	// we can be safe and just compare ptr values
	p1 := uintptr(unsafe.Pointer(metricsSnapshot))
	p2 := uintptr(unsafe.Pointer(newMetricsSnapshot))

	t.Logf("Snapshot 1: %d, Snapshot 2: %d", p1, p2)
	if p1 != p2 {
		t.Fatalf("Expected the same snapshot to be returned, got different pointers")
	}

	// wait beyond cache duration
	time.Sleep(time.Second)

	newestSnapshot, err := snapshotProvider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	newestMetricsSnapshot := newestSnapshot.Metrics

	// compare the two snapshots (should be different)
	p3 := uintptr(unsafe.Pointer(newestMetricsSnapshot))

	t.Logf("Snapshot 1: %d, Snapshot 3: %d", p1, p3)
	if p1 == p3 {
		t.Fatalf("Expected different snapshots to be returned, got same pointers")
	}
}
