package emitter

import (
	"errors"
	"testing"
	"time"
	"unsafe"

	"github.com/ibm/finops-agent/internal/mocks"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// mockQueryGroupFuture is a simple mock that implements the QueryGroupFuture interface
type mockQueryGroupFuture[T any] struct {
	result []*T
	err    error
}

func (m *mockQueryGroupFuture[T]) Await() ([]*T, error) {
	return m.result, m.err
}

func newMockFuture[T any](result []*T, err error) *mockQueryGroupFuture[T] {
	return &mockQueryGroupFuture[T]{result: result, err: err}
}

func TestAwaitWithLog_Success(t *testing.T) {
	// Reset the counter before test
	metricQueryFailures.Reset()
	
	// Create a successful future with mock data
	mockResult := []*source.PVActiveMinutesResult{{}}
	future := newMockFuture(mockResult, nil)

	result := awaitWithLog("testQuery", future)

	// Verify result is returned
	if len(result) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result))
	}

	// Verify counter was not incremented
	count := testutil.ToFloat64(metricQueryFailures.WithLabelValues("testQuery"))
	if count != 0 {
		t.Errorf("Expected counter to be 0, got %f", count)
	}
}

func TestAwaitWithLog_Error(t *testing.T) {
	// Reset the counter before test
	metricQueryFailures.Reset()

	queryName := "testFailedQuery"
	
	// Create a future that returns an error
	future := newMockFuture[source.PVActiveMinutesResult](nil, errors.New("test error: query failed"))

	result := awaitWithLog(queryName, future)

	// Verify result is nil (error case)
	if result != nil {
		t.Errorf("Expected nil result on error, got %v", result)
	}

	// Verify counter was incremented
	count := testutil.ToFloat64(metricQueryFailures.WithLabelValues(queryName))
	if count != 1 {
		t.Errorf("Expected counter to be 1, got %f", count)
	}
}

func TestAwaitWithLog_MultipleErrors(t *testing.T) {
	// Reset the counter before test
	metricQueryFailures.Reset()
	
	// Create multiple futures that return errors
	queries := []string{"query1", "query2", "query3"}
	
	for _, queryName := range queries {
		future := newMockFuture[source.PVActiveMinutesResult](nil, errors.New("test error"))
		awaitWithLog(queryName, future)
	}

	// Verify counter was incremented for each query
	for _, queryName := range queries {
		count := testutil.ToFloat64(metricQueryFailures.WithLabelValues(queryName))
		if count != 1 {
			t.Errorf("Expected counter for '%s' to be 1, got %f", queryName, count)
		}
	}
}

func TestAwaitWithLog_MixedSuccessAndFailure(t *testing.T) {
	// Reset the counter before test
	metricQueryFailures.Reset()
	
	// Successful query
	successFuture := newMockFuture([]*source.PVActiveMinutesResult{{}}, nil)
	awaitWithLog("successQuery", successFuture)

	// Failed query
	failFuture := newMockFuture[source.PVActiveMinutesResult](nil, errors.New("test error"))
	awaitWithLog("failQuery", failFuture)

	// Another successful query
	successFuture2 := newMockFuture([]*source.PVActiveMinutesResult{{}}, nil)
	awaitWithLog("successQuery2", successFuture2)

	// Verify counters
	if count := testutil.ToFloat64(metricQueryFailures.WithLabelValues("successQuery")); count != 0 {
		t.Errorf("Expected counter for successQuery to be 0, got %f", count)
	}
	if count := testutil.ToFloat64(metricQueryFailures.WithLabelValues("failQuery")); count != 1 {
		t.Errorf("Expected counter for failQuery to be 1, got %f", count)
	}
	if count := testutil.ToFloat64(metricQueryFailures.WithLabelValues("successQuery2")); count != 0 {
		t.Errorf("Expected counter for successQuery2 to be 0, got %f", count)
	}
}

// TestMetricQueryFailuresCounter verifies the Prometheus counter is properly registered and functional
func TestMetricQueryFailuresCounter(t *testing.T) {
	// Reset the counter before test
	metricQueryFailures.Reset()
	
	// Trigger a failure to ensure the metric is registered and incremented
	future := newMockFuture[source.PVActiveMinutesResult](nil, errors.New("test error"))
	awaitWithLog("testMetricCounter", future)
	
	// Verify the counter was incremented
	count := testutil.ToFloat64(metricQueryFailures.WithLabelValues("testMetricCounter"))
	if count != 1 {
		t.Errorf("Expected counter to be 1, got %f", count)
	}
	
	// Verify the metric exists in the registry
	metricFamily, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	found := false
	for _, mf := range metricFamily {
		if mf.GetName() == "finops_agent_metric_query_failures_total" {
			found = true
			// Verify it's a counter type
			if mf.GetType().String() != "COUNTER" {
				t.Errorf("Expected COUNTER type, got %v", mf.GetType())
			}
			break
		}
	}

	if !found {
		t.Error("Expected to find finops_agent_metric_query_failures_total metric")
	}
}

// --- Integration tests from original snapshot_test.go ---

type timeBender struct {
	current time.Time
}

func newTimeBender() *timeBender {
	return &timeBender{
		current: time.Now().UTC(),
	}
}

func (td *timeBender) now() time.Time {
	return td.current
}

type expected struct {
	totalMinMetrics  int
	totalHourMetrics int
	totalDayMetrics  int
}

type testCase struct {
	name string

	firstSnapshotAt       string
	firstSnapshotExpected expected

	secondSnapshotAt       string
	secondSnapshotExpected expected
}

// checks whether the snapshotted metrics match the expectations given the snapshot time and window resolution
func checkExpected(t *testing.T, snapshotTime time.Time, res time.Duration, metrics []*MetricsSnapshot, expectedTotal int) {
	t.Helper()

	if len(metrics) != expectedTotal {
		t.Fatalf("Expected %d metric(s), got %d", expectedTotal, len(metrics))
		return
	}

	truncTime := snapshotTime.Truncate(res)
	count := 0
	for i := len(metrics) - 1; i >= 0; i-- {
		start := metrics[i].Window.Start()
		end := metrics[i].Window.End()
		expectedStart := truncTime.Add(-res * time.Duration(count))
		expectedEnd := expectedStart.Add(res)

		if !start.Equal(expectedStart) {
			t.Fatalf("Expected Window Start of: %s, got: %s", expectedStart, *start)
			return
		}

		if !end.Equal(expectedEnd) {
			t.Fatalf("Expected Window End of: %s, got: %s", expectedEnd, *end)
			return
		}

		count += 1
	}
}

// fast-forwards time to the provided snapshotAt, and executes a snapshot. then checks the metrics summary for the expected results
func snapshotAndCheckExpected(t *testing.T, snapshotter SnapshotProvider, ds *mocks.MockDataSource, bender *timeBender, snapshotAt string, exp expected) {
	t.Helper()

	snapshotTime, err := time.Parse(time.RFC3339, snapshotAt)
	if err != nil {
		t.Fatalf("Invalid RFC3339 Time: %s", err)
		return
	}

	bender.current = snapshotTime
	snapshot, err := snapshotter.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("failed to create snapshot: %v", err)
	}

	tenMinMetrics := snapshot.Metrics.Minutely
	hourMetrics := snapshot.Metrics.Hourly
	dayMetrics := snapshot.Metrics.Daily

	checkExpected(t, snapshotTime, 10*time.Minute, tenMinMetrics, exp.totalMinMetrics)
	checkExpected(t, snapshotTime, time.Hour, hourMetrics, exp.totalHourMetrics)
	checkExpected(t, snapshotTime, 24*time.Hour, dayMetrics, exp.totalDayMetrics)
}

func TestSnapshottingMetricsStaggeredWindows(t *testing.T) {
	testCases := []testCase{
		{
			name:            "same boundary",
			firstSnapshotAt: "2025-01-01T15:06:22Z",
			firstSnapshotExpected: expected{
				totalMinMetrics:  1,
				totalHourMetrics: 1,
				totalDayMetrics:  1,
			},
			secondSnapshotAt: "2025-01-01T15:07:22Z",
			secondSnapshotExpected: expected{
				totalMinMetrics:  1,
				totalHourMetrics: 1,
				totalDayMetrics:  1,
			},
		},
		{
			name:            "minutely boundary crossover",
			firstSnapshotAt: "2025-01-01T15:09:14Z",
			firstSnapshotExpected: expected{
				totalMinMetrics:  1,
				totalHourMetrics: 1,
				totalDayMetrics:  1,
			},
			secondSnapshotAt: "2025-01-01T15:10:14Z",
			secondSnapshotExpected: expected{
				totalMinMetrics:  2,
				totalHourMetrics: 1,
				totalDayMetrics:  1,
			},
		},
		{
			name:            "hourly boundary crossover",
			firstSnapshotAt: "2025-01-01T15:59:19Z",
			firstSnapshotExpected: expected{
				totalMinMetrics:  1,
				totalHourMetrics: 1,
				totalDayMetrics:  1,
			},
			secondSnapshotAt: "2025-01-01T16:00:20Z",
			secondSnapshotExpected: expected{
				totalMinMetrics:  2,
				totalHourMetrics: 2,
				totalDayMetrics:  1,
			},
		},
		{
			name:            "daily boundary crossover",
			firstSnapshotAt: "2025-01-01T23:59:32Z",
			firstSnapshotExpected: expected{
				totalMinMetrics:  1,
				totalHourMetrics: 1,
				totalDayMetrics:  1,
			},
			secondSnapshotAt: "2025-01-02T00:00:32Z",
			secondSnapshotExpected: expected{
				totalMinMetrics:  2,
				totalHourMetrics: 2,
				totalDayMetrics:  2,
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Setenv("MINUTE_METRICS_ENABLED", "true")

			timeBender := newTimeBender()

			ds := mocks.NewMockDataSource()
			config := NewSnapshotConfigFromEnv()
			config.Now = timeBender.now

			snapshotter := NewConcurrentSnapshotProvider(config)

			// we always test the first snapshot time, followed by the second -- the first test runs against a cold snapshotter,
			// while the second is able to determine window boundary traversals and behave differently
			snapshotAndCheckExpected(t, snapshotter, ds, timeBender, testCase.firstSnapshotAt, testCase.firstSnapshotExpected)
			snapshotAndCheckExpected(t, snapshotter, ds, timeBender, testCase.secondSnapshotAt, testCase.secondSnapshotExpected)
		})
	}
}

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
