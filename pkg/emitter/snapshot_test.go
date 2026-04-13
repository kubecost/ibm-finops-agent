package emitter

import (
	"errors"
	"testing"

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

	var failedQueries []string
	
	// Create a successful future with mock data
	mockResult := []*source.PVActiveMinutesResult{{}}
	future := newMockFuture(mockResult, nil)

	result := awaitWithLog("testQuery", future, &failedQueries)

	// Verify result is returned
	if len(result) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result))
	}

	// Verify no failed queries recorded
	if len(failedQueries) != 0 {
		t.Errorf("Expected 0 failed queries, got %d: %v", len(failedQueries), failedQueries)
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

	var failedQueries []string
	queryName := "testFailedQuery"
	
	// Create a future that returns an error
	future := newMockFuture[source.PVActiveMinutesResult](nil, errors.New("test error: query failed"))

	result := awaitWithLog(queryName, future, &failedQueries)

	// Verify result is nil (error case)
	if result != nil {
		t.Errorf("Expected nil result on error, got %v", result)
	}

	// Verify failed query was recorded
	if len(failedQueries) != 1 {
		t.Fatalf("Expected 1 failed query, got %d: %v", len(failedQueries), failedQueries)
	}
	if failedQueries[0] != queryName {
		t.Errorf("Expected failed query name '%s', got '%s'", queryName, failedQueries[0])
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

	var failedQueries []string
	
	// Create multiple futures that return errors
	queries := []string{"query1", "query2", "query3"}
	
	for _, queryName := range queries {
		future := newMockFuture[source.PVActiveMinutesResult](nil, errors.New("test error"))
		awaitWithLog(queryName, future, &failedQueries)
	}

	// Verify all failed queries were recorded
	if len(failedQueries) != 3 {
		t.Errorf("Expected 3 failed queries, got %d: %v", len(failedQueries), failedQueries)
	}

	// Verify each query name is in the list
	for _, queryName := range queries {
		found := false
		for _, fq := range failedQueries {
			if fq == queryName {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected to find '%s' in failed queries: %v", queryName, failedQueries)
		}

		// Verify counter was incremented for each query
		count := testutil.ToFloat64(metricQueryFailures.WithLabelValues(queryName))
		if count != 1 {
			t.Errorf("Expected counter for '%s' to be 1, got %f", queryName, count)
		}
	}
}

func TestAwaitWithLog_MixedSuccessAndFailure(t *testing.T) {
	// Reset the counter before test
	metricQueryFailures.Reset()

	var failedQueries []string
	
	// Successful query
	successFuture := newMockFuture([]*source.PVActiveMinutesResult{{}}, nil)
	awaitWithLog("successQuery", successFuture, &failedQueries)

	// Failed query
	failFuture := newMockFuture[source.PVActiveMinutesResult](nil, errors.New("test error"))
	awaitWithLog("failQuery", failFuture, &failedQueries)

	// Another successful query
	successFuture2 := newMockFuture([]*source.PVActiveMinutesResult{{}}, nil)
	awaitWithLog("successQuery2", successFuture2, &failedQueries)

	// Verify only the failed query was recorded
	if len(failedQueries) != 1 {
		t.Errorf("Expected 1 failed query, got %d: %v", len(failedQueries), failedQueries)
	}
	if failedQueries[0] != "failQuery" {
		t.Errorf("Expected failed query 'failQuery', got '%s'", failedQueries[0])
	}

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
	var failedQueries []string
	future := newMockFuture[source.PVActiveMinutesResult](nil, errors.New("test error"))
	awaitWithLog("testMetricCounter", future, &failedQueries)
	
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
