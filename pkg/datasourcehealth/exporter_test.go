package datasourcehealth

import (
	"encoding/json"
	"testing"
	"time"
)

func TestCaptureMetricsSnapshot(t *testing.T) {
	// Trigger some metric updates to ensure we have data
	HTTPRequestsTotal.WithLabelValues("test_endpoint", "GET", "200").Inc()
	HTTPRequestDuration.WithLabelValues("test_endpoint", "GET", "200").Observe(0.5)
	MetricsQueryTotal.WithLabelValues("1h", "success").Inc()

	snapshot, err := CaptureMetricsSnapshot()
	if err != nil {
		t.Fatalf("CaptureMetricsSnapshot() error = %v", err)
	}

	if snapshot == nil {
		t.Fatal("CaptureMetricsSnapshot() returned nil snapshot")
	}

	if snapshot.Timestamp.IsZero() {
		t.Error("Snapshot timestamp is zero")
	}

	if len(snapshot.Metrics) == 0 {
		t.Error("Snapshot contains no metrics")
	}

	// Verify we captured finops metrics
	foundHTTPRequests := false
	foundMetricsQuery := false

	for name := range snapshot.Metrics {
		if name == "finops_http_requests_total" {
			foundHTTPRequests = true
		}
		if name == "finops_metrics_query_total" {
			foundMetricsQuery = true
		}
	}

	if !foundHTTPRequests {
		t.Error("Snapshot missing finops_http_requests_total metric")
	}
	if !foundMetricsQuery {
		t.Error("Snapshot missing finops_metrics_query_total metric")
	}
}

func TestMetricsSnapshot_ToJSON(t *testing.T) {
	snapshot := &MetricsSnapshot{
		Timestamp: time.Now().UTC(),
		Metrics: map[string]MetricValue{
			"test_metric": {
				Type: "COUNTER",
				Help: "Test metric",
				Values: []MetricDataPoint{
					{
						Labels: map[string]string{"label1": "value1"},
						Value:  42.0,
					},
				},
			},
		},
	}

	jsonData, err := snapshot.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON() error = %v", err)
	}

	if len(jsonData) == 0 {
		t.Error("ToJSON() returned empty data")
	}

	// Verify it's valid JSON by unmarshaling
	var decoded MetricsSnapshot
	if err := json.Unmarshal(jsonData, &decoded); err != nil {
		t.Errorf("ToJSON() produced invalid JSON: %v", err)
	}

	if decoded.Metrics["test_metric"].Type != "COUNTER" {
		t.Error("JSON roundtrip failed to preserve metric type")
	}
}

func TestExtractLabels(t *testing.T) {
	// This is tested indirectly through CaptureMetricsSnapshot
	// but we can add specific tests if needed
	t.Skip("Tested indirectly through CaptureMetricsSnapshot")
}

func TestExtractValue(t *testing.T) {
	// This is tested indirectly through CaptureMetricsSnapshot
	// but we can add specific tests if needed
	t.Skip("Tested indirectly through CaptureMetricsSnapshot")
}

func TestMetricsSnapshotStructure(t *testing.T) {
	snapshot, err := CaptureMetricsSnapshot()
	if err != nil {
		t.Fatalf("CaptureMetricsSnapshot() error = %v", err)
	}

	// Verify structure of captured metrics
	for name, metric := range snapshot.Metrics {
		if metric.Type == "" {
			t.Errorf("Metric %s has empty type", name)
		}

		if len(metric.Values) == 0 {
			t.Logf("Warning: Metric %s has no values (may be expected if not used yet)", name)
		}

		for i, value := range metric.Values {
			if value.Labels == nil {
				t.Errorf("Metric %s value %d has nil labels", name, i)
			}
		}
	}
}

func TestMetricsSnapshotTimestamp(t *testing.T) {
	before := time.Now().UTC()
	snapshot, err := CaptureMetricsSnapshot()
	after := time.Now().UTC()

	if err != nil {
		t.Fatalf("CaptureMetricsSnapshot() error = %v", err)
	}

	if snapshot.Timestamp.Before(before) || snapshot.Timestamp.After(after) {
		t.Errorf("Snapshot timestamp %v is outside expected range [%v, %v]",
			snapshot.Timestamp, before, after)
	}
}

// Made with Bob
