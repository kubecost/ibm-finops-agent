package datasourcehealth

import (
	"encoding/json"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// MetricsSnapshot represents a point-in-time snapshot of all data source health metrics
type MetricsSnapshot struct {
	Timestamp time.Time              `json:"timestamp"`
	Metrics   map[string]MetricValue `json:"metrics"`
}

// MetricValue represents a single metric with its labels and value
type MetricValue struct {
	Type   string             `json:"type"`
	Help   string             `json:"help"`
	Values []MetricDataPoint  `json:"values"`
}

// MetricDataPoint represents a single data point with labels and value
type MetricDataPoint struct {
	Labels map[string]string `json:"labels"`
	Value  float64           `json:"value"`
}

// CaptureMetricsSnapshot captures the current state of all data source health metrics
func CaptureMetricsSnapshot() (*MetricsSnapshot, error) {
	snapshot := &MetricsSnapshot{
		Timestamp: time.Now().UTC(),
		Metrics:   make(map[string]MetricValue),
	}

	// Gather all metrics from the default registry
	metricFamilies, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return nil, err
	}

	// Filter for only our finops metrics
	for _, mf := range metricFamilies {
		name := mf.GetName()
		
		// Only include finops_* metrics
		if len(name) < 7 || name[:7] != "finops_" {
			continue
		}

		metricValue := MetricValue{
			Type:   mf.GetType().String(),
			Help:   mf.GetHelp(),
			Values: []MetricDataPoint{},
		}

		// Extract values based on metric type
		for _, m := range mf.GetMetric() {
			dataPoint := MetricDataPoint{
				Labels: extractLabels(m),
				Value:  extractValue(m, mf.GetType()),
			}
			metricValue.Values = append(metricValue.Values, dataPoint)
		}

		snapshot.Metrics[name] = metricValue
	}

	return snapshot, nil
}

// extractLabels extracts label key-value pairs from a metric
func extractLabels(m *dto.Metric) map[string]string {
	labels := make(map[string]string)
	for _, label := range m.GetLabel() {
		labels[label.GetName()] = label.GetValue()
	}
	return labels
}

// extractValue extracts the numeric value from a metric based on its type
func extractValue(m *dto.Metric, metricType dto.MetricType) float64 {
	switch metricType {
	case dto.MetricType_COUNTER:
		if m.Counter != nil {
			return m.Counter.GetValue()
		}
	case dto.MetricType_GAUGE:
		if m.Gauge != nil {
			return m.Gauge.GetValue()
		}
	case dto.MetricType_HISTOGRAM:
		if m.Histogram != nil {
			return float64(m.Histogram.GetSampleCount())
		}
	case dto.MetricType_SUMMARY:
		if m.Summary != nil {
			return float64(m.Summary.GetSampleCount())
		}
	}
	return 0
}

// ToJSON converts the metrics snapshot to JSON bytes
func (ms *MetricsSnapshot) ToJSON() ([]byte, error) {
	return json.MarshalIndent(ms, "", "  ")
}

// Made with Bob
