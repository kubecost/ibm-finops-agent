package telemetry

import "github.com/prometheus/client_golang/prometheus"

// Collectors returns every collector Register registers.
func Collectors(m *Metrics) []prometheus.Collector { return m.collectors() }
