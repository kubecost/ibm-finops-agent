package datasourcehealth

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// HTTPRequestsTotal tracks the total number of HTTP requests by endpoint, method, and status code
	HTTPRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "finops_http_requests_total",
			Help: "Total number of HTTP requests made by the finops agent",
		},
		[]string{"endpoint", "method", "status_code"},
	)

	// HTTPRequestDuration tracks the duration of HTTP requests in seconds
	HTTPRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "finops_http_request_duration_seconds",
			Help:    "Duration of HTTP requests in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"endpoint", "method", "status_code"},
	)

	// HTTPRequestFailures tracks HTTP request failures by endpoint and failure type
	HTTPRequestFailures = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "finops_http_request_failures_total",
			Help: "Total number of HTTP request failures",
		},
		[]string{"endpoint", "failure_type"},
	)

	// HTTPRequestRetries tracks the number of retry attempts per endpoint
	HTTPRequestRetries = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "finops_http_request_retries_total",
			Help: "Total number of HTTP request retry attempts",
		},
		[]string{"endpoint"},
	)

	// HTTPRequestsInFlight tracks the number of currently active HTTP requests
	HTTPRequestsInFlight = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "finops_http_requests_in_flight",
			Help: "Number of HTTP requests currently in flight",
		},
		[]string{"endpoint"},
	)

	// MetricsQueryTotal tracks metrics query operations by resolution and status
	MetricsQueryTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "finops_metrics_query_total",
			Help: "Total number of metrics queries executed",
		},
		[]string{"resolution", "status"},
	)

	// MetricsQueryDuration tracks the duration of metrics queries
	MetricsQueryDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "finops_metrics_query_duration_seconds",
			Help:    "Duration of metrics queries in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"resolution"},
	)

	// MetricsQueryErrors tracks metrics query errors by type
	MetricsQueryErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "finops_metrics_query_errors_total",
			Help: "Total number of metrics query errors",
		},
		[]string{"resolution", "error_type"},
	)
)

// Made with Bob
