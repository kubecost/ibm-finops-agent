package datasourcehealth

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// InstrumentedHTTPClient wraps an http.Client and automatically tracks metrics for all requests
type InstrumentedHTTPClient struct {
	client   *http.Client
	endpoint string // logical endpoint name for metrics (e.g., "node_stats", "cloudability_api")
}

// NewInstrumentedHTTPClient creates a new instrumented HTTP client
func NewInstrumentedHTTPClient(client *http.Client, endpoint string) *InstrumentedHTTPClient {
	if client == nil {
		client = http.DefaultClient
	}
	return &InstrumentedHTTPClient{
		client:   client,
		endpoint: endpoint,
	}
}

// Do executes an HTTP request and tracks metrics
func (c *InstrumentedHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return c.DoWithContext(req.Context(), req)
}

// DoWithContext executes an HTTP request with context and tracks metrics
func (c *InstrumentedHTTPClient) DoWithContext(ctx context.Context, req *http.Request) (*http.Response, error) {
	// Track in-flight requests
	HTTPRequestsInFlight.WithLabelValues(c.endpoint).Inc()
	defer HTTPRequestsInFlight.WithLabelValues(c.endpoint).Dec()

	// Track request duration
	start := time.Now()
	
	// Execute the request
	resp, err := c.client.Do(req.WithContext(ctx))
	duration := time.Since(start).Seconds()

	// Determine status code and failure type
	statusCode := "unknown"
	if resp != nil {
		statusCode = strconv.Itoa(resp.StatusCode)
	}

	method := req.Method
	if method == "" {
		method = "GET"
	}

	// Track metrics based on outcome
	if err != nil {
		// Request failed at network/transport level
		failureType := classifyError(err)
		HTTPRequestFailures.WithLabelValues(c.endpoint, failureType).Inc()
		HTTPRequestsTotal.WithLabelValues(c.endpoint, method, "error").Inc()
		HTTPRequestDuration.WithLabelValues(c.endpoint, method, "error").Observe(duration)
		return resp, err
	}

	// Request succeeded at transport level, track by status code
	HTTPRequestsTotal.WithLabelValues(c.endpoint, method, statusCode).Inc()
	HTTPRequestDuration.WithLabelValues(c.endpoint, method, statusCode).Observe(duration)

	// Track failures for non-2xx status codes
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		failureType := classifyHTTPStatus(resp.StatusCode)
		HTTPRequestFailures.WithLabelValues(c.endpoint, failureType).Inc()
	}

	return resp, nil
}

// TrackRetry increments the retry counter for this endpoint
func (c *InstrumentedHTTPClient) TrackRetry() {
	HTTPRequestRetries.WithLabelValues(c.endpoint).Inc()
}

// GetUnderlyingClient returns the wrapped http.Client for cases where direct access is needed
func (c *InstrumentedHTTPClient) GetUnderlyingClient() *http.Client {
	return c.client
}

// classifyError categorizes errors for metrics
func classifyError(err error) string {
	if err == nil {
		return "none"
	}

	// Check for common error types
	switch {
	case isTimeoutError(err):
		return "timeout"
	case isConnectionError(err):
		return "connection_error"
	case isContextError(err):
		return "context_cancelled"
	case isDNSError(err):
		return "dns_error"
	default:
		return "unknown_error"
	}
}

// classifyHTTPStatus categorizes HTTP status codes for failure tracking
func classifyHTTPStatus(statusCode int) string {
	switch {
	case statusCode >= 500:
		return fmt.Sprintf("5xx_server_error_%d", statusCode)
	case statusCode == 404:
		return "404_not_found"
	case statusCode == 403:
		return "403_forbidden"
	case statusCode == 401:
		return "401_unauthorized"
	case statusCode >= 400:
		return fmt.Sprintf("4xx_client_error_%d", statusCode)
	case statusCode >= 300:
		return fmt.Sprintf("3xx_redirect_%d", statusCode)
	default:
		return fmt.Sprintf("unexpected_%d", statusCode)
	}
}

// Error type checking helpers
func isTimeoutError(err error) bool {
	if urlErr, ok := err.(*url.Error); ok {
		return urlErr.Timeout()
	}
	return false
}

func isConnectionError(err error) bool {
	if urlErr, ok := err.(*url.Error); ok {
		return urlErr.Err != nil && !urlErr.Timeout()
	}
	return false
}

func isContextError(err error) bool {
	return err == context.Canceled || err == context.DeadlineExceeded
}

func isDNSError(err error) bool {
	if urlErr, ok := err.(*url.Error); ok {
		_, isDNS := urlErr.Err.(*net.DNSError)
		return isDNS
	}
	return false
}

// Made with Bob
