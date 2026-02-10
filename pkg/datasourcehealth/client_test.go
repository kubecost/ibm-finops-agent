package datasourcehealth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestInstrumentedHTTPClient_Success(t *testing.T) {
	// Create a test server that returns 200 OK
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("success"))
	}))
	defer server.Close()

	// Create instrumented client
	client := NewInstrumentedHTTPClient(http.DefaultClient, "test_endpoint")

	// Make request
	req, err := http.NewRequest("GET", server.URL, nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}
}

func TestInstrumentedHTTPClient_4xxError(t *testing.T) {
	// Create a test server that returns 404
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := NewInstrumentedHTTPClient(http.DefaultClient, "test_endpoint")

	req, err := http.NewRequest("GET", server.URL, nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", resp.StatusCode)
	}
}

func TestInstrumentedHTTPClient_5xxError(t *testing.T) {
	// Create a test server that returns 503
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := NewInstrumentedHTTPClient(http.DefaultClient, "test_endpoint")

	req, err := http.NewRequest("GET", server.URL, nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("Expected status 503, got %d", resp.StatusCode)
	}
}

func TestInstrumentedHTTPClient_Timeout(t *testing.T) {
	// Create a test server that delays response
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Create client with very short timeout
	httpClient := &http.Client{
		Timeout: 10 * time.Millisecond,
	}
	client := NewInstrumentedHTTPClient(httpClient, "test_endpoint")

	req, err := http.NewRequest("GET", server.URL, nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	_, err = client.Do(req)
	if err == nil {
		t.Error("Expected timeout error, got nil")
	}
}

func TestInstrumentedHTTPClient_ContextCancellation(t *testing.T) {
	// Create a test server that delays response
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewInstrumentedHTTPClient(http.DefaultClient, "test_endpoint")

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, "GET", server.URL, nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	// Cancel context immediately
	cancel()

	_, err = client.Do(req)
	if err == nil {
		t.Error("Expected context cancellation error, got nil")
	}
}

func TestInstrumentedHTTPClient_TrackRetry(t *testing.T) {
	client := NewInstrumentedHTTPClient(http.DefaultClient, "test_endpoint")

	// This should not panic
	client.TrackRetry()
	client.TrackRetry()
}

func TestInstrumentedHTTPClient_GetUnderlyingClient(t *testing.T) {
	httpClient := &http.Client{Timeout: 5 * time.Second}
	client := NewInstrumentedHTTPClient(httpClient, "test_endpoint")

	underlying := client.GetUnderlyingClient()
	if underlying != httpClient {
		t.Error("GetUnderlyingClient did not return the original client")
	}
}

func TestClassifyError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: "none",
		},
		{
			name:     "generic error",
			err:      errors.New("generic error"),
			expected: "unknown_error",
		},
		{
			name:     "context cancelled",
			err:      context.Canceled,
			expected: "context_cancelled",
		},
		{
			name:     "context deadline exceeded",
			err:      context.DeadlineExceeded,
			expected: "context_cancelled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := classifyError(tt.err)
			if result != tt.expected {
				t.Errorf("classifyError(%v) = %s, expected %s", tt.err, result, tt.expected)
			}
		})
	}
}

func TestClassifyHTTPStatus(t *testing.T) {
	tests := []struct {
		statusCode int
		expected   string
	}{
		{200, "unexpected_200"},
		{404, "404_not_found"},
		{403, "403_forbidden"},
		{401, "401_unauthorized"},
		{400, "4xx_client_error_400"},
		{500, "5xx_server_error_500"},
		{503, "5xx_server_error_503"},
		{301, "3xx_redirect_301"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := classifyHTTPStatus(tt.statusCode)
			if result != tt.expected {
				t.Errorf("classifyHTTPStatus(%d) = %s, expected %s", tt.statusCode, result, tt.expected)
			}
		})
	}
}

func TestNewInstrumentedHTTPClient_NilClient(t *testing.T) {
	// Should use default client when nil is passed
	client := NewInstrumentedHTTPClient(nil, "test_endpoint")
	if client == nil {
		t.Error("NewInstrumentedHTTPClient returned nil")
	}
	if client.GetUnderlyingClient() == nil {
		t.Error("Underlying client is nil")
	}
}

// Made with Bob
