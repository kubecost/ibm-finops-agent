# Data Source Health Package

The datasourcehealth package provides Prometheus-based metrics instrumentation for the IBM FinOps Agent, enabling comprehensive monitoring of data source HTTP requests and metrics queries.

## Overview

This package automatically tracks:
- HTTP request success/failure rates by endpoint and status code
- Request duration and latency
- Retry attempts
- In-flight request counts
- Metrics query performance and errors

## Metrics Exposed

### HTTP Request Metrics

#### `finops_http_requests_total`
Counter tracking total HTTP requests.

**Labels:**
- `endpoint`: Logical endpoint name (e.g., `node_stats`, `cloudability_api`)
- `method`: HTTP method (GET, POST, etc.)
- `status_code`: HTTP status code or "error" for transport failures

**Example:**
```
finops_http_requests_total{endpoint="node_stats",method="GET",status_code="200"} 1523
finops_http_requests_total{endpoint="cloudability_api",method="POST",status_code="503"} 12
```

#### `finops_http_request_duration_seconds`
Histogram tracking HTTP request duration in seconds.

**Labels:**
- `endpoint`: Logical endpoint name
- `method`: HTTP method
- `status_code`: HTTP status code or "error"

**Buckets:** Default Prometheus buckets (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)

#### `finops_http_request_failures_total`
Counter tracking HTTP request failures by type.

**Labels:**
- `endpoint`: Logical endpoint name
- `failure_type`: Type of failure (e.g., `timeout`, `404_not_found`, `5xx_server_error_503`, `connection_error`)

**Example:**
```
finops_http_request_failures_total{endpoint="node_stats",failure_type="timeout"} 5
finops_http_request_failures_total{endpoint="cloudability_api",failure_type="5xx_server_error_503"} 3
```

#### `finops_http_request_retries_total`
Counter tracking retry attempts per endpoint.

**Labels:**
- `endpoint`: Logical endpoint name

#### `finops_http_requests_in_flight`
Gauge tracking currently active HTTP requests.

**Labels:**
- `endpoint`: Logical endpoint name

### Metrics Query Metrics

#### `finops_metrics_query_total`
Counter tracking metrics query operations.

**Labels:**
- `resolution`: Query resolution (`10m`, `1h`, `24h`)
- `status`: Query status (`success`, `error`)

#### `finops_metrics_query_duration_seconds`
Histogram tracking metrics query duration.

**Labels:**
- `resolution`: Query resolution (`10m`, `1h`, `24h`)

#### `finops_metrics_query_errors_total`
Counter tracking metrics query errors.

**Labels:**
- `resolution`: Query resolution (`10m`, `1h`, `24h`)
- `error_type`: Type of error (e.g., `snapshot_not_available`, `invalid_resolution`)

## Usage

### Instrumenting HTTP Clients

The package provides `InstrumentedHTTPClient` which wraps `http.Client`:

```go
import "github.com/ibm/finops-agent/pkg/datasourcehealth"

// Create instrumented client
httpClient := &http.Client{Timeout: 10 * time.Second}
instrumentedClient := datasourcehealth.NewInstrumentedHTTPClient(httpClient, "my_endpoint")

// Use like a normal http.Client
req, _ := http.NewRequest("GET", "https://api.example.com", nil)
resp, err := instrumentedClient.Do(req)
```

### Tracking Retries

```go
// Track retry attempts
instrumentedClient.TrackRetry()
```

### Accessing Metrics

Metrics are automatically registered with Prometheus and exposed via the `/metrics` endpoint. No additional registration is required.

## Integration Points

The datasourcehealth package is integrated at the following locations:

1. **Node Stats Queries** (`pkg/nodes/request.go`)
   - Endpoint: `node_stats`
   - Tracks kubelet stats summary API calls

2. **Cloudability API** (`cldy/clients.go`)
   - Endpoint: `cloudability_api`
   - Tracks uploads and authentication requests

3. **Metrics Queries** (`kubecost/adapters/metricsquerier.go`)
   - Tracks OpenCost metrics query performance
   - Monitors snapshot availability

## Querying Metrics

### Example PromQL Queries

**Request success rate by endpoint:**
```promql
rate(finops_http_requests_total{status_code="200"}[5m]) 
/ 
rate(finops_http_requests_total[5m])
```

**95th percentile request latency:**
```promql
histogram_quantile(0.95, 
  rate(finops_http_request_duration_seconds_bucket[5m])
)
```

**Error rate by failure type:**
```promql
rate(finops_http_request_failures_total[5m])
```

**Metrics query success rate:**
```promql
rate(finops_metrics_query_total{status="success"}[5m])
/
rate(finops_metrics_query_total[5m])
```

## Failure Classification

### HTTP Status Codes
- `404_not_found`: Resource not found
- `403_forbidden`: Access denied
- `401_unauthorized`: Authentication required
- `4xx_client_error_XXX`: Other 4xx errors
- `5xx_server_error_XXX`: Server errors (503, 500, etc.)
- `3xx_redirect_XXX`: Redirect responses

### Transport Errors
- `timeout`: Request timeout
- `connection_error`: Network connection failure
- `context_cancelled`: Request cancelled via context
- `dns_error`: DNS resolution failure
- `unknown_error`: Other transport errors

## Metrics Export

When `LOG_EXPORT_ENABLED` is set, data source health metrics are automatically exported alongside logs:

### Export Behavior
- Snapshots captured at each `LOG_EXPORT_INTERVAL` (default: 5 minutes)
- Exported as timestamped JSON files: `dataSourceMetrics-YYYYMMDDHHMMSS.json.gz`
- Uploaded to same bucket path as logs: `<path_prefix>/<cluster>/`
- Compressed with gzip for efficient storage

### Snapshot Format
```json
{
  "timestamp": "2026-02-10T03:00:00Z",
  "metrics": {
    "finops_http_requests_total": {
      "type": "COUNTER",
      "help": "Total number of HTTP requests made by the finops agent",
      "values": [
        {
          "labels": {
            "endpoint": "node_stats",
            "method": "GET",
            "status_code": "200"
          },
          "value": 1523
        }
      ]
    }
  }
}
```

### Use Cases
- Historical analysis of data source health trends
- Debugging intermittent connectivity issues
- Capacity planning and performance optimization
- Compliance and audit trails

## Testing

Run tests with:
```bash
go test ./pkg/datasourcehealth/...
```

## Performance Considerations

- Metrics use Prometheus's efficient storage format
- Label cardinality is controlled (limited set of endpoints and status codes)
- Histograms use default buckets suitable for typical HTTP request latencies
- In-flight gauge updates are atomic and lock-free
- Snapshot export adds minimal overhead (~1-2ms per interval)