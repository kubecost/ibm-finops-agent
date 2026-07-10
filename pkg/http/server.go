package http

import (
	"fmt"
	"net/http"

	"github.com/ibm/finops-agent/pkg/version"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/cors"
)

// HealthChecker is a function that returns true if the agent is healthy.
// When nil, the health endpoint unconditionally reports healthy.
type HealthChecker func() bool

func healthzHandler(checker HealthChecker) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		if checker != nil && !checker() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusOK)
	}
}

func Version(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	_, err := fmt.Fprintf(w, "%s", version.Version)
	if err != nil {
		log.Errorf("error retrieving version on api request: %s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func NewHttpServer(h http.Handler, port int, healthCheck HealthChecker) *http.Server {
	rootMux := http.NewServeMux()
	rootMux.HandleFunc("/healthz", healthzHandler(healthCheck))
	rootMux.HandleFunc("/version", Version)
	rootMux.Handle("/metrics", promhttp.Handler())
	rootMux.Handle("/", h)
	telemetryHandler := metrics.ResponseMetricMiddleware(rootMux)
	handler := cors.AllowAll().Handler(telemetryHandler)

	addr := fmt.Sprintf(":%d", port)
	server := &http.Server{Addr: addr, Handler: handler}

	return server
}
