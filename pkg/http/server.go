package http

import (
	"fmt"
	"net/http"
	"time"

	"github.com/ibm/finops-agent/pkg/version"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/cors"
)

const (
	readHeaderTimeout = 10 * time.Second
	readTimeout       = 30 * time.Second
	idleTimeout       = 120 * time.Second
)

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

func NewHttpServer(h http.Handler, port int) *http.Server {
	rootMux := http.NewServeMux()
	rootMux.HandleFunc("/version", Version)
	rootMux.Handle("/metrics", promhttp.Handler())
	rootMux.Handle("/", h)
	telemetryHandler := metrics.ResponseMetricMiddleware(rootMux)
	handler := cors.AllowAll().Handler(telemetryHandler)

	addr := fmt.Sprintf(":%d", port)
	server := &http.Server{
		Addr:    addr,
		Handler: handler,

		// ReadHeaderTimeout is the one that closes the Slowloris hole: without it
		// a client can hold a connection open indefinitely by dribbling out header
		// bytes, and enough such connections exhaust the server's accept capacity.
		ReadHeaderTimeout: readHeaderTimeout,

		// Bounds a complete request, and how long an idle keep-alive connection is
		// retained. Every route on this server is a small GET, so these are
		// generous.
		ReadTimeout: readTimeout,
		IdleTimeout: idleTimeout,

		// WriteTimeout is deliberately not set. When PPROF_ENABLED is on, this
		// server exposes /debug/pprof/profile and /debug/pprof/trace, which stream
		// a response for the whole duration of the requested profile -- 30s by
		// default and longer with ?seconds=. A WriteTimeout would truncate those
		// mid-profile. The read-side timeouts above are what actually mitigate
		// slow-client attacks; WriteTimeout would not add to that.
	}

	return server
}
