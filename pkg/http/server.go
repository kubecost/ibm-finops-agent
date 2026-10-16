package http

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/ibm/finops-agent/pkg/version"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// DefaultPort is the port of the main listener: probes, /status, /metrics and the OpenCost
// endpoints.
const DefaultPort = 9003

// DefaultPprofPort is the port of the pprof listener, which only binds 127.0.0.1.
const DefaultPprofPort = 6060

// Timeouts of the main listener. ReadHeaderTimeout cuts off a client that never finishes its
// request header; WriteTimeout bounds a handler, including /metrics and the OpenCost queries.
const (
	ReadHeaderTimeout = 10 * time.Second
	ReadTimeout       = 30 * time.Second
	WriteTimeout      = 2 * time.Minute
	IdleTimeout       = 2 * time.Minute
)

// pprofWriteTimeout leaves room for a CPU profile or trace, whose duration must be shorter than
// the server's WriteTimeout.
const pprofWriteTimeout = 5 * time.Minute

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

// NewHttpServer returns the main server on port, serving h with /version and /metrics. It sends
// no CORS headers: nothing browser-based calls the agent (D6).
func NewHttpServer(h http.Handler, port int) *http.Server {
	rootMux := http.NewServeMux()
	rootMux.HandleFunc("/version", Version)
	rootMux.Handle("/metrics", promhttp.Handler())
	rootMux.Handle("/", h)

	return &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           metrics.ResponseMetricMiddleware(rootMux),
		ReadHeaderTimeout: ReadHeaderTimeout,
		ReadTimeout:       ReadTimeout,
		WriteTimeout:      WriteTimeout,
		IdleTimeout:       IdleTimeout,
	}
}

// NewPprofServer returns a server for the pprof endpoints on 127.0.0.1:port, reachable only from
// inside the pod (kubectl port-forward), never on the probe port.
func NewPprofServer(port int) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	return &http.Server{
		Addr:              fmt.Sprintf("127.0.0.1:%d", port),
		Handler:           mux,
		ReadHeaderTimeout: ReadHeaderTimeout,
		ReadTimeout:       ReadTimeout,
		WriteTimeout:      pprofWriteTimeout,
		IdleTimeout:       IdleTimeout,
	}
}
