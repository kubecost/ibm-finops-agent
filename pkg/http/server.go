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
	server := &http.Server{Addr: addr, Handler: handler}

	return server
}
