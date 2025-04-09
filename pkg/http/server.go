package http

import (
	"fmt"
	"net/http"

	"github.com/opencost/opencost/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/cors"
)

func Healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Length", "0")
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(200)
}

func NewHttpServer(h http.Handler, port int) *http.Server {
	rootMux := http.NewServeMux()
	rootMux.HandleFunc("/healthz", Healthz)
	rootMux.Handle("/metrics", promhttp.Handler())
	rootMux.Handle("/", h)
	telemetryHandler := metrics.ResponseMetricMiddleware(rootMux)
	handler := cors.AllowAll().Handler(telemetryHandler)

	addr := fmt.Sprintf(":%d", port)
	server := &http.Server{Addr: addr, Handler: handler}

	return server
}
