package main

import (
	"context"
	gohttp "net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ibm/finops-agent/cldy"
	"github.com/ibm/finops-agent/kubecost"
	"github.com/ibm/finops-agent/pkg/core"
	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/ibm/finops-agent/pkg/env"
	"github.com/ibm/finops-agent/pkg/http"
	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

const EmissionFrequency time.Duration = time.Minute

func initLogging() {
	// Setup viper to read from the env, this allows reading flags from the command line or the env
	// using the format 'LOG_LEVEL'
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	// Default to using pretty formatting
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerologger.Logger = zerologger.Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339Nano, NoColor: true})
	//zerolog.SetGlobalLevel(zerolog.TraceLevel)
}

// entry point for finops-agent
func main() {
	initLogging()

	log.Infof("Starting IBM Finops Agent...")

	// Shared application utilities (http router, diagnostics, etc...)
	router := httprouter.New()

	// Add profiling endpoints if enabled
	if env.IsPProfEnabled() {
		router.HandlerFunc(gohttp.MethodGet, "/debug/pprof/", pprof.Index)
		router.HandlerFunc(gohttp.MethodGet, "/debug/pprof/cmdline", pprof.Cmdline)
		router.HandlerFunc(gohttp.MethodGet, "/debug/pprof/profile", pprof.Profile)
		router.HandlerFunc(gohttp.MethodGet, "/debug/pprof/symbol", pprof.Symbol)
		router.HandlerFunc(gohttp.MethodGet, "/debug/pprof/trace", pprof.Trace)
		router.Handler(gohttp.MethodGet, "/debug/pprof/goroutine", pprof.Handler("goroutine"))
		router.Handler(gohttp.MethodGet, "/debug/pprof/heap", pprof.Handler("heap"))
	}

	diag := diagnostics.NewDiagnosticService()

	// Setup the HTTP server
	server := http.NewHttpServer(router, 9003)
	go func() {
		err := server.ListenAndServe()
		if err != nil {
			log.Errorf("Error starting HTTP server: %s", err)
		}
	}()

	defer server.Shutdown(context.Background())

	// Initialize/Bootstrap the Agent Data Source
	dataSource := core.NewAgentDataSource(router, diag, EmissionFrequency)

	var emitters []emitter.Emitter

	if env.IsKubecostEmitterEnabled() {
		emitters = append(emitters, kubecost.NewKubecostEmitter(diag, kubecost.NewEmitterConfigFromEnv()))
	}
	if env.IsCloudyEmitterEnabled() {
		cldyConfig, err := cldy.NewEmitterConfigFromEnv()
		if err != nil {
			panic("invalid cloudability emitter config " + err.Error())
		}
		emitters = append(emitters, cldy.NewEmitter(cldyConfig, make(chan struct{})))
	}
	if env.IsTurboEmitterEnabled() {
		//emitters = append(emitters, emitter.NewTurboEmitter(dataSource))
	}

	// TODO: Uncomment once we have full support for all emitters.
	/*
		if len(emitters) == 0 {
			panic("No emitters enabled!")
		}
	*/

	snapshotProvider := emitter.NewConcurrentSnapshotProvider(emitter.NewSnapshotConfigFromEnv())
	exporter := emitter.NewExporter(dataSource, snapshotProvider, emitters...)

	if ok := exporter.Start(EmissionFrequency); !ok {
		panic("Failed to start exporter")
	}

	defer exporter.Stop()

	WaitForSignal()
}

// WaitForSignal waits for a termination signal (SIGINT or SIGTERM) and then exits the program.
func WaitForSignal() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan struct{}, 1)

	go func() {
		defer close(done)
		<-signalChan
	}()

	<-done
}
