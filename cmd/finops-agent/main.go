package main

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ibm/finops-agent/kubecost"
	"github.com/ibm/finops-agent/pkg/core"
	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/ibm/finops-agent/pkg/http"
	"github.com/julienschmidt/httprouter"
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

	// Initialize/Bootstrap the Agent Data Source
	dataSource := core.NewAgentDataSource()

	// TODO: load emitters with data source
	kc := kubecost.NewKubecostEmitter(kubecost.NewEmitterConfigFromEnv())
	// cldy := emitter.NewCloudyEmitter(dataSource)
	// turbo := emitter.NewTurboEmitter(dataSource)

	snapshotProvider := emitter.NewConcurrentSnapshotProvider()
	exporter := emitter.NewExporter(dataSource, snapshotProvider, kc /*, cldy, turbo*/)

	if ok := exporter.Start(EmissionFrequency); !ok {
		panic("Failed to start exporter")
	}

	router := httprouter.New()
	// we can probably hook this router into the emitters to append any debug/diagnostic endpoints
	// or we can initialize them directly in the `AgentDataSource` instantiation.

	server := http.NewHttpServer(router, 9003)
	go func() {
		err := server.ListenAndServe()
		if err != nil {
			log.Errorf("Error starting HTTP server: %s", err)
		}
	}()

	defer server.Shutdown(context.Background())
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
