package main

import (
	"fmt"
	"github.com/ibm/finops-agent/cldy"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ibm/finops-agent/pkg/core"
	"github.com/ibm/finops-agent/pkg/emitter"
)

const EmissionFrequency time.Duration = time.Minute

// entry point for finops-agent
func main() {
	fmt.Println("Starting IBM Finops Agent...")

	// Initialize/Bootstrap the Agent Data Source
	dataSource := core.NewAgentDataSource()

	// TODO: load emitters with data source
	// kc := emitter.NewKubecostEmitter(dataSource)
	tempDir, err := os.MkdirTemp("", "")
	if err != nil {
		fmt.Println("Error creating temp directory")
	}
	fmt.Println(tempDir)
	cldyconfig := cldy.EmitterConfig{
		UploaderConfig: cldy.UploaderConfig{
			ApptioConfig: cldy.ApptioConfig{
				KeyAccess:       os.Getenv("CLDY_KEY_ACCESS"),
				KeySecret:       os.Getenv("CLDY_KEY_SECRET"),
				EnvID:           os.Getenv("CLDY_ENV_ID"),
				Timeout:         time.Second * 30,
				Retries:         1,
				FrontdoorURL:    "https://frontdoor-stage.apptio.com",
				CloudabilityURL: "https://api-s.cloudability.com",
			},
			UploadFrequency: time.Minute,
			ScratchDir:      tempDir,
		},
		EmitAsJson: true,
	}
	stop := make(chan struct{})
	defer close(stop)
	fmt.Println("Starting cldy emitter")
	cldyEmitter := cldy.NewEmitter(cldyconfig, stop)

	snapshotProvider := emitter.NewConcurrentSnapshotProvider()
	exporter := emitter.NewExporter(dataSource, snapshotProvider, cldyEmitter)

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
