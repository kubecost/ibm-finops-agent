package main

import (
	"fmt"
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
	// cldy := emitter.NewCloudyEmitter(dataSource)
	// turbo := emitter.NewTurboEmitter(dataSource)

	snapshotProvider := emitter.NewConcurrentSnapshotProvider()
	exporter := emitter.NewExporter(dataSource, snapshotProvider /*, kc, cldy, turbo*/)

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
