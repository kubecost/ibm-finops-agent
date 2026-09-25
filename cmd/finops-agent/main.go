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
	"github.com/ibm/finops-agent/pkg/cluster"
	"github.com/ibm/finops-agent/pkg/core"
	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/ibm/finops-agent/pkg/env"
	"github.com/ibm/finops-agent/pkg/health"
	"github.com/ibm/finops-agent/pkg/http"
	"github.com/ibm/finops-agent/pkg/nodes"
	"github.com/ibm/finops-agent/pkg/version"
	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/kubeconfig"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/monitor"
	"github.com/spf13/viper"
	"k8s.io/client-go/kubernetes"
)

func initLogging() {
	// Setup viper to read from the env, this allows reading flags from the command line or the env
	// using the format 'LOG_LEVEL'
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	log.InitLogging(true)
}

// entry point for finops-agent
func main() {
	initLogging()

	if env.IsAutoMemLimitEnabled() {
		err := monitor.StartMemoryLimiter()
		if err != nil {
			log.Warnf("Auto Memory Limit was Enabled, but failed to start: %s", err)
		}
	}

	log.Infof("Starting IBM Finops Agent version %s", version.FriendlyVersion())

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

	// Health model (docs/reliability/FINDINGS.md chunk 08). Until components register, the agent
	// is live and not ready; /readyz reports the startup phase.
	registry := health.NewRegistry()
	registerHealthRoutes(router, registry)

	var emitters []emitter.Emitter

	// Setup the HTTP server - ensure the goroutine starts before continuing to initialization
	// of the data source and emitters
	started := make(chan struct{})
	server := http.NewHttpServer(router, 9003)
	go func() {
		close(started)

		err := server.ListenAndServe()
		if err != nil {
			log.Errorf("Error starting HTTP server: %s", err)
		}
	}()

	<-started
	defer func() {
		err := server.Shutdown(context.Background())
		if err != nil {
			log.Errorf("Error shutting down HTTP server: %s", err)
		}
	}()

	// Initialize/Bootstrap the Agent Data Source
	emissionInterval := env.GetExporterEmissionInterval()
	if emissionInterval <= 0 {
		log.Fatalf("%s must be a positive duration, got %s", env.ExporterEmissionIntervalEnvVar, emissionInterval)
	}

	// Initialize Kubernetes Client
	kubeConfig, err := kubeconfig.LoadKubeconfig("")
	if err != nil {
		log.Fatalf("Failed to load Kubernetes configuration: %s", err.Error())
	}

	kubeClientset, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		log.Fatalf("Failed to build Kubernetes client: %s", err.Error())
	}

	clusterUID, err := kubeconfig.GetClusterUID(kubeClientset)
	if err != nil {
		log.Fatalf("Failed to determine cluster UID: %s", err)
	}

	// Informer sync (bounded by INFORMER_SYNC_TIMEOUT) and the collector WAL restore run here.
	registry.SetPhase(health.PhaseDataSource)
	dataSource := core.NewAgentDataSource(kubeConfig, kubeClientset, router, diag, emissionInterval)
	registerDataSourceHealth(registry, dataSource, time.Now())
	registry.SetPhase(health.PhaseEmitters)

	// Snapshot configuration will gather specific kubernetes resource requirements
	// from each emitter that is enabled such that we only snapshot the resources that
	// are required by the emitters.
	snapshotConfig := emitter.NewSnapshotConfigFromEnv()

	if env.IsKubecostEmitterEnabled() {
		kubecostEmitterConfig := kubecost.NewEmitterConfigFromEnv(clusterUID)
		kubecostEmitterConfig.QueryResolution = dataSource.OpenCostSource().Resolution()

		if err := kubecost.ValidateConfig(kubecostEmitterConfig); err != nil {
			panic("invalid kubecost emitter config: " + err.Error())
		}

		kubecostCloudProvider := dataSource.OpenCostCloudCostProvider()
		if kubecostCloudProvider == nil {
			panic("Public cloud provider pricing API never initialzed. Set OPENCOST_SOURCE_ENABLED=true.")
		}

		// Update the snapshot config to include the kubecost emitter's required resources
		snapshotConfig = snapshotConfig.WithKubernetesSnapshotConfig(
			emitter.NewKubernetesSnapshotConfigFromEnabled(kubecostEmitterConfig.KubernetesResourcesRequired),
		)

		emitters = append(emitters, kubecost.NewKubecostEmitter(kubecostCloudProvider, diag, kubecostEmitterConfig))
	}
	if env.IsCloudyEmitterEnabled() {
		cldyConfig, err := cldy.NewEmitterConfigFromEnv()
		if err != nil {
			panic("invalid cloudability emitter config: " + err.Error())
		}

		clusterInfo := dataSource.ClusterMetadata().GetClusterInfo()
		if clusterInfo != nil {
			cldyConfig.ClusterVersion = version.FormatVersionInfo(clusterInfo.Version)
			cldyConfig.ClusterVersionGit = clusterInfo.Version.GitVersion
			cldyConfig.ClusterVersionMajor = clusterInfo.Version.Major
			cldyConfig.ClusterVersionMinor = clusterInfo.Version.Minor
		}

		// Update the snapshot config to include the cloudability emitter's required resources
		snapshotConfig = snapshotConfig.WithKubernetesSnapshotConfig(
			emitter.NewKubernetesSnapshotConfigFromEnabled(cldyConfig.KubernetesResourcesRequired),
		)

		emitters = append(emitters, cldy.NewEmitter(cldyConfig, make(chan struct{})))
	}
	if env.IsTurboEmitterEnabled() {
		log.Infof("Turbonomic emitter not yet implemented.")
		//emitters = append(emitters, emitter.NewTurboEmitter(dataSource))
	}

	// TODO: Uncomment once we have full support for all emitters.
	/*
		if len(emitters) == 0 {
			panic("No emitters enabled!")
		}
	*/

	// Any emitter that implements health.Component or health.ConditionReporter takes part in
	// readiness and /status.
	for _, e := range emitters {
		registry.RegisterAny(string(e.ID()), e)
	}

	snapshotProvider := emitter.NewConcurrentSnapshotProvider(snapshotConfig)
	exporter := emitter.NewExporterWithConfig(dataSource, snapshotProvider, emitter.ExporterConfig{
		SnapshotTimeout: env.GetExporterSnapshotTimeout(),
		EmitTimeout:     env.GetExporterEmitTimeout(),
	}, emitters...)

	if ok := exporter.Start(emissionInterval); !ok {
		panic("Failed to start exporter")
	}
	registry.Register("exporter", emitter.HealthComponent(exporter))
	registry.SetPhase(health.PhaseRunning)

	defer exporter.Stop()

	WaitForSignal()
}

// registerHealthRoutes serves the registry's probes and status on router.
func registerHealthRoutes(router *httprouter.Router, registry *health.Registry) {
	router.HandlerFunc(gohttp.MethodGet, "/healthz", registry.LivenessHandler())
	router.HandlerFunc(gohttp.MethodGet, "/readyz", registry.ReadinessHandler())
	router.HandlerFunc(gohttp.MethodGet, "/startupz", registry.StartupHandler())
	router.HandlerFunc(gohttp.MethodGet, "/status", registry.StatusHandler())
}

// registerDataSourceHealth registers the informers' sync state and node-stats freshness. Both are
// readiness only: an RBAC fault or unreachable kubelets aren't fixed by a restart (D1, D9).
func registerDataSourceHealth(registry *health.Registry, ds core.DataSource, started time.Time) {
	if sr, ok := ds.Cluster().(cluster.SyncReporter); ok {
		registry.Register("informers", cluster.SyncComponent(sr))
	}
	if timer, ok := ds.StatsSummary().(nodes.CollectionTimer); ok {
		maxAge := time.Duration(cldy.MaxStaleUploadCycles) * cldy.UploadFrequencyDuration
		registry.Register("node_stats", nodes.FreshnessComponent(timer, maxAge, started))
	}
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
