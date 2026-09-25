package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	gohttp "net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := run(ctx)
	stop()
	if err != nil {
		log.Errorf("IBM Finops Agent exiting: %s", err)
		os.Exit(1)
	}
	log.Infof("IBM Finops Agent stopped")
}

// listenAddr is the main listener's address. Tests change it.
var listenAddr = fmt.Sprintf(":%d", http.DefaultPort)

// run starts the agent and runs it until ctx is cancelled (SIGTERM), then shuts it down within
// SHUTDOWN_TIMEOUT. It returns startup errors instead of exiting, so that whatever had started
// is stopped on the way out, and returns nil after a shutdown, even one that ran out of time:
// anything undelivered stays on disk for the next start.
func run(ctx context.Context) error {
	if env.IsAutoMemLimitEnabled() {
		err := monitor.StartMemoryLimiter()
		if err != nil {
			log.Warnf("Auto Memory Limit was Enabled, but failed to start: %s", err)
		}
	}

	log.Infof("Starting IBM Finops Agent version %s", version.FriendlyVersion())

	// Initialize/Bootstrap the Agent Data Source
	emissionInterval := env.GetExporterEmissionInterval()
	if emissionInterval <= 0 {
		return fmt.Errorf("%s must be a positive duration, got %s", env.ExporterEmissionIntervalEnvVar, emissionInterval)
	}

	// Shared application utilities (http router, diagnostics, etc...)
	router := httprouter.New()
	diag := diagnostics.NewDiagnosticService()

	// Health model (docs/reliability/FINDINGS.md chunk 08). Until components register, the agent
	// is live and not ready; /readyz reports the startup phase.
	registry := health.NewRegistry()
	registerHealthRoutes(router, registry)

	// The HTTP server starts before the data source and emitters, so the probes answer during
	// startup.
	a := newAgent(registry)
	server := http.NewHttpServer(router, http.DefaultPort)
	server.Addr = listenAddr
	if _, err := a.listenAndServe(server); err != nil {
		return fmt.Errorf("failed to start the HTTP server: %w", err)
	}
	defer func() {
		if err := a.shutdown(env.GetShutdownTimeout()); err != nil {
			log.Errorf("Shutdown was incomplete; anything undelivered stays on disk for the next start: %s", err)
		}
	}()

	// Profiling endpoints, if enabled, are on their own loopback-only listener (F-24).
	if env.IsPProfEnabled() {
		if _, err := a.listenAndServe(http.NewPprofServer(http.DefaultPprofPort)); err != nil {
			return fmt.Errorf("failed to start the pprof server: %w", err)
		}
	}

	// Initialize Kubernetes Client
	kubeConfig, err := kubeconfig.LoadKubeconfig("")
	if err != nil {
		return fmt.Errorf("failed to load Kubernetes configuration: %w", err)
	}

	kubeClientset, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return fmt.Errorf("failed to build Kubernetes client: %w", err)
	}

	clusterUID, err := kubeconfig.GetClusterUID(kubeClientset)
	if err != nil {
		return fmt.Errorf("failed to determine cluster UID: %w", err)
	}

	// Informer sync (bounded by INFORMER_SYNC_TIMEOUT) and the collector WAL restore run here.
	registry.SetPhase(health.PhaseDataSource)
	dataSource, err := core.NewAgentDataSource(ctx, kubeConfig, kubeClientset, router, diag, emissionInterval)
	if err != nil {
		if ctx.Err() != nil {
			return nil // stopped during startup
		}
		return fmt.Errorf("failed to start the data source: %w", err)
	}
	a.dataSource = dataSource
	if ctx.Err() != nil {
		return nil
	}
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
			return fmt.Errorf("invalid kubecost emitter config: %w", err)
		}

		kubecostCloudProvider := dataSource.OpenCostCloudCostProvider()
		if kubecostCloudProvider == nil {
			return errors.New("public cloud provider pricing API never initialized; set OPENCOST_SOURCE_ENABLED=true")
		}

		// Update the snapshot config to include the kubecost emitter's required resources
		snapshotConfig = snapshotConfig.WithKubernetesSnapshotConfig(
			emitter.NewKubernetesSnapshotConfigFromEnabled(kubecostEmitterConfig.KubernetesResourcesRequired),
		)

		a.emitters = append(a.emitters, kubecost.NewKubecostEmitter(kubecostCloudProvider, diag, kubecostEmitterConfig))
	}
	if env.IsCloudyEmitterEnabled() {
		cldyConfig, err := cldy.NewEmitterConfigFromEnv()
		if err != nil {
			return fmt.Errorf("invalid cloudability emitter config: %w", err)
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

		cldyEmitter, err := cldy.NewEmitter(ctx, cldyConfig)
		if err != nil {
			return fmt.Errorf("failed to start the cloudability emitter: %w", err)
		}
		a.emitters = append(a.emitters, cldyEmitter)
	}
	if env.IsTurboEmitterEnabled() {
		log.Infof("Turbonomic emitter not yet implemented.")
		//emitters = append(emitters, emitter.NewTurboEmitter(dataSource))
	}

	// TODO: Return an error once we have full support for all emitters.
	/*
		if len(a.emitters) == 0 {
			return errors.New("no emitters enabled")
		}
	*/

	// Any emitter that implements health.Component or health.ConditionReporter takes part in
	// readiness and /status.
	for _, e := range a.emitters {
		registry.RegisterAny(string(e.ID()), e)
	}

	snapshotProvider := emitter.NewConcurrentSnapshotProvider(snapshotConfig)
	exporter := emitter.NewExporterWithConfig(dataSource, snapshotProvider, emitter.ExporterConfig{
		SnapshotTimeout: env.GetExporterSnapshotTimeout(),
		EmitTimeout:     env.GetExporterEmitTimeout(),
	}, a.emitters...)

	if ok := exporter.Start(emissionInterval); !ok {
		return errors.New("failed to start exporter")
	}
	a.exporter = exporter
	registry.Register("exporter", emitter.HealthComponent(exporter))
	registry.SetPhase(health.PhaseRunning)

	return a.wait(ctx)
}

// agent holds what run started, for shutdown. Fields are nil until started.
type agent struct {
	registry   *health.Registry
	dataSource *core.AgentDataSource
	exporter   emitter.Exporter
	emitters   []emitter.Emitter

	servers  []*gohttp.Server
	serving  sync.WaitGroup
	serveErr chan error
}

func newAgent(registry *health.Registry) *agent {
	return &agent{registry: registry, serveErr: make(chan error, 1)}
}

// listenAndServe binds srv.Addr and serves srv until shutdown. A bind failure is returned.
func (a *agent) listenAndServe(srv *gohttp.Server) (net.Addr, error) {
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return nil, err
	}
	a.servers = append(a.servers, srv)
	a.serving.Go(func() {
		if err := srv.Serve(ln); !errors.Is(err, gohttp.ErrServerClosed) {
			select {
			case a.serveErr <- fmt.Errorf("serving %s: %w", srv.Addr, err):
			default:
			}
		}
	})
	return ln.Addr(), nil
}

// wait blocks until ctx is cancelled or a listener fails.
func (a *agent) wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		log.Infof("Received a termination signal")
		return nil
	case err := <-a.serveErr:
		return err
	}
}

// stopper is an emitter that drains at shutdown within a context: the Cloudability emitter, and
// the Kubecost emitter once it has Stop (chunk 07).
type stopper interface {
	Stop(ctx context.Context) error
}

// serverShutdownReserve is the part of the shutdown budget kept for closing the HTTP servers
// after the exporter and emitters.
const serverShutdownReserve = 2 * time.Second

// shutdown stops what run started, within budget:
//  1. the exporter, so no cycle starts and the one in flight ends;
//  2. the emitters, concurrently so that one that hangs doesn't hold up the other (I5): the
//     Cloudability emitter packages its last samples and runs one last upload cycle, and the
//     Kubecost emitter stops its controllers;
//  3. the informers and node-stats collection;
//  4. the HTTP servers, last, so the probes answer while the emitters drain. Readiness fails
//     from the start of shutdown.
//
// A step that outlasts its part of the budget is left running and reported; the process is about
// to exit, and whatever wasn't delivered is on disk for the next start.
func (a *agent) shutdown(budget time.Duration) error {
	log.Infof("Shutting down within %s", budget)
	a.registry.SetPhase(health.PhaseStopping)
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	drainCtx, cancelDrain := context.WithTimeout(ctx, max(budget-serverShutdownReserve, budget/2))
	defer cancelDrain()

	var mu sync.Mutex
	var errs []error
	record := func(what string, err error) {
		if err != nil {
			mu.Lock()
			errs = append(errs, fmt.Errorf("%s: %w", what, err))
			mu.Unlock()
		}
	}

	if a.exporter != nil {
		record("exporter", within(drainCtx, func() error {
			a.exporter.Stop()
			return nil
		}))
	}
	var drains sync.WaitGroup
	for _, e := range a.emitters {
		s, ok := e.(stopper)
		if !ok {
			continue
		}
		drains.Go(func() {
			record(string(e.ID()), within(drainCtx, func() error { return s.Stop(drainCtx) }))
		})
	}
	drains.Wait()
	cancelDrain()

	if a.dataSource != nil {
		record("data source", a.dataSource.Stop(ctx))
	}
	for _, srv := range a.servers {
		if err := srv.Shutdown(ctx); err != nil {
			record("HTTP server "+srv.Addr, err)
			_ = srv.Close()
		}
	}
	a.serving.Wait()
	return errors.Join(errs...)
}

// within runs f and waits for it until ctx is done. If ctx ends first, f is left running.
func within(ctx context.Context, f func() error) error {
	done := make(chan error, 1)
	go func() { done <- f() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
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
