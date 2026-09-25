package kubecost

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ibm/finops-agent/kubecost/adapters"
	"github.com/ibm/finops-agent/pkg/condition"
	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/ibm/finops-agent/pkg/version"
	"github.com/opencost/opencost/core/pkg/diagnostics"
	diagexporter "github.com/opencost/opencost/core/pkg/diagnostics/exporter"
	ocexporter "github.com/opencost/opencost/core/pkg/exporter"
	heartbeatexporter "github.com/opencost/opencost/core/pkg/heartbeat/exporter"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost/exporter"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/costmodel"
)

type KubecostEmitter struct {
	cloudCostProvider   models.Provider
	dataSource          *adapters.OpenCostDataSourceAdapter
	costModel           *costmodel.CostModel
	pipelineControllers *exporter.PipelineExportControllers
	heartbeatController ocexporter.ExportController
	diagController      ocexporter.ExportController
	diag                diagnostics.DiagnosticService

	config *EmitterConfig

	// newBucketStorage builds the export bucket store; tests replace it.
	newBucketStorage func([]byte) (storage.Storage, error)

	conditions *condition.Set

	// mu guards the fields below, which Init sets once it has started everything.
	mu       sync.Mutex
	stopped  bool
	activity *exportActivity
	canary   *bucketCanary
}

// WALGuard reports the state of the collector's write-ahead log, which keeps a restart from
// overwriting in-progress export windows with partial data (docs/reliability/FINDINGS.md F-20).
type WALGuard interface {
	Conditions() []condition.Condition
}

func NewKubecostEmitter(
	cloudCostProvider models.Provider,
	diag diagnostics.DiagnosticService,
	config *EmitterConfig,
) *KubecostEmitter {
	return &KubecostEmitter{
		cloudCostProvider: cloudCostProvider,
		diag:              diag,
		config:            config,
		newBucketStorage:  storage.NewBucketStorage,
		conditions:        condition.NewSet("kubecost"),
	}
}

// ID returns the kubecost emitter identifier.
func (ke *KubecostEmitter) ID() emitter.EmitterID {
	return emitter.KubecostEmitterID
}

// Init builds the adapters and cost model and starts the export controllers. Every failure
// returns before anything is started or assigned, so a failed Init can be retried; the exporter
// retries it each cycle and never calls it again after it succeeds.
//
// Init fails while the collector runs without its write-ahead log (F-41): exporting without it
// lets the next restart overwrite in-progress windows with partial data.
func (ke *KubecostEmitter) Init(snapshot *emitter.ClusterSnapshot) error {
	if ke.dataSource != nil {
		return errors.New("kubecost emitter is already initialised")
	}
	ke.mu.Lock()
	stopped := ke.stopped
	ke.mu.Unlock()
	if stopped {
		return errStopped
	}
	if ke.config.WAL != nil {
		for _, c := range ke.config.WAL.Conditions() {
			if c.Type == ConditionWALUnavailable {
				return fmt.Errorf("not starting Kubecost exports without the collector's write-ahead log (%s: %s)", c.Reason, c.Message)
			}
		}
	}

	// Setup exporters for kubecost pipelines
	bucketConfig, err := os.ReadFile(ke.config.BucketConfigFile)
	if err != nil {
		log.Errorf("Failed to initialize bucket output storage, please check your configuration and bucket security settings: %s", err)
		return fmt.Errorf("failed to read bucket config file: %w", err)
	}

	bucketStore, err := ke.newBucketStorage(bucketConfig)
	if err != nil {
		log.Errorf("Failed to create export bucket storage, please check your configuration and bucket security settings: %s", err)
		return fmt.Errorf("failed to create bucket storage: %w", err)
	}

	log.Infof("Successfully created bucket storage")

	clusterInfo := adapters.NewClusterInfoProviderAdapter(snapshot.ClusterInfo)
	clusterMap := adapters.NewClusterMapAdapter(snapshot.ClusterInfo)
	clusterCache := adapters.NewClusterCacheAdapter(snapshot.Kubernetes)
	metricsQuerier := adapters.NewMetricsQuerierAdapter(snapshot.Metrics)

	// create our updateable adapter that will drive the opencost exporters
	dataSource := adapters.NewOpenCostDataSourceAdapter(clusterInfo, clusterMap, clusterCache, metricsQuerier, ke.config.QueryResolution)

	costModel := costmodel.NewCostModel(ke.config.ClusterUID, dataSource, ke.cloudCostProvider, clusterCache, clusterMap, dataSource.BatchDuration())

	pipelineConfig := exporter.NewPipelinesExportConfig(ke.config.AppName, ke.config.ClusterUID, ke.config.ClusterName, ke.config.EmitLegacyDateModels, ke.config.EmitKubeModel)
	if ke.config.EmitAllocationMinuteResolution {
		pipelineConfig.AllocationPiplineResolutions = append(
			pipelineConfig.AllocationPiplineResolutions,
			10*time.Minute,
		)
	}
	if ke.config.EmitAssetMinuteResolution {
		pipelineConfig.AssetPipelineResolutons = append(
			pipelineConfig.AssetPipelineResolutons,
			10*time.Minute,
		)
	}
	if ke.config.EmitKubeModelMinuteResolution {
		pipelineConfig.KubeModelPipelineResolutions = append(
			pipelineConfig.KubeModelPipelineResolutions,
			10*time.Minute,
		)
	}
	pipelineConfig.Streaming = ke.config.StreamingExportEnabled
	pipelineConfig.Compression = ke.config.StreamingExportCompressionLevel
	if ke.config.StreamingExportEnabled {
		log.Infof("Streaming export enabled with compression level: %d", ke.config.StreamingExportCompressionLevel)
	}

	// Every export goes through exportStore, so Stop can wait for writes in flight, and every
	// computation through pinnedComputeSource, so it reads one snapshot (F-19).
	activity := &exportActivity{}
	exportBucket := &exportStore{Storage: bucketStore, activity: activity}
	computeSource := &pinnedComputeSource{ComputePipelineSource: costModel, dataSource: dataSource, activity: activity}

	// all pipeline export controllers
	pipelineControllers := exporter.NewPipelineExportControllers(exportBucket, computeSource, pipelineConfig)
	pipelineControllers.AllocationExportController.Start(ke.config.ExportIntervals.AllocationInterval)
	pipelineControllers.AssetExportController.Start(ke.config.ExportIntervals.AssetInterval)
	pipelineControllers.NetworkInsightExportController.Start(ke.config.ExportIntervals.NetworkInsightInterval)
	pipelineControllers.KubeModelExportController.Start(ke.config.ExportIntervals.KubeModelInterval)

	// agent presence and heartbeat
	heartbeatMetadata := heartbeatexporter.NewMultiMetadataProvider(
		heartbeatexporter.NewClusterInfoMetadataProvider(clusterInfo),
		heartbeatexporter.NewLogLevelMetadataProvider(),
	)
	agentHeartbeat := heartbeatexporter.NewHeartbeatExportController(ke.config.AppName, ke.config.ClusterName, version.FriendlyVersion(), exportBucket, heartbeatMetadata)
	if ke.config.HeartbeatExportEnabled {
		agentHeartbeat.Start(ke.config.ExportIntervals.HeartbeatInterval)
	}

	// diagnostics exporter
	diagnosticsExporter := diagexporter.NewDiagnosticsExportController(ke.config.AppName, ke.config.ClusterName, exportBucket, ke.diag)
	if ke.config.DiagnosticsExportEnabled {
		diagnosticsExporter.Start(ke.config.ExportIntervals.DiagnosticsInterval)
	}

	var canary *bucketCanary
	if ke.config.BucketCanaryInterval > 0 {
		canary = newBucketCanary(bucketStore, ke.config.ClusterName, ke.config.BucketCanaryInterval, ke.conditions)
		canary.start()
	}

	// initialize emitter's internal state
	ke.mu.Lock()
	stopped = ke.stopped
	if !stopped {
		ke.activity, ke.canary = activity, canary
		ke.dataSource = dataSource
		ke.costModel = costModel
		ke.pipelineControllers = pipelineControllers
		ke.heartbeatController = agentHeartbeat
		ke.diagController = diagnosticsExporter
	}
	ke.mu.Unlock()

	if stopped {
		// Stop ran while Init was building; stop what was just started.
		pipelineControllers.Stop()
		agentHeartbeat.Stop()
		diagnosticsExporter.Stop()
		if canary != nil {
			ctx, cancel := context.WithTimeout(context.Background(), canary.interval)
			_ = canary.stop(ctx)
			cancel()
		}
		return errStopped
	}
	return nil
}

func (ke *KubecostEmitter) Emit(ctx context.Context, snapshot *emitter.ClusterSnapshot) error {
	if ke.dataSource == nil {
		return errors.New("kubecost emitter is not initialised")
	}
	ke.dataSource.Update(snapshot)

	return nil
}

// Conditions returns the emitter's active degraded conditions, including the collector WAL's.
func (ke *KubecostEmitter) Conditions() []condition.Condition {
	conditions := ke.conditions.List()
	if ke.config.WAL != nil {
		conditions = append(conditions, ke.config.WAL.Conditions()...)
		slices.SortFunc(conditions, func(a, b condition.Condition) int { return strings.Compare(a.Type, b.Type) })
	}
	return conditions
}

// ExportStatus holds the Kubecost emitter's counters. They only ever increase.
type ExportStatus struct {
	// WritesTotal and WriteFailuresTotal count export writes to the bucket (all pipelines,
	// heartbeat and diagnostics).
	WritesTotal, WriteFailuresTotal uint64
	// WritesRejectedAfterStopTotal counts exports refused because the emitter had stopped.
	WritesRejectedAfterStopTotal uint64
	// CanaryRunsTotal and CanaryFailuresTotal count bucket canary probes; LastCanarySuccess is
	// zero until one succeeds.
	CanaryRunsTotal, CanaryFailuresTotal uint64
	LastCanarySuccess                    time.Time
	// ForcedSnapshotSwapsTotal counts snapshots published under a computation pinned for too
	// long (see adapters.OpenCostDataSourceAdapter.Pin).
	ForcedSnapshotSwapsTotal uint64
}

// Status returns the emitter's counters. It is zero before Init succeeds.
func (ke *KubecostEmitter) Status() ExportStatus {
	ke.mu.Lock()
	activity, canary, ds := ke.activity, ke.canary, ke.dataSource
	ke.mu.Unlock()

	var status ExportStatus
	if activity == nil {
		return status
	}
	status.WritesTotal = activity.writesTotal.Load()
	status.WriteFailuresTotal = activity.writeFailuresTotal.Load()
	status.WritesRejectedAfterStopTotal = activity.rejectedAfterStopTotal.Load()
	if canary != nil {
		status.CanaryRunsTotal = canary.runs.Load()
		status.CanaryFailuresTotal = canary.failures.Load()
		if ns := canary.lastSuccess.Load(); ns != 0 {
			status.LastCanarySuccess = time.Unix(0, ns)
		}
	}
	if ds != nil {
		status.ForcedSnapshotSwapsTotal = ds.ForcedSwapsTotal()
	}
	return status
}

// Stop stops every export controller and the bucket canary, refuses new computations, and waits
// until no computation, existence check or bucket write has been in flight for a short quiet
// period, or ctx ends. Only if ctx ends first are later writes refused (and counted). Call it after the exporter has stopped; it is safe to call more than once
// and before Init.
func (ke *KubecostEmitter) Stop(ctx context.Context) error {
	ke.mu.Lock()
	ke.stopped = true
	activity, canary := ke.activity, ke.canary
	ke.mu.Unlock()
	if activity == nil {
		return nil
	}

	ke.pipelineControllers.Stop()
	ke.heartbeatController.Stop()
	ke.diagController.Stop()
	var errs []error
	if canary != nil {
		errs = append(errs, canary.stop(ctx))
	}
	errs = append(errs, activity.drain(ctx))
	return errors.Join(errs...)
}
