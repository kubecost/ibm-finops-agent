package kubecost

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ibm/finops-agent/kubecost/adapters"
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
	}
}

// ID returns the kubecost emitter identifier.
func (ke *KubecostEmitter) ID() emitter.EmitterID {
	return emitter.KubecostEmitterID
}

func (ke *KubecostEmitter) Init(snapshot *emitter.ClusterSnapshot) error {
	clusterInfo := adapters.NewClusterInfoProviderAdapter(snapshot.ClusterInfo)
	clusterMap := adapters.NewClusterMapAdapter(snapshot.ClusterInfo)
	clusterCache := adapters.NewClusterCacheAdapter(snapshot.Kubernetes)
	metricsQuerier := adapters.NewMetricsQuerierAdapter(snapshot.Metrics)

	// create our updateable adapter that will drive the opencost exporters
	dataSource := adapters.NewOpenCostDataSourceAdapter(clusterInfo, clusterMap, clusterCache, metricsQuerier, ke.config.QueryResolution)

	costModel := costmodel.NewCostModel(ke.config.ClusterUID, dataSource, ke.cloudCostProvider, clusterCache, clusterMap, dataSource.BatchDuration())

	// Setup exporters for kubecost pipelines
	bucketConfig, err := os.ReadFile(ke.config.BucketConfigFile)
	if err != nil {
		log.Errorf("Failed to initialize bucket output storage, please check your configuration and bucket security settings: %s", err)
		return fmt.Errorf("failed to read bucket config file: %w", err)
	}

	bucketStore, err := storage.NewBucketStorage(bucketConfig)
	if err != nil {
		log.Errorf("Failed to create export bucket storage, please check your configuration and bucket security settings: %s", err)
		return fmt.Errorf("failed to create bucket storage: %w", err)
	}

	log.Infof("Successfully created bucket storage")

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

	// all pipeline export controllers
	pipelineControllers := exporter.NewPipelineExportControllers(bucketStore, costModel, pipelineConfig)
	pipelineControllers.AllocationExportController.Start(ke.config.ExportIntervals.AllocationInterval)
	pipelineControllers.AssetExportController.Start(ke.config.ExportIntervals.AssetInterval)
	pipelineControllers.NetworkInsightExportController.Start(ke.config.ExportIntervals.NetworkInsightInterval)
	pipelineControllers.KubeModelExportController.Start(ke.config.ExportIntervals.KubeModelInterval)

	// agent presence and heartbeat
	heartbeatMetadata := heartbeatexporter.NewMultiMetadataProvider(
		heartbeatexporter.NewClusterInfoMetadataProvider(clusterInfo),
		heartbeatexporter.NewLogLevelMetadataProvider(),
	)
	agentHeartbeat := heartbeatexporter.NewHeartbeatExportController(ke.config.AppName, ke.config.ClusterName, version.FriendlyVersion(), bucketStore, heartbeatMetadata)
	if ke.config.HeartbeatExportEnabled {
		agentHeartbeat.Start(ke.config.ExportIntervals.HeartbeatInterval)
	}

	// diagnostics exporter
	diagnosticsExporter := diagexporter.NewDiagnosticsExportController(ke.config.AppName, ke.config.ClusterName, bucketStore, ke.diag)
	if ke.config.DiagnosticsExportEnabled {
		diagnosticsExporter.Start(ke.config.ExportIntervals.DiagnosticsInterval)
	}

	// initialize emitter's internal state
	ke.dataSource = dataSource
	ke.costModel = costModel
	ke.pipelineControllers = pipelineControllers
	ke.heartbeatController = agentHeartbeat
	ke.diagController = diagnosticsExporter

	return nil
}

func (ke *KubecostEmitter) Emit(ctx context.Context, snapshot *emitter.ClusterSnapshot) error {
	ke.dataSource.Update(snapshot)

	return nil
}
