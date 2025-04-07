package kubecost

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ibm/finops-agent/kubecost/adapters"
	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/cloud/provider"
	"github.com/opencost/opencost/pkg/config"
	"github.com/opencost/opencost/pkg/costmodel"

	"github.com/opencost/opencost/pkg/exporter"
)

type KubecostEmitter struct {
	cloudProvider       models.Provider
	dataSource          *adapters.OpenCostDataSourceAdapter
	costModel           *costmodel.CostModel
	pipelineControllers *exporter.PipelineExportControllers

	config *EmitterConfig
}

func NewKubecostEmitter(config *EmitterConfig) *KubecostEmitter {
	return &KubecostEmitter{
		config: config,
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

	confManager := config.NewConfigFileManager(&config.ConfigFileManagerOpts{
		LocalConfigPath:   ke.config.ConfigPath,
		BucketStoreConfig: "",
	})

	cloudProvider, err := provider.NewProvider(clusterCache, ke.config.CloudProviderAPIKey, confManager)
	if err != nil {
		return fmt.Errorf("failed to initialize cloud provider: %w", err)
	}

	// create our updateable adapter that will drive the opencost exporters
	dataSource := adapters.NewOpenCostDataSourceAdapter(clusterInfo, clusterMap, clusterCache, metricsQuerier)

	// FIXME: We need a solution for watching kubernetes configmap for the cost provider used to drive the
	// FIXME: emitter. I don't believe we want to control these costs explicitly in the agent, but it needs
	// FIXME: further discussion. This watcher utility _may_ be useful for other teams, so we should maybe
	// FIXME: look to include it in the exporter implementation.
	/*
		configWatchers := watcher.NewConfigMapWatchers(kubeClientset, configNamespace, additionalConfigWatchers...)
		configWatchers.AddWatcher(provider.ConfigWatcherFor(cloudProvider))
		configWatchers.AddWatcher(metrics.GetMetricsConfigWatcher())
		configWatchers.Watch()
	*/

	// download the pricing data
	err = cloudProvider.DownloadPricingData()
	if err != nil {
		log.Warnf("Failed to download pricing data: %s", err)
	}

	costModel := costmodel.NewCostModel(dataSource, cloudProvider, clusterCache, clusterMap, dataSource.BatchDuration())

	// Setup exporters for kubecost pipelines
	bucketConfig, err := os.ReadFile(ke.config.BucketConfigFile)
	if err != nil {
		log.Errorf("Failed to initialize bucket output storage, please check your configuration and bucket security settings: %s", err)
		return fmt.Errorf("failed to read bucket config file: %w", err)
	}

	bucketStore, err := storage.NewBucketStorage(bucketConfig)
	if err != nil {
		log.Errorf("Failed to create federated storage, please check your configuration and bucket security settings: %s", err)
		return fmt.Errorf("failed to create bucket storage: %w", err)
	}

	log.Infof("Successfully created bucket storage")

	pipelineControllers := exporter.NewPipelineExportControllers(ke.config.ClusterID, bucketStore, costModel)
	pipelineControllers.Start(2 * time.Minute)

	// initialize emitter's internal state
	ke.cloudProvider = cloudProvider
	ke.dataSource = dataSource
	ke.costModel = costModel
	ke.pipelineControllers = pipelineControllers

	return nil
}

func (ke *KubecostEmitter) Emit(ctx context.Context, snapshot *emitter.ClusterSnapshot) error {
	ke.dataSource.Update(snapshot)

	return nil
}
