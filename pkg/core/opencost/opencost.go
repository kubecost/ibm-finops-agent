package opencost

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/opencost/opencost/core/pkg/external"
	"github.com/opencost/opencost/core/pkg/kubeconfig"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/pkg/util/watcher"

	"github.com/ibm/finops-agent/pkg/cluster"
	"github.com/ibm/finops-agent/pkg/nodes"
	"github.com/julienschmidt/httprouter"
	"k8s.io/client-go/kubernetes"

	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util/retry"

	"github.com/opencost/opencost/pkg/config"
	"github.com/opencost/opencost/pkg/costmodel"

	"github.com/opencost/opencost/modules/collector-source/pkg/collector"
	"github.com/opencost/opencost/modules/prometheus-source/pkg/prom"

	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/cloud/provider"
)

// NewOpenCostDataSource builds the OpenCost data source and cloud provider and starts their
// background work. Cancelling ctx stops the data source retries. The config watchers and the
// CostModelMetricsEmitter it starts have no stop API and run for the life of the process (U-6).
func NewOpenCostDataSource(
	ctx context.Context,
	kubeClientset kubernetes.Interface,
	k8sCache cluster.ClusterCache,
	nodeClient nodes.StatSummaryClient,
	router *httprouter.Router,
	diag diagnostics.DiagnosticService,
	conf *OpenCostConfig,
) (source.OpenCostDataSource, models.Provider, error) {
	clusterUID, err := kubeconfig.GetClusterUID(kubeClientset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to determine cluster UID: %w", err)
	}

	// Create ConfigFileManager for synchronization of shared configuration
	confManager := config.NewConfigFileManager(nil)
	clusterCache := cluster.NewOpenCostClusterCacheAdapter(kubeClientset, k8sCache)

	cloudProvider, err := provider.NewProvider(clusterCache, conf.CloudProviderAPIKey, confManager)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create the cloud provider: %w", err)
	}

	err = cloudProvider.DownloadPricingData()
	if err != nil {
		log.Warnf("Failed to download public pricing data. Falling back to defaults: %s", err)
	}

	configWatchers := watcher.NewConfigMapWatchers(kubeClientset, conf.AgentNamespace)
	configWatchers.AddWatcher(provider.ConfigWatcherFor(cloudProvider))

	var labelProvider external.LabelProvider
	if conf.ExternalCfg != nil {
		labelProvider = external.NewNodeLabelProvider()
		labelSource, err := external.NewLabelSource(conf.ExternalCfg)
		if err != nil {
			log.Errorf("Failed to create an external label source: %s", err)
		}

		nodeLabelConfig := conf.ExternalCfg.NodeLabelConfig()
		nodeLabelNamespace := nodeLabelConfig.Namespace()
		// If configmap is in the same namespace as the finops agent we can just use the same configmap watcher.
		if nodeLabelNamespace == "" {
			configWatchers.Add(nodeLabelConfig.ConfigMapName(), external.WatchFunc(labelSource, labelProvider))
		} else {
			elWatchers := watcher.NewConfigMapWatchers(kubeClientset, nodeLabelNamespace)
			elWatchers.Add(nodeLabelConfig.ConfigMapName(), external.WatchFunc(labelSource, labelProvider))
			elWatchers.Watch()
		}
	}

	configWatchers.Watch()

	// ClusterInfo Provider to provide the cluster map with local and remote cluster data
	clusterInfoProvider := costmodel.NewLocalClusterInfoProvider(kubeClientset, cloudProvider)

	const maxRetries = 10
	const retryInterval = 10 * time.Second

	var fatalErr error

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	fn := func() (source.OpenCostDataSource, error) {
		ds, e := prom.NewDefaultPrometheusDataSource(clusterInfoProvider)
		if e != nil {
			if source.IsRetryable(e) {
				return nil, e
			}
			fatalErr = e
			cancel()
		}

		return ds, e
	}

	if conf.CollectorDataSourceEnabled {
		fn = func() (source.OpenCostDataSource, error) {
			var store storage.Storage
			if conf.BucketConfigFile != "" {
				bucketConfig, err := os.ReadFile(conf.BucketConfigFile)
				if err != nil {
					log.Errorf("Failed to initialize bucket output storage, please check your configuration and bucket security settings: %s", err)
				} else {
					store, err = storage.NewBucketStorage(bucketConfig)
					if err != nil {
						log.Errorf("Failed to create bucket storage, please check your configuration and bucket security settings: %s", err)
					}
				}
			}

			ds := collector.NewDefaultCollectorDataSource(
				clusterUID,
				store,
				clusterInfoProvider,
				clusterCache,
				nodeClient,
				labelProvider,
			)
			return ds, nil
		}
	}

	dataSource, err := retry.Retry(
		ctx,
		fn,
		maxRetries,
		retryInterval,
	)
	if fatalErr != nil {
		return nil, nil, fmt.Errorf("failed to create the OpenCost data source: %w", fatalErr)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create the OpenCost data source after %d attempts: %w", maxRetries, err)
	}
	if dataSource == nil {
		return nil, nil, errors.New("failed to create the OpenCost data source: no data source and no error")
	}

	dataSource.RegisterEndPoints(router)
	dataSource.RegisterDiagnostics(diag)

	clusterMap := dataSource.ClusterMap()

	costModel := costmodel.NewCostModel(clusterUID, dataSource, cloudProvider, clusterCache, clusterMap, dataSource.BatchDuration())
	metricsEmitter := costmodel.NewCostModelMetricsEmitter(clusterCache, cloudProvider, clusterInfoProvider, costModel)
	metricsEmitter.Start()

	return dataSource, cloudProvider, nil
}
