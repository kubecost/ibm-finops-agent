package opencost

import (
	"context"
	"os"
	"time"

	kcenv "github.com/ibm/finops-agent/kubecost/env"
	"github.com/opencost/opencost/core/pkg/externallabels"
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

func NewOpenCostDataSource(
	kubeClientset kubernetes.Interface,
	k8sCache cluster.ClusterCache,
	nodeClient nodes.StatSummaryClient,
	router *httprouter.Router,
	diag diagnostics.DiagnosticService,
	conf *OpenCostConfig,
) (source.OpenCostDataSource, models.Provider) {
	clusterUID, err := kubeconfig.GetClusterUID(kubeClientset)
	if err != nil {
		log.Fatalf("Failed to determine cluster UID: %s", err)
	}

	// Create ConfigFileManager for synchronization of shared configuration
	confManager := config.NewConfigFileManager(nil)
	clusterCache := cluster.NewOpenCostClusterCacheAdapter(kubeClientset, k8sCache)

	cloudProvider, err := provider.NewProvider(clusterCache, conf.CloudProviderAPIKey, confManager)
	if err != nil {
		panic(err.Error())
	}

	err = cloudProvider.DownloadPricingData()
	if err != nil {
		log.Warnf("Failed to download public pricing data. Falling back to defaults: %s", err)
	}

	configWatchers := watcher.NewConfigMapWatchers(kubeClientset, kcenv.GetFinOpsAgentNamespace())
	configWatchers.AddWatcher(provider.ConfigWatcherFor(cloudProvider))

	var elProvider externallabels.Provider
	if conf.ExternalLabelsCfg != nil {
		elProvider = externallabels.NewConfigMapProvider()
		elNamespace := conf.ExternalLabelsCfg.Namespace
		// If configmap is in the same namespace as the finops agent we can just use the same configmap watcher.
		if elNamespace == "" {
			elNamespace = kcenv.GetFinOpsAgentNamespace()
			configWatchers.Add(conf.ExternalLabelsCfg.ConfigMapName, externallabels.ParseFunc(conf.ExternalLabelsCfg, elProvider))
		} else {
			elWatchers := watcher.NewConfigMapWatchers(kubeClientset, elNamespace)
			elWatchers.Add(conf.ExternalLabelsCfg.ConfigMapName, externallabels.ParseFunc(conf.ExternalLabelsCfg, elProvider))
			elWatchers.Watch()
		}

	}

	configWatchers.Watch()

	// ClusterInfo Provider to provide the cluster map with local and remote cluster data
	clusterInfoProvider := costmodel.NewLocalClusterInfoProvider(kubeClientset, cloudProvider)

	const maxRetries = 10
	const retryInterval = 10 * time.Second

	var fatalErr error

	ctx, cancel := context.WithCancel(context.Background())
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
				elProvider,
			)
			return ds, nil
		}
	}

	dataSource, _ := retry.Retry(
		ctx,
		fn,
		maxRetries,
		retryInterval,
	)

	if fatalErr != nil {
		log.Fatalf("Failed to create opencost data source: %s", fatalErr)
		panic(fatalErr)
	}

	dataSource.RegisterEndPoints(router)
	dataSource.RegisterDiagnostics(diag)

	clusterMap := dataSource.ClusterMap()

	costModel := costmodel.NewCostModel(clusterUID, dataSource, cloudProvider, clusterCache, clusterMap, dataSource.BatchDuration())
	metricsEmitter := costmodel.NewCostModelMetricsEmitter(clusterCache, cloudProvider, clusterInfoProvider, costModel)
	metricsEmitter.Start()

	return dataSource, cloudProvider
}
