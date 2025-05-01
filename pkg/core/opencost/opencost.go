package opencost

import (
	"context"
	"time"

	"github.com/ibm/finops-agent/pkg/cluster"
	"k8s.io/client-go/kubernetes"

	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util/retry"

	"github.com/opencost/opencost/pkg/config"
	"github.com/opencost/opencost/pkg/costmodel"

	"github.com/opencost/opencost/modules/prometheus-source/pkg/prom"

	"github.com/opencost/opencost/pkg/cloud/provider"
)

func NewOpenCostDataSource(kubeClientset kubernetes.Interface, k8sCache cluster.ClusterCache, conf *OpenCostConfig) source.OpenCostDataSource {
	// Create ConfigFileManager for synchronization of shared configuration
	confManager := config.NewConfigFileManager(&config.ConfigFileManagerOpts{
		LocalConfigPath:   "/",
		BucketStoreConfig: "",
	})

	clusterCache := cluster.NewOpenCostClusterCacheAdapter(k8sCache)

	// NOTE: this cloud provider is purely an implementation used to provide cluster info (it does not actively pull pricing data).
	cloudProvider, err := provider.NewProvider(clusterCache, conf.CloudProviderAPIKey, confManager)
	if err != nil {
		panic(err.Error())
	}

	// ClusterInfo Provider to provide the cluster map with local and remote cluster data
	clusterInfoProvider := costmodel.NewLocalClusterInfoProvider(kubeClientset, cloudProvider)

	const maxRetries = 10
	const retryInterval = 10 * time.Second

	var fatalErr error

	ctx, cancel := context.WithCancel(context.Background())
	dataSource, _ := retry.Retry(
		ctx,
		func() (source.OpenCostDataSource, error) {
			ds, e := prom.NewDefaultPrometheusDataSource(clusterInfoProvider)
			if e != nil {
				if source.IsRetryable(e) {
					return nil, e
				}
				fatalErr = e
				cancel()
			}

			return ds, e
		},
		maxRetries,
		retryInterval,
	)

	if fatalErr != nil {
		//log.Fatalf("Failed to create Prometheus data source: %s", fatalErr)
		//panic(fatalErr)
	}

	clusterMap := dataSource.ClusterMap()

	costModel := costmodel.NewCostModel(dataSource, cloudProvider, clusterCache, clusterMap, dataSource.BatchDuration())
	metricsEmitter := costmodel.NewCostModelMetricsEmitter(clusterCache, cloudProvider, clusterInfoProvider, costModel)
	metricsEmitter.Start()

	return dataSource
}
