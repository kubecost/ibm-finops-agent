package opencost

import (
	"context"
	"time"

	"github.com/ibm/finops-agent/pkg/cluster"
	"k8s.io/client-go/kubernetes"

	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util/retry"

	"github.com/opencost/opencost/pkg/costmodel"

	"github.com/opencost/opencost/modules/prometheus-source/pkg/prom"

	"github.com/opencost/opencost/pkg/cloud/provider"
	"github.com/opencost/opencost/pkg/config"
	"github.com/opencost/opencost/pkg/env"
)

func NewOpenCostDataSource(kubeClientset kubernetes.Interface, k8sCache cluster.ClusterCache) source.OpenCostDataSource {
	// Create ConfigFileManager for synchronization of shared configuration
	confManager := config.NewConfigFileManager(&config.ConfigFileManagerOpts{
		BucketStoreConfig: env.GetKubecostConfigBucket(),
		LocalConfigPath:   "/",
	})

	//configPrefix := env.GetConfigPathWithDefault("/var/configs/")

	cloudProviderKey := env.GetCloudProviderAPIKey()
	cloudProvider, err := provider.NewProvider(cluster.NewOpenCostClusterCacheAdapter(k8sCache), cloudProviderKey, confManager)
	if err != nil {
		panic(err.Error())
	}

	// ClusterInfo Provider to provide the cluster map with local and remote cluster data
	clusterInfoProvider := costmodel.NewLocalClusterInfoProvider(kubeClientset, cloudProvider)

	// var nssg NodeStatsSummaryGetter = newGetter()

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

	return dataSource
}
