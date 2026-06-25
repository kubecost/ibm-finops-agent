package opencost

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	agentenv "github.com/ibm/finops-agent/pkg/env"
	kcenv "github.com/ibm/finops-agent/kubecost/env"
	"github.com/opencost/opencost/core/pkg/kubeconfig"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/pkg/util/watcher"
	"sigs.k8s.io/yaml"

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

	// If an external labels ConfigMap is configured, watch it and log its labels on every change.
	if cmName := agentenv.GetExternalLabelsConfigMapName(); cmName != "" {
		cmNamespace := agentenv.GetExternalLabelsConfigMapNamespace()
		cmPath := agentenv.GetExternalLabelsConfigMapPath()

		// The watcher monitors ConfigMaps in the agent namespace; for cross-namespace ConfigMaps
		// we use a separate watcher scoped to the target namespace.
		extWatchers := watcher.NewConfigMapWatchers(kubeClientset, cmNamespace)
		extWatchers.Add(cmName, func(_ string, data map[string]string) error {
			labels, err := extractLabelsFromConfigMap(data, cmPath)
			if err != nil {
				log.Warnf("ExternalLabels: failed to extract labels from ConfigMap %s/%s at path %q: %s", cmNamespace, cmName, cmPath, err)
				return nil
			}
			log.Infof("ExternalLabels: loaded %d label(s) from ConfigMap %s/%s (path: %q)", len(labels), cmNamespace, cmName, cmPath)
			for k, v := range labels {
				log.Infof("ExternalLabels:   %s = %s", k, v)
			}
			return nil
		})
		extWatchers.Watch()
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

// extractLabelsFromConfigMap reads the "config.yaml" key from ConfigMap data, unmarshals it,
// and walks the dot-separated path to return the labels map. If path is empty the entire
// config.yaml content is expected to be a flat map[string]string.
func extractLabelsFromConfigMap(data map[string]string, path string) (map[string]string, error) {
	raw, ok := data["config.yaml"]
	if !ok {
		return nil, fmt.Errorf("key \"config.yaml\" not found in ConfigMap data")
	}

	// Unmarshal the YAML blob into a generic map.
	var root map[string]interface{}
	if err := yaml.Unmarshal([]byte(raw), &root); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config.yaml: %w", err)
	}

	// Walk the dot-separated path to find the labels node.
	var node interface{} = root
	if path != "" {
		for _, segment := range strings.Split(path, ".") {
			m, ok := node.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("path segment %q: parent is not a map", segment)
			}
			node, ok = m[segment]
			if !ok {
				return nil, fmt.Errorf("path segment %q not found", segment)
			}
		}
	}

	// The final node must be a map whose values are all strings.
	rawMap, ok := node.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("value at path %q is not a map", path)
	}

	labels := make(map[string]string, len(rawMap))
	for k, v := range rawMap {
		labels[k] = fmt.Sprintf("%v", v)
	}
	return labels, nil
}
