package core

import (
	"context"
	"log"
	"time"

	"github.com/ibm/finops-agent/pkg/cluster"
	"github.com/ibm/finops-agent/pkg/core/opencost"
	"github.com/ibm/finops-agent/pkg/env"
	"github.com/ibm/finops-agent/pkg/nodes"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/pkg/kubeconfig"
)

// NOTE: We can use this as an intermediate data source local to this project. We can defer pushing all the implementation down
// NOTE: into OpenCost until we have a better understanding of the reusability of the opencost code, and/or what it's lacking.
type DataSource interface {
	// OpenCost Data Source
	OpenCostSource() source.OpenCostDataSource

	// Opencost Metrics Query API
	Metrics() source.MetricsQuerier

	// Kubernetes Cluster Informers
	Cluster() cluster.ClusterCache

	// Node Stats Summary Client
	StatsSummary() nodes.NodeClient
}

var (
	defaultCacheResyncDuration = 60 * time.Minute
)

func NewAgentDataSource() DataSource {
	// NOTE: (bolt) This just uses a fairly straight-forward kube client initialization. We should add specific proxy/auth
	// NOTE: (bolt) requirements for the other data sources.
	kubeClientset, err := kubeconfig.LoadKubeClient("")
	if err != nil {
		log.Fatalf("Failed to build Kubernetes client: %s", err.Error())
	}

	cfg, err := kubeconfig.LoadKubeconfig("")
	if err != nil {
		log.Fatalf("Failed to load Kubernetes config: %s", err.Error())
	}
	// Create Kubernetes Cluster Cache + Watchers
	k8sCache, err := cluster.NewDynamicClusterCache(cfg, defaultCacheResyncDuration)
	if err != nil {
		log.Fatalf("Failed to build Kubernetes client: %s", err.Error())
	}

	k8sCache.Start(context.Background().Done())

	var opencostSource source.OpenCostDataSource
	if env.IsOpenCostDataSourceEnabled() {
		opencostConf := opencost.NewOpenCostConfigFromEnv()
		opencostSource = opencost.NewOpenCostDataSource(kubeClientset, k8sCache, opencostConf)
	} else {
		// fulfill the contract with a no-op opencost datasource
		opencostSource = opencost.NewNoOpOpenCostDataSource()
	}

	// Alex TODO: Return the config from an env file
	config := nodes.NewNodeClientConfig("", false, 10, false)
	nodeStatsSummaryClient := nodes.NewNodeStatsSummaryClient(k8sCache, config)

	// TODO: Initialization of any other data sources here

	return &agentDataSource{
		nodeStatsSummaryClient: nodeStatsSummaryClient,
		opencostSource:         opencostSource,
		metrics:                opencostSource.Metrics(),
		clusterCache:           k8sCache,
	}
}

type agentDataSource struct {
	// opencost data source
	opencostSource source.OpenCostDataSource

	// OpenCost Metrics Query API
	metrics source.MetricsQuerier

	// Kubernetes Cluster Informers
	clusterCache cluster.ClusterCache

	// Node Stats Summary Client
	nodeStatsSummaryClient nodes.NodeClient

	// TODO: HTTP Server/Proxy for Turbo?
}

func (ads *agentDataSource) OpenCostSource() source.OpenCostDataSource {
	return ads.opencostSource
}

func (ads *agentDataSource) Metrics() source.MetricsQuerier {
	return ads.metrics
}

func (ads *agentDataSource) Cluster() cluster.ClusterCache {
	return ads.clusterCache
}

func (ads *agentDataSource) StatsSummary() nodes.NodeClient {
	return ads.nodeStatsSummaryClient
}
