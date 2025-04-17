package core

import (
	"context"
	"log"
	"time"

	"github.com/ibm/finops-agent/pkg/cluster"
	"github.com/ibm/finops-agent/pkg/core/opencost"
	"github.com/ibm/finops-agent/pkg/nodes"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/pkg/kubeconfig"
	"k8s.io/client-go/rest"
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
	StatsSummary() nodes.StatSummaryClient
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

	opencostConf := opencost.NewOpenCostConfigFromEnv()
	opencostSource := opencost.NewOpenCostDataSource(kubeClientset, k8sCache, opencostConf)

	inClusterConfig, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("error retrieving in cluster config: %s", err)
	}
	nodeStatsSummaryClient := nodes.NewNodeStatsSummaryClient(k8sCache, nodes.NewNodeClientConfig("", false, 10, false), inClusterConfig)

	// TODO: Initialization of any other data sources here

	return &agentDataSource{
		opencostSource: opencostSource,
		metrics:        opencostSource.Metrics(),
		clusterCache:   k8sCache,
		nodeStatsSummaryClient: nodeStatsSummaryClient,
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
	nodeStatsSummaryClient nodes.StatSummaryClient
	
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

func (ads *agentDataSource) StatsSummary() nodes.StatSummaryClient {
	return ads.nodeStatsSummaryClient
}
