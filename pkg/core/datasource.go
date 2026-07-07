package core

import (
	"context"
	"time"

	"github.com/ibm/finops-agent/pkg/cluster"
	"github.com/ibm/finops-agent/pkg/core/opencost"
	"github.com/ibm/finops-agent/pkg/env"
	"github.com/ibm/finops-agent/pkg/nodes"
	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/pkg/cloud/models"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/julienschmidt/httprouter"
)

// NOTE: We can use this as an intermediate data source local to this project. We can defer pushing all the implementation down
// NOTE: into OpenCost until we have a better understanding of the reusability of the opencost code, and/or what it's lacking.
type DataSource interface {
	// OpenCost Data Source
	OpenCostSource() source.OpenCostDataSource

	// OpenCost implementation of the cloud provider's public pricing.
	OpenCostCloudCostProvider() models.Provider

	// Opencost Metrics Query API
	Metrics() source.MetricsQuerier

	// Kubernetes Cluster Informers
	Cluster() cluster.ClusterCache

	// Node Stats Summary Client
	StatsSummary() nodes.StatSummaryClient

	// K8s Version Object
	ClusterMetadata() cluster.Metadata

	// NodeStatsProvider returns the background NodeStatsSummaryProvider if background collection is enabled, nil otherwise.
	NodeStatsProvider() *nodes.NodeStatsSummaryProvider
}

func NewAgentDataSource(
	kubeConfig *rest.Config,
	kubeClientset kubernetes.Interface,
	router *httprouter.Router,
	diag diagnostics.DiagnosticService,
	interval time.Duration,
) DataSource {
	discClient, err := discovery.NewDiscoveryClientForConfig(kubeConfig)
	if err != nil {
		log.Warnf("Failed to create Kubernetes discovery client: %s", err.Error())
	}

	versionInfo, err := discClient.ServerVersion()
	if err != nil {
		log.Warnf("Failed to fetch Kubernetes version: %s", err.Error())
	}
	clusterMetadata := cluster.NewClusterMetadata(versionInfo)

	informerCfg := cluster.LoadInformerConfig()
	// Create Kubernetes Cluster Cache + Watchers
	k8sCache, err := cluster.NewDynamicClusterCache(kubeConfig, informerCfg.ResyncInterval, informerCfg.SanitizeData, interval)
	if err != nil {
		log.Fatalf("Failed to build Kubernetes client: %s", err.Error())
	}

	k8sCache.Start(context.Background().Done())

	nodeClientConfig, err := nodes.NewNodeClientConfigFromEnv()
	if err != nil {
		log.Fatalf("error retrieving node client config: %s", err)
	}
	nodeStatsSummaryClient := nodes.NewNodeStatsSummaryClient(k8sCache, nodeClientConfig, kubeConfig)

	// If we use a background service, we leverage the client to refresh node data on an interval
	// otherwise, we retrieve node data _at_ snapshot time
	var nodesProvider *nodes.NodeStatsSummaryProvider
	var nodeStatsProvider nodes.StatSummaryClient
	if nodeClientConfig.BackgroundNodeCollection {
		nodesProvider = nodes.NewNodeStatsSummaryProvider(nodeStatsSummaryClient)
		nodesProvider.Start(nodeClientConfig.RefreshInterval)

		nodeStatsProvider = nodesProvider
	} else {
		nodeStatsProvider = nodeStatsSummaryClient
	}

	var opencostCloudCostProvider models.Provider
	var opencostSource source.OpenCostDataSource
	if env.IsOpenCostDataSourceEnabled() {
		opencostConf := opencost.NewOpenCostConfigFromEnv()
		opencostSource, opencostCloudCostProvider = opencost.NewOpenCostDataSource(kubeClientset, k8sCache, nodeStatsSummaryClient, router, diag, opencostConf)
	} else {
		// fulfill the contract with a no-op opencost datasource
		opencostSource = opencost.NewNoOpOpenCostDataSource()
		opencostCloudCostProvider = nil
	}

	// TODO: Initialization of any other data sources here

	return &agentDataSource{
		opencostSource:            opencostSource,
		opencostCloudCostProvider: opencostCloudCostProvider,
		metrics:                   opencostSource.Metrics(),
		clusterCache:              k8sCache,
		nodeStatsSummaryClient:    nodeStatsProvider,
		nodeStatsProvider:         nodesProvider,
		clusterMetadata:           clusterMetadata,
	}
}

type agentDataSource struct {
	// opencost data source
	opencostSource source.OpenCostDataSource

	// opencost public pricing data for cloud
	opencostCloudCostProvider models.Provider

	// OpenCost Metrics Query API
	metrics source.MetricsQuerier

	// Kubernetes Cluster Informers
	clusterCache cluster.ClusterCache

	// Node Stats Summary Client
	nodeStatsSummaryClient nodes.StatSummaryClient

	// Background node stats provider (nil if background collection is disabled)
	nodeStatsProvider *nodes.NodeStatsSummaryProvider

	// Cluster Metadata
	clusterMetadata cluster.Metadata

	// TODO: HTTP Server/Proxy for Turbo?
}

func (ads *agentDataSource) OpenCostSource() source.OpenCostDataSource {
	return ads.opencostSource
}

func (ads *agentDataSource) OpenCostCloudCostProvider() models.Provider {
	return ads.opencostCloudCostProvider
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

func (ads *agentDataSource) ClusterMetadata() cluster.Metadata {
	return ads.clusterMetadata
}

func (ads *agentDataSource) NodeStatsProvider() *nodes.NodeStatsSummaryProvider {
	return ads.nodeStatsProvider
}
