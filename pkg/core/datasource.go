package core

import (
	"context"
	"time"

	"github.com/ibm/finops-agent/pkg/cluster"
	"github.com/ibm/finops-agent/pkg/core/opencost"
	"github.com/ibm/finops-agent/pkg/env"
	"github.com/ibm/finops-agent/pkg/nodes"
	"github.com/ibm/finops-agent/pkg/version"
	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/kubeconfig"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"k8s.io/client-go/discovery"

	"github.com/julienschmidt/httprouter"
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

	// K8s Version Object
	ClusterMetadata() version.Metadata
}

func NewAgentDataSource(
	router *httprouter.Router,
	diag diagnostics.DiagnosticService,
	interval time.Duration,
) DataSource {
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

	discClient, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		log.Warnf("Failed to create Kubernetes discovery client: %s", err.Error())
	}

	versionInfo, err := discClient.ServerVersion()
	if err != nil {
		log.Warnf("Failed to fetch Kubernetes version: %s", err.Error())
	}
	clusterMetadata := version.NewClusterMetadata(versionInfo)

	informerCfg := cluster.LoadInformerConfig()
	// Create Kubernetes Cluster Cache + Watchers
	k8sCache, err := cluster.NewDynamicClusterCache(cfg, informerCfg.ResyncInterval, informerCfg.SanitizeData, interval)
	if err != nil {
		log.Fatalf("Failed to build Kubernetes client: %s", err.Error())
	}

	k8sCache.Start(context.Background().Done())

	nodeClientConfig, err := nodes.NewNodeClientConfigFromEnv()
	if err != nil {
		log.Fatalf("error retrieving node client config: %s", err)
	}
	nodeStatsSummaryClient := nodes.NewNodeStatsSummaryClient(k8sCache, nodeClientConfig, cfg)

	var opencostSource source.OpenCostDataSource
	if env.IsOpenCostDataSourceEnabled() {
		opencostConf := opencost.NewOpenCostConfigFromEnv()
		opencostSource = opencost.NewOpenCostDataSource(kubeClientset, k8sCache, nodeStatsSummaryClient, router, diag, opencostConf)
	} else {
		// fulfill the contract with a no-op opencost datasource
		opencostSource = opencost.NewNoOpOpenCostDataSource()
	}

	// TODO: Initialization of any other data sources here

	return &agentDataSource{
		opencostSource:         opencostSource,
		metrics:                opencostSource.Metrics(),
		clusterCache:           k8sCache,
		nodeStatsSummaryClient: nodeStatsSummaryClient,
		clusterMetadata:        clusterMetadata,
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

	// K8s Cluster Metadata
	clusterMetadata version.Metadata

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

func (ads *agentDataSource) ClusterMetadata() version.Metadata {
	return ads.clusterMetadata
}
