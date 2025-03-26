package core

import (
	"context"
	"log"
	"time"

	"github.com/ibm/finops-agent/pkg/cluster"
	"github.com/ibm/finops-agent/pkg/core/opencost"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/pkg/kubeconfig"
)

// NOTE: We can use this as an intermediate data source local to this project. We can defer pushing all the implementation down
// NOTE: into OpenCost until we have a better understanding of the reusability of the opencost code, and/or what it's lacking.
type DataSource interface {
	// Opencost Metrics Query API
	Metrics() source.MetricsQuerier

	// Kubernetes Cluster Informers
	Cluster() cluster.ClusterCache
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

	opencostSource := opencost.NewOpenCostDataSource(kubeClientset, k8sCache)

	// TODO: Initialization of any other data sources here

	return &agentDataSource{
		metrics:      opencostSource.Metrics(),
		clusterCache: k8sCache,
	}
}

type agentDataSource struct {
	// OpenCost Metrics Query API
	metrics source.MetricsQuerier

	// Kubernetes Cluster Informers
	clusterCache cluster.ClusterCache

	// TODO: Node Stats Summary Client
	// TODO: HTTP Server/Proxy for Turbo?
}

func (ads *agentDataSource) Metrics() source.MetricsQuerier {
	return ads.metrics
}

func (ads *agentDataSource) Cluster() cluster.ClusterCache {
	return ads.clusterCache
}
