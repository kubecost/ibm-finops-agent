package core

import (
	"github.com/ibm/finops-agent/pkg/cluster"
	"github.com/ibm/finops-agent/pkg/core/opencost"
	"github.com/opencost/opencost/core/pkg/source"
)

// NOTE: We can use this as an intermediate data source local to this project. We can defer pushing all the implementation down
// NOTE: into OpenCost until we have a better understanding of the reusability of the opencost code, and/or what it's lacking.
type DataSource interface {
	// Opencost Metrics Query API
	Metrics() source.MetricsQuerier

	// Kubernetes Cluster Informers
	Cluster() cluster.ClusterCache
}

func NewAgentDataSource() DataSource {
	// FIXME: returning the clusterCache here is temporary - need to extract k8s client instantiation from opencost
	// FIXME: to provide proxy/auth customization for cloudy/turbo
	opencostSource, clusterCache := opencost.NewOpenCostDataSource()

	// TODO: Initialization of any other data sources here

	return &agentDataSource{
		metrics:      opencostSource.Metrics(),
		clusterCache: clusterCache,
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
