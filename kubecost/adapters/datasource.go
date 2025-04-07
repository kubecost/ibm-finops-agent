package adapters

import (
	"time"

	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/source"
)

type OpenCostDataSourceAdapter struct {
	infoAdapter    *ClusterInfoProviderAdapter
	mapAdapter     *ClusterMapAdapter
	clusterAdapter *ClusterCacheAdapter
	metricsAdapter *MetricsQuerierAdapter
	resolution     time.Duration
}

func NewOpenCostDataSourceAdapter(
	infoAdapter *ClusterInfoProviderAdapter,
	mapAdapter *ClusterMapAdapter,
	clusterAdapter *ClusterCacheAdapter,
	metricsAdapter *MetricsQuerierAdapter,
	resolution time.Duration,
) *OpenCostDataSourceAdapter {
	return &OpenCostDataSourceAdapter{
		infoAdapter:    infoAdapter,
		mapAdapter:     mapAdapter,
		clusterAdapter: clusterAdapter,
		metricsAdapter: metricsAdapter,
		resolution:     resolution,
	}
}

// Update emits the internal opencost source structures with the latest snapshot data
func (ocdsa *OpenCostDataSourceAdapter) Update(snapshot *emitter.ClusterSnapshot) {
	ocdsa.infoAdapter.Update(snapshot.ClusterInfo)
	ocdsa.mapAdapter.Update(snapshot.ClusterInfo)
	ocdsa.clusterAdapter.Update(snapshot.Kubernetes)
	ocdsa.metricsAdapter.Update(snapshot.Metrics)
}

// RegisterEndPoints registers any custom endpoints that can be used for diagnostics or debug purposes.
func (ocdsa *OpenCostDataSourceAdapter) RegisterEndPoints(router *httprouter.Router) {
	// TODO: What specific opencost endpoints should we expose? Debug, diagnostics, etc...
}

// Metrics returns a MetricsQuerier that can be used to query historical metrics data from the data source.
func (ocdsa *OpenCostDataSourceAdapter) Metrics() source.MetricsQuerier {
	return ocdsa.metricsAdapter
}

// ClusterMap returns a mapping of cluster identifier to ClusterInfo for all known clusters (local only for
// single cluster deployments).
func (ocdsa *OpenCostDataSourceAdapter) ClusterMap() clusters.ClusterMap {
	return ocdsa.mapAdapter
}

// ClusterInfo returns the ClusterInfoProvider for the local cluster.
func (ocdsa *OpenCostDataSourceAdapter) ClusterInfo() clusters.ClusterInfoProvider {
	return ocdsa.infoAdapter
}

func (ocdsa *OpenCostDataSourceAdapter) BatchDuration() time.Duration {
	return 730 * time.Hour // monthly batch to encompass all reasonable query lengths
}

func (ocdsa *OpenCostDataSourceAdapter) Resolution() time.Duration {
	return ocdsa.resolution
}
