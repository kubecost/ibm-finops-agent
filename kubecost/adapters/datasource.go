package adapters

import (
	"sync/atomic"
	"time"

	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/source"
)

// adapterState groups all adapter components so they can be swapped atomically.
// This ensures readers always get a fully consistent set of adapters — either all old or all new, never mixed.
type adapterState struct {
	info    *ClusterInfoProviderAdapter
	mapAdpt *ClusterMapAdapter
	cluster *ClusterCacheAdapter
	metrics *MetricsQuerierAdapter
}

type OpenCostDataSourceAdapter struct {
	state      atomic.Pointer[adapterState]
	resolution time.Duration
}

func NewOpenCostDataSourceAdapter(
	infoAdapter *ClusterInfoProviderAdapter,
	mapAdapter *ClusterMapAdapter,
	clusterAdapter *ClusterCacheAdapter,
	metricsAdapter *MetricsQuerierAdapter,
	resolution time.Duration,
) *OpenCostDataSourceAdapter {
	adapter := &OpenCostDataSourceAdapter{
		resolution: resolution,
	}
	
	// Initialize with the provided adapters
	initial := &adapterState{
		info:    infoAdapter,
		mapAdpt: mapAdapter,
		cluster: clusterAdapter,
		metrics: metricsAdapter,
	}
	adapter.state.Store(initial)
	
	return adapter
}

// Update emits the internal opencost source structures with the latest snapshot data.
// This method builds a complete new state and swaps it in atomically, ensuring readers
// always see a consistent set of adapters (either all old or all new, never mixed).
func (ocdsa *OpenCostDataSourceAdapter) Update(snapshot *emitter.ClusterSnapshot) {
	// Load current state to get the existing adapters
	current := ocdsa.state.Load()
	
	// Update each adapter (these modify internal state via their own locks)
	current.info.Update(snapshot.ClusterInfo)
	current.mapAdpt.Update(snapshot.ClusterInfo)
	current.cluster.Update(snapshot.Kubernetes)
	current.metrics.Update(snapshot.Metrics)
	
	// Build a new state object with the updated adapters and swap it in.
	// After this single Store() call, all readers see the new state atomically.
	newState := &adapterState{
		info:    current.info,
		mapAdpt: current.mapAdpt,
		cluster: current.cluster,
		metrics: current.metrics,
	}
	ocdsa.state.Store(newState)
}

// RegisterEndPoints registers any custom endpoints that can be used for diagnostics or debug purposes.
func (ocdsa *OpenCostDataSourceAdapter) RegisterEndPoints(router *httprouter.Router) {
	// TODO: What specific opencost endpoints should we expose? Debug, diagnostics, etc...
}

// RegisterDiagnostics registers any custom diagnostics that can be used for monitoring the data source.
func (ocds *OpenCostDataSourceAdapter) RegisterDiagnostics(diag diagnostics.DiagnosticService) {

}

// Metrics returns a MetricsQuerier that can be used to query historical metrics data from the data source.
func (ocdsa *OpenCostDataSourceAdapter) Metrics() source.MetricsQuerier {
	return ocdsa.state.Load().metrics
}

// ClusterMap returns a mapping of cluster identifier to ClusterInfo for all known clusters (local only for
// single cluster deployments).
func (ocdsa *OpenCostDataSourceAdapter) ClusterMap() clusters.ClusterMap {
	return ocdsa.state.Load().mapAdpt
}

// ClusterInfo returns the ClusterInfoProvider for the local cluster.
func (ocdsa *OpenCostDataSourceAdapter) ClusterInfo() clusters.ClusterInfoProvider {
	return ocdsa.state.Load().info
}

// ClusterCache returns the ClusterCache for accessing Kubernetes resource snapshots.
func (ocdsa *OpenCostDataSourceAdapter) ClusterCache() *ClusterCacheAdapter {
	return ocdsa.state.Load().cluster
}

func (ocdsa *OpenCostDataSourceAdapter) BatchDuration() time.Duration {
	return 730 * time.Hour // monthly batch to encompass all reasonable query lengths
}

func (ocdsa *OpenCostDataSourceAdapter) Resolution() time.Duration {
	return ocdsa.resolution
}
