package adapters

import (
	"time"

	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/source"
)

// OpenCostDataSourceAdapter serves exporter snapshots to OpenCost's cost model. Its four
// adapters share one stateHolder, so Update replaces all of them as a unit, and a computation
// that holds Pin reads a single snapshot from all four for its whole duration (F-19).
type OpenCostDataSourceAdapter struct {
	src            *stateHolder
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
	// Rebind the adapters to one holder seeded with their current data.
	src := newStateHolder(&adapterState{
		info:       infoAdapter.src.load().info,
		kubernetes: clusterAdapter.src.load().kubernetes,
		metrics:    metricsAdapter.src.load().metrics,
	})
	infoAdapter.src, mapAdapter.src, clusterAdapter.src, metricsAdapter.src = src, src, src, src

	return &OpenCostDataSourceAdapter{
		src:            src,
		infoAdapter:    infoAdapter,
		mapAdapter:     mapAdapter,
		clusterAdapter: clusterAdapter,
		metricsAdapter: metricsAdapter,
		resolution:     resolution,
	}
}

// Update publishes the latest snapshot to all four adapters as one unit. It never blocks: while
// a computation is pinned, the snapshot is held back until the last pin is released.
func (ocdsa *OpenCostDataSourceAdapter) Update(snapshot *emitter.ClusterSnapshot) {
	ocdsa.src.update(func(s *adapterState) *adapterState {
		return &adapterState{
			info:       snapshot.ClusterInfo,
			kubernetes: snapshot.Kubernetes,
			metrics:    s.metrics.with(snapshot.Metrics),
		}
	})
}

// Pin holds the adapters on their current snapshot until release is called, so a computation
// that reads several adapters, or runs several query groups, sees one consistent snapshot.
// Pins nest and are cheap; release is idempotent.
func (ocdsa *OpenCostDataSourceAdapter) Pin() (release func()) {
	return ocdsa.src.pin()
}

// ForcedSwapsTotal counts snapshots published while a computation was still pinned because it
// had been pinned for longer than maxSwapDeferral.
func (ocdsa *OpenCostDataSourceAdapter) ForcedSwapsTotal() uint64 {
	return ocdsa.src.forcedSwaps.Load()
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
