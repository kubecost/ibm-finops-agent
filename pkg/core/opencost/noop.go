package opencost

import (
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/source"
)

//--------------------------------------------------------------------------
//  No-Op OpenCostDataSource
//--------------------------------------------------------------------------

// NoOpOpenCostDataSource
type NoOpOpenCostDataSource struct {
	MetricsQuerier      *source.NoOpMetricsQuerier
	ClusterInfoProvider clusters.ClusterInfoProvider
}

func NewNoOpOpenCostDataSource() *NoOpOpenCostDataSource {
	return &NoOpOpenCostDataSource{
		MetricsQuerier: source.NewNoOpMetricsQuerier(),
		ClusterInfoProvider: clusters.NewMockClusterInfoProvider(
			map[string]string{
				clusters.ClusterInfoIdKey: "mock-cluster-id",
			},
		),
	}
}

// RegisterEndPoints registers any custom endpoints that can be used for diagnostics or debug purposes.
func (mocds *NoOpOpenCostDataSource) RegisterEndPoints(router *httprouter.Router) {
	// No-op
}

func (mocds *NoOpOpenCostDataSource) RegisterDiagnostics(diagService diagnostics.DiagnosticService) {
	// No-op
}

// Metrics returns a MetricsQuerier that can be used to query historical metrics data from the data source.
func (mocds *NoOpOpenCostDataSource) Metrics() source.MetricsQuerier {
	return mocds.MetricsQuerier
}

// ClusterMap returns a mapping of cluster identifier to ClusterInfo for all known clusters (local only for
// single cluster deployments).
func (mocds *NoOpOpenCostDataSource) ClusterMap() clusters.ClusterMap {
	return nil
}

// ClusterInfo returns the ClusterInfoProvider for the local cluster.
func (mocds *NoOpOpenCostDataSource) ClusterInfo() clusters.ClusterInfoProvider {
	return mocds.ClusterInfoProvider
}

func (mocds *NoOpOpenCostDataSource) BatchDuration() time.Duration {
	return 24 * time.Hour
}

func (mocds *NoOpOpenCostDataSource) Resolution() time.Duration {
	return 5 * time.Minute
}
