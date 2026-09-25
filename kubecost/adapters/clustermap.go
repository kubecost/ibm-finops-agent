package adapters

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/clusters"
)

// OpenCost expects a cluster id -> cluster info mapping for multi-cluster support. Our agent
// will only be a single cluster emitter, so we just need to adapt the single cluster entry to
// the lookup.
type ClusterMapAdapter struct {
	src *stateHolder
}

// ClusterMapAdapter is an adapter for the OpenCost cluster map interface. It allows
// the OpenCost emitter to work with the single cluster information provided by the
// agent.
func NewClusterMapAdapter(clusterInfo *clusters.ClusterInfo) *ClusterMapAdapter {
	return &ClusterMapAdapter{
		src: newStateHolder(&adapterState{info: clusterInfo, metrics: newMetricsState(nil)}),
	}
}

// Update updates the `ClusterInfo` data driving the adapter.
func (cma *ClusterMapAdapter) Update(info *clusters.ClusterInfo) {
	cma.src.update(func(s *adapterState) *adapterState {
		next := *s
		next.info = info
		return &next
	})
}

// AsMap returns a map representation of the cluster information
func (cma *ClusterMapAdapter) AsMap() map[string]*clusters.ClusterInfo {
	info := cma.src.load().info
	if info == nil {
		return map[string]*clusters.ClusterInfo{}
	}

	return map[string]*clusters.ClusterInfo{
		info.ID: info,
	}
}

// InfoFor returns the ClusterInfo for the provided clusterID or nil if it doesn't exist.
func (cma *ClusterMapAdapter) InfoFor(clusterID string) *clusters.ClusterInfo {
	info := cma.src.load().info
	if info == nil || info.ID != clusterID {
		return nil
	}

	return info
}

// GetClusterIDs returns all cluster IDs
func (cma *ClusterMapAdapter) GetClusterIDs() []string {
	info := cma.src.load().info
	if info == nil {
		return []string{}
	}

	return []string{info.ID}
}

// NameFor returns the name of the cluster provided the clusterID.
func (cma *ClusterMapAdapter) NameFor(clusterID string) string {
	info := cma.src.load().info
	if info == nil || info.ID != clusterID {
		return ""
	}

	return info.Name
}

// NameIDFor returns an identifier in the format "<clusterName>/<clusterID>" if the cluster has an
// assigned name. Otherwise, just the clusterID is returned.
func (cma *ClusterMapAdapter) NameIDFor(clusterID string) string {
	info := cma.src.load().info
	if info == nil || info.ID != clusterID {
		return clusterID
	}

	if info.Name == "" {
		return clusterID
	}

	return fmt.Sprintf("%s/%s", info.Name, clusterID)
}
