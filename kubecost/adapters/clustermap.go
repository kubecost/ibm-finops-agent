package adapters

import (
	"fmt"
	"sync"

	"github.com/opencost/opencost/core/pkg/clusters"
)

// OpenCost expects a cluster id -> cluster info mapping for multi-cluster support. Our agent
// will only be a single cluster emitter, so we just need to adapt the single cluster entry to
// the lookup.
type ClusterMapAdapter struct {
	lock sync.RWMutex
	info *clusters.ClusterInfo
}

// ClusterMapAdapter is an adapter for the OpenCost cluster map interface. It allows
// the OpenCost emitter to work with the single cluster information provided by the
// agent.
func NewClusterMapAdapter(clusterInfo *clusters.ClusterInfo) *ClusterMapAdapter {
	return &ClusterMapAdapter{
		info: clusterInfo,
	}
}

// Update updates the `ClusterInfo` data driving the adapter.
func (cma *ClusterMapAdapter) Update(info *clusters.ClusterInfo) {
	cma.lock.Lock()
	defer cma.lock.Unlock()

	cma.info = info
}

// AsMap returns a map representation of the cluster information
func (cma *ClusterMapAdapter) AsMap() map[string]*clusters.ClusterInfo {
	cma.lock.RLock()
	defer cma.lock.RUnlock()

	if cma.info == nil {
		return map[string]*clusters.ClusterInfo{}
	}

	return map[string]*clusters.ClusterInfo{
		cma.info.ID: cma.info,
	}
}

// InfoFor returns the ClusterInfo for the provided clusterID or nil if it doesn't exist.
func (cma *ClusterMapAdapter) InfoFor(clusterID string) *clusters.ClusterInfo {
	cma.lock.RLock()
	defer cma.lock.RUnlock()

	if cma.info == nil || cma.info.ID != clusterID {
		return nil
	}

	return cma.info
}

// GetClusterIDs returns all cluster IDs
func (cma *ClusterMapAdapter) GetClusterIDs() []string {
	cma.lock.RLock()
	defer cma.lock.RUnlock()

	if cma.info == nil {
		return []string{}
	}

	return []string{cma.info.ID}
}

// NameFor returns the name of the cluster provided the clusterID.
func (cma *ClusterMapAdapter) NameFor(clusterID string) string {
	cma.lock.RLock()
	defer cma.lock.RUnlock()

	if cma.info == nil || cma.info.ID != clusterID {
		return ""
	}

	return cma.info.Name
}

// NameIDFor returns an identifier in the format "<clusterName>/<clusterID>" if the cluster has an
// assigned name. Otherwise, just the clusterID is returned.
func (cma *ClusterMapAdapter) NameIDFor(clusterID string) string {
	cma.lock.RLock()
	defer cma.lock.RUnlock()

	if cma.info == nil || cma.info.ID != clusterID {
		return clusterID
	}

	if cma.info.Name == "" {
		return clusterID
	}

	return fmt.Sprintf("%s/%s", cma.info.Name, clusterID)
}
