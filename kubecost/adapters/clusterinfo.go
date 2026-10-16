package adapters

import (
	"github.com/opencost/opencost/core/pkg/clusters"
)

type ClusterInfoProviderAdapter struct {
	src *stateHolder
}

func NewClusterInfoProviderAdapter(info *clusters.ClusterInfo) *ClusterInfoProviderAdapter {
	return &ClusterInfoProviderAdapter{
		src: newStateHolder(&adapterState{info: info, metrics: newMetricsState(nil)}),
	}
}

func (cipa *ClusterInfoProviderAdapter) Update(info *clusters.ClusterInfo) {
	cipa.src.update(func(s *adapterState) *adapterState {
		next := *s
		next.info = info
		return &next
	})
}

// GetClusterInfo returns a string map containing the local/remote connected cluster info
func (cipa *ClusterInfoProviderAdapter) GetClusterInfo() map[string]string {
	info := cipa.src.load().info
	if info == nil {
		return nil
	}

	return map[string]string{
		clusters.ClusterInfoIdKey:          info.ID,
		clusters.ClusterInfoNameKey:        info.Name,
		clusters.ClusterInfoProfileKey:     info.Profile,
		clusters.ClusterInfoProviderKey:    info.Provider,
		clusters.ClusterInfoAccountKey:     info.Account,
		clusters.ClusterInfoProjectKey:     info.Project,
		clusters.ClusterInfoRegionKey:      info.Region,
		clusters.ClusterInfoProvisionerKey: info.Provisioner,
		clusters.ClusterInfoVersionKey:     info.Version,
	}
}
