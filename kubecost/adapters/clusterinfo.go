package adapters

import (
	"sync"

	"github.com/opencost/opencost/core/pkg/clusters"
)

type ClusterInfoProviderAdapter struct {
	lock sync.RWMutex
	info *clusters.ClusterInfo
}

func NewClusterInfoProviderAdapter(info *clusters.ClusterInfo) *ClusterInfoProviderAdapter {
	return &ClusterInfoProviderAdapter{
		info: info,
	}
}

func (cipa *ClusterInfoProviderAdapter) Update(info *clusters.ClusterInfo) {
	cipa.lock.Lock()
	defer cipa.lock.Unlock()

	cipa.info = info
}

// GetClusterInfo returns a string map containing the local/remote connected cluster info
func (cipa *ClusterInfoProviderAdapter) GetClusterInfo() map[string]string {
	cipa.lock.RLock()
	defer cipa.lock.RUnlock()

	if cipa.info == nil {
		return nil
	}

	return map[string]string{
		clusters.ClusterInfoIdKey:          cipa.info.ID,
		clusters.ClusterInfoNameKey:        cipa.info.Name,
		clusters.ClusterInfoProfileKey:     cipa.info.Profile,
		clusters.ClusterInfoProviderKey:    cipa.info.Provider,
		clusters.ClusterInfoAccountKey:     cipa.info.Account,
		clusters.ClusterInfoProjectKey:     cipa.info.Project,
		clusters.ClusterInfoRegionKey:      cipa.info.Region,
		clusters.ClusterInfoProvisionerKey: cipa.info.Provisioner,
	}
}
