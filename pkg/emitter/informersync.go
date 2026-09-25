package emitter

import (
	clustercache "github.com/ibm/finops-agent/pkg/cluster"
)

// omitUnsynced returns config with the snapshot of every resource whose informer hasn't synced
// turned off, and the names of the resources it turned off. Until an informer syncs its list is
// empty or partial, so the snapshot leaves that resource empty and says so
// (KubernetesSnapshot.UnsyncedResources) rather than passing a partial list off as complete. The
// other resources are snapshotted as usual, so one missing RBAC permission doesn't stop every
// emitter (F-12, D9).
func omitUnsynced(cluster clustercache.ClusterCache, config *SnapshotConfig) (*SnapshotConfig, []string) {
	sr, ok := cluster.(clustercache.SyncReporter)
	if !ok {
		return config, nil
	}
	unsynced := sr.UnsyncedResources()
	if len(unsynced) == 0 {
		return config, nil
	}
	kconfig := NewKubernetesSnapshotConfig().EnableAll()
	if config.KubernetesSnapshot != nil {
		copied := *config.KubernetesSnapshot
		kconfig = &copied
	}
	var omitted []string
	for _, gvr := range unsynced {
		name := clustercache.FormatResource(gvr)
		if flag := snapshotFlag(kconfig, name); flag != nil && *flag {
			*flag = false
			omitted = append(omitted, name)
		}
	}
	if len(omitted) == 0 {
		return config, nil
	}
	copied := *config
	copied.KubernetesSnapshot = kconfig
	return &copied, omitted
}

// snapshotFlag returns kconfig's flag for the named resource, or nil if it has none.
func snapshotFlag(kconfig *KubernetesSnapshotConfig, resource string) *bool {
	switch resource {
	case "nodes":
		return &kconfig.Nodes
	case "pods":
		return &kconfig.Pods
	case "namespaces":
		return &kconfig.Namespaces
	case "services":
		return &kconfig.Services
	case "apps/daemonsets":
		return &kconfig.DaemonSets
	case "apps/deployments":
		return &kconfig.Deployments
	case "apps/statefulsets":
		return &kconfig.StatefulSets
	case "apps/replicasets":
		return &kconfig.ReplicaSets
	case "persistentvolumes":
		return &kconfig.PersistentVolumes
	case "persistentvolumeclaims":
		return &kconfig.PersistentVolumeClaims
	case "storage.k8s.io/storageclasses":
		return &kconfig.StorageClasses
	case "batch/jobs":
		return &kconfig.Jobs
	case "policy/poddisruptionbudgets":
		return &kconfig.PodDisruptionBudgets
	case "replicationcontrollers":
		return &kconfig.ReplicationControllers
	case "resourcequotas":
		return &kconfig.ResourceQuotas
	}
	return nil
}
