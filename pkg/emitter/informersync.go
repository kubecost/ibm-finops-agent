package emitter

import (
	"fmt"

	clustercache "github.com/ibm/finops-agent/pkg/cluster"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// checkInformersSynced fails when an informer for a resource the snapshot needs hasn't synced.
// Until it has, its list is empty or partial, and emitting it would report objects as absent.
// Informers for resources no emitter needs don't block (F-12, D9).
func checkInformersSynced(cluster clustercache.ClusterCache, config *SnapshotConfig) error {
	sr, ok := cluster.(clustercache.SyncReporter)
	if !ok {
		return nil
	}
	unsynced := sr.UnsyncedResources()
	if len(unsynced) == 0 {
		return nil
	}
	kconfig := config.KubernetesSnapshot
	if kconfig == nil {
		kconfig = NewKubernetesSnapshotConfig().EnableAll()
	}
	var needed []schema.GroupVersionResource
	for _, gvr := range unsynced {
		if snapshotNeeds(kconfig, gvr) {
			needed = append(needed, gvr)
		}
	}
	if len(needed) == 0 {
		return nil
	}
	return fmt.Errorf("%s", clustercache.InformersUnsyncedMessage(needed))
}

// snapshotNeeds reports whether a snapshot under kconfig lists gvr. Pods are always needed:
// short-lived pods are captured from the pod informer.
func snapshotNeeds(kconfig *KubernetesSnapshotConfig, gvr schema.GroupVersionResource) bool {
	switch clustercache.FormatResource(gvr) {
	case "nodes":
		return kconfig.Nodes
	case "pods":
		return true
	case "namespaces":
		return kconfig.Namespaces
	case "services":
		return kconfig.Services
	case "apps/daemonsets":
		return kconfig.DaemonSets
	case "apps/deployments":
		return kconfig.Deployments
	case "apps/statefulsets":
		return kconfig.StatefulSets
	case "apps/replicasets":
		return kconfig.ReplicaSets
	case "persistentvolumes":
		return kconfig.PersistentVolumes
	case "persistentvolumeclaims":
		return kconfig.PersistentVolumeClaims
	case "storage.k8s.io/storageclasses":
		return kconfig.StorageClasses
	case "batch/jobs":
		return kconfig.Jobs
	case "policy/poddisruptionbudgets":
		return kconfig.PodDisruptionBudgets
	case "replicationcontrollers":
		return kconfig.ReplicationControllers
	case "resourcequotas":
		return kconfig.ResourceQuotas
	}
	return false
}
