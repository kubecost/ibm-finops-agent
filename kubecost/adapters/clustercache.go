package adapters

import (
	"sync"

	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/opencost/opencost/core/pkg/clustercache"
)

// ClusterCacheAdapter is an adapter for the OpenCost cluster cache interface. It is used to provide
// snapshot data to opencost from the emitter.
type ClusterCacheAdapter struct {
	lock     sync.RWMutex
	snapshot *emitter.KubernetesSnapshot
}

// NewClusterCacheAdapter creates a new ClusterCacheAdapter instance.
func NewClusterCacheAdapter(snapshot *emitter.KubernetesSnapshot) *ClusterCacheAdapter {
	return &ClusterCacheAdapter{
		snapshot: snapshot,
	}
}

// Update refreshes the `KubernetesSnapshot` data driving the adapter.
func (cca *ClusterCacheAdapter) Update(snapshot *emitter.KubernetesSnapshot) {
	cca.lock.Lock()
	defer cca.lock.Unlock()

	cca.snapshot = snapshot
}

func (cca *ClusterCacheAdapter) Run() {
	// no-op
}

func (cca *ClusterCacheAdapter) Stop() {
	// no-op
}

// TODO revisit
func (cca *ClusterCacheAdapter) GetClusterUID() string {
	return cca.snapshot.ClusterUID
}

func (cca *ClusterCacheAdapter) GetAllNamespaces() []*clustercache.Namespace {
	cca.lock.RLock()
	defer cca.lock.RUnlock()

	cc := cca.snapshot

	var namespaces []*clustercache.Namespace
	for _, ns := range cc.Namespaces {
		namespaces = append(namespaces, clustercache.TransformNamespace(ns))
	}
	return namespaces
}

func (cca *ClusterCacheAdapter) GetAllNodes() []*clustercache.Node {
	cca.lock.RLock()
	defer cca.lock.RUnlock()

	cc := cca.snapshot

	var nodes []*clustercache.Node
	for _, node := range cc.Nodes {
		nodes = append(nodes, clustercache.TransformNode(node))
	}
	return nodes
}

func (cca *ClusterCacheAdapter) GetAllPods() []*clustercache.Pod {
	cca.lock.RLock()
	defer cca.lock.RUnlock()

	cc := cca.snapshot

	var pods []*clustercache.Pod
	for _, pod := range cc.Pods {
		pods = append(pods, clustercache.TransformPod(pod))
	}
	return pods
}

func (cca *ClusterCacheAdapter) GetAllServices() []*clustercache.Service {
	cca.lock.RLock()
	defer cca.lock.RUnlock()

	cc := cca.snapshot

	var services []*clustercache.Service
	for _, service := range cc.Services {
		services = append(services, clustercache.TransformService(service))
	}
	return services
}

func (cca *ClusterCacheAdapter) GetAllDaemonSets() []*clustercache.DaemonSet {
	cca.lock.RLock()
	defer cca.lock.RUnlock()

	cc := cca.snapshot

	var daemonsets []*clustercache.DaemonSet
	for _, daemonset := range cc.DaemonSets {
		daemonsets = append(daemonsets, clustercache.TransformDaemonSet(daemonset))
	}
	return daemonsets
}

func (cca *ClusterCacheAdapter) GetAllDeployments() []*clustercache.Deployment {
	cca.lock.RLock()
	defer cca.lock.RUnlock()

	cc := cca.snapshot

	var deployments []*clustercache.Deployment
	for _, deployment := range cc.Deployments {
		deployments = append(deployments, clustercache.TransformDeployment(deployment))
	}
	return deployments
}

func (cca *ClusterCacheAdapter) GetAllStatefulSets() []*clustercache.StatefulSet {
	cca.lock.RLock()
	defer cca.lock.RUnlock()

	cc := cca.snapshot

	var statefulsets []*clustercache.StatefulSet
	for _, statefulset := range cc.StatefulSets {
		statefulsets = append(statefulsets, clustercache.TransformStatefulSet(statefulset))
	}
	return statefulsets
}

func (cca *ClusterCacheAdapter) GetAllReplicaSets() []*clustercache.ReplicaSet {
	cca.lock.RLock()
	defer cca.lock.RUnlock()

	cc := cca.snapshot

	var replicasets []*clustercache.ReplicaSet
	for _, replicaset := range cc.ReplicaSets {
		replicasets = append(replicasets, clustercache.TransformReplicaSet(replicaset))
	}
	return replicasets
}

func (cca *ClusterCacheAdapter) GetAllPersistentVolumes() []*clustercache.PersistentVolume {
	cca.lock.RLock()
	defer cca.lock.RUnlock()

	cc := cca.snapshot

	var pvs []*clustercache.PersistentVolume
	for _, pv := range cc.PersistentVolumes {
		pvs = append(pvs, clustercache.TransformPersistentVolume(pv))
	}
	return pvs
}

func (cca *ClusterCacheAdapter) GetAllPersistentVolumeClaims() []*clustercache.PersistentVolumeClaim {
	cca.lock.RLock()
	defer cca.lock.RUnlock()

	cc := cca.snapshot

	var pvcs []*clustercache.PersistentVolumeClaim
	for _, pvc := range cc.PersistentVolumeClaims {
		pvcs = append(pvcs, clustercache.TransformPersistentVolumeClaim(pvc))
	}
	return pvcs
}

func (cca *ClusterCacheAdapter) GetAllStorageClasses() []*clustercache.StorageClass {
	cca.lock.RLock()
	defer cca.lock.RUnlock()

	cc := cca.snapshot

	var storageClasses []*clustercache.StorageClass
	for _, stc := range cc.StorageClasses {
		storageClasses = append(storageClasses, clustercache.TransformStorageClass(stc))
	}
	return storageClasses
}

func (cca *ClusterCacheAdapter) GetAllJobs() []*clustercache.Job {
	cca.lock.RLock()
	defer cca.lock.RUnlock()

	cc := cca.snapshot

	var jobs []*clustercache.Job
	for _, job := range cc.Jobs {
		jobs = append(jobs, clustercache.TransformJob(job))
	}
	return jobs
}

func (cca *ClusterCacheAdapter) GetAllPodDisruptionBudgets() []*clustercache.PodDisruptionBudget {
	cca.lock.RLock()
	defer cca.lock.RUnlock()

	cc := cca.snapshot

	var pdbs []*clustercache.PodDisruptionBudget
	for _, pdb := range cc.PodDisruptionBudgets {
		pdbs = append(pdbs, clustercache.TransformPodDisruptionBudget(pdb))
	}
	return pdbs
}

func (cca *ClusterCacheAdapter) GetAllReplicationControllers() []*clustercache.ReplicationController {
	cca.lock.RLock()
	defer cca.lock.RUnlock()

	cc := cca.snapshot

	var rcs []*clustercache.ReplicationController
	for _, rc := range cc.ReplicationControllers {
		rcs = append(rcs, clustercache.TransformReplicationController(rc))
	}
	return rcs
}

func (cca *ClusterCacheAdapter) GetAllResourceQuotas() []*clustercache.ResourceQuota {
	cca.lock.RLock()
	defer cca.lock.RUnlock()

	cc := cca.snapshot

	var rqs []*clustercache.ResourceQuota
	for _, rq := range cc.ResourceQuotas {
		rqs = append(rqs, clustercache.TransformResourceQuota(rq))
	}
	return rqs
}
