package cluster

import (
	"github.com/opencost/opencost/core/pkg/clustercache"
)

type OpenCostClusterCacheAdapter struct {
	clusterCache ClusterCache
}

// NewOpenCostClusterCacheAdapter creates a new adapter that will adapt the unified agent cluster cache to the opencost variant.
func NewOpenCostClusterCacheAdapter(clusterCache ClusterCache) *OpenCostClusterCacheAdapter {
	return &OpenCostClusterCacheAdapter{
		clusterCache: clusterCache,
	}
}

func (kcc *OpenCostClusterCacheAdapter) Run() {
	// no-op
}

func (kcc *OpenCostClusterCacheAdapter) Stop() {
	// no-op
}

func (kcc *OpenCostClusterCacheAdapter) GetAllNamespaces() []*clustercache.Namespace {
	cc := kcc.clusterCache

	var namespaces []*clustercache.Namespace
	for _, ns := range cc.GetAllNamespaces() {
		namespaces = append(namespaces, clustercache.TransformNamespace(ns))
	}
	return namespaces
}

func (kcc *OpenCostClusterCacheAdapter) GetAllNodes() []*clustercache.Node {
	cc := kcc.clusterCache

	var nodes []*clustercache.Node
	for _, node := range cc.GetAllNodes() {
		nodes = append(nodes, clustercache.TransformNode(node))
	}
	return nodes
}

func (kcc *OpenCostClusterCacheAdapter) GetAllPods() []*clustercache.Pod {
	cc := kcc.clusterCache

	var pods []*clustercache.Pod
	for _, pod := range cc.GetAllPods() {
		pods = append(pods, clustercache.TransformPod(pod))
	}
	return pods
}

func (kcc *OpenCostClusterCacheAdapter) GetAllServices() []*clustercache.Service {
	cc := kcc.clusterCache

	var services []*clustercache.Service
	for _, service := range cc.GetAllServices() {
		services = append(services, clustercache.TransformService(service))
	}
	return services
}

func (kcc *OpenCostClusterCacheAdapter) GetAllDaemonSets() []*clustercache.DaemonSet {
	cc := kcc.clusterCache

	var daemonsets []*clustercache.DaemonSet
	for _, daemonset := range cc.GetAllDaemonSets() {
		daemonsets = append(daemonsets, clustercache.TransformDaemonSet(daemonset))
	}
	return daemonsets
}

func (kcc *OpenCostClusterCacheAdapter) GetAllDeployments() []*clustercache.Deployment {
	cc := kcc.clusterCache

	var deployments []*clustercache.Deployment
	for _, deployment := range cc.GetAllDeployments() {
		deployments = append(deployments, clustercache.TransformDeployment(deployment))
	}
	return deployments
}

func (kcc *OpenCostClusterCacheAdapter) GetAllStatefulSets() []*clustercache.StatefulSet {
	cc := kcc.clusterCache

	var statefulsets []*clustercache.StatefulSet
	for _, statefulset := range cc.GetAllStatefulSets() {
		statefulsets = append(statefulsets, clustercache.TransformStatefulSet(statefulset))
	}
	return statefulsets
}

func (kcc *OpenCostClusterCacheAdapter) GetAllReplicaSets() []*clustercache.ReplicaSet {
	cc := kcc.clusterCache

	var replicasets []*clustercache.ReplicaSet
	for _, replicaset := range cc.GetAllReplicaSets() {
		replicasets = append(replicasets, clustercache.TransformReplicaSet(replicaset))
	}
	return replicasets
}

func (kcc *OpenCostClusterCacheAdapter) GetAllPersistentVolumes() []*clustercache.PersistentVolume {
	cc := kcc.clusterCache

	var pvs []*clustercache.PersistentVolume
	for _, pv := range cc.GetAllPersistentVolumes() {
		pvs = append(pvs, clustercache.TransformPersistentVolume(pv))
	}
	return pvs
}

func (kcc *OpenCostClusterCacheAdapter) GetAllPersistentVolumeClaims() []*clustercache.PersistentVolumeClaim {
	cc := kcc.clusterCache

	var pvcs []*clustercache.PersistentVolumeClaim
	for _, pvc := range cc.GetAllPersistentVolumeClaims() {
		pvcs = append(pvcs, clustercache.TransformPersistentVolumeClaim(pvc))
	}
	return pvcs
}

func (kcc *OpenCostClusterCacheAdapter) GetAllStorageClasses() []*clustercache.StorageClass {
	cc := kcc.clusterCache

	var storageClasses []*clustercache.StorageClass
	for _, stc := range cc.GetAllStorageClasses() {
		storageClasses = append(storageClasses, clustercache.TransformStorageClass(stc))
	}
	return storageClasses
}

func (kcc *OpenCostClusterCacheAdapter) GetAllJobs() []*clustercache.Job {
	cc := kcc.clusterCache

	var jobs []*clustercache.Job
	for _, job := range cc.GetAllJobs() {
		jobs = append(jobs, clustercache.TransformJob(job))
	}
	return jobs
}

func (kcc *OpenCostClusterCacheAdapter) GetAllPodDisruptionBudgets() []*clustercache.PodDisruptionBudget {
	cc := kcc.clusterCache

	var pdbs []*clustercache.PodDisruptionBudget
	for _, pdb := range cc.GetAllPodDisruptionBudgets() {
		pdbs = append(pdbs, clustercache.TransformPodDisruptionBudget(pdb))
	}
	return pdbs
}

func (kcc *OpenCostClusterCacheAdapter) GetAllReplicationControllers() []*clustercache.ReplicationController {
	cc := kcc.clusterCache

	var rcs []*clustercache.ReplicationController
	for _, rc := range cc.GetAllReplicationControllers() {
		rcs = append(rcs, clustercache.TransformReplicationController(rc))
	}
	return rcs
}

func (kcc *OpenCostClusterCacheAdapter) GetAllResourceQuotas() []*clustercache.ResourceQuota {
	cc := kcc.clusterCache

	var rqs []*clustercache.ResourceQuota
	for _, rq := range cc.GetAllResourceQuotas() {
		rqs = append(rqs, clustercache.TransformResourceQuota(rq))
	}
	return rqs
}
