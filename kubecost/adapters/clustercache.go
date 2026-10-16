package adapters

import (
	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/opencost/opencost/core/pkg/clustercache"
)

// ClusterCacheAdapter is an adapter for the OpenCost cluster cache interface. It is used to provide
// snapshot data to opencost from the emitter.
type ClusterCacheAdapter struct {
	src *stateHolder
}

// NewClusterCacheAdapter creates a new ClusterCacheAdapter instance.
func NewClusterCacheAdapter(snapshot *emitter.KubernetesSnapshot) *ClusterCacheAdapter {
	return &ClusterCacheAdapter{
		src: newStateHolder(&adapterState{kubernetes: snapshot, metrics: newMetricsState(nil)}),
	}
}

// Update refreshes the `KubernetesSnapshot` data driving the adapter.
func (cca *ClusterCacheAdapter) Update(snapshot *emitter.KubernetesSnapshot) {
	cca.src.update(func(s *adapterState) *adapterState {
		next := *s
		next.kubernetes = snapshot
		return &next
	})
}

func (cca *ClusterCacheAdapter) Run() {
	// no-op
}

func (cca *ClusterCacheAdapter) Stop() {
	// no-op
}

func (cca *ClusterCacheAdapter) GetAllNamespaces() []*clustercache.Namespace {
	cc := cca.src.load().kubernetes

	var namespaces []*clustercache.Namespace
	for _, ns := range cc.Namespaces {
		namespaces = append(namespaces, clustercache.TransformNamespace(ns))
	}
	return namespaces
}

func (cca *ClusterCacheAdapter) GetAllNodes() []*clustercache.Node {
	cc := cca.src.load().kubernetes

	var nodes []*clustercache.Node
	for _, node := range cc.Nodes {
		nodes = append(nodes, clustercache.TransformNode(node))
	}
	return nodes
}

func (cca *ClusterCacheAdapter) GetAllPods() []*clustercache.Pod {
	cc := cca.src.load().kubernetes

	var pods []*clustercache.Pod
	for _, pod := range cc.Pods {
		pods = append(pods, clustercache.TransformPod(pod))
	}
	return pods
}

func (cca *ClusterCacheAdapter) GetAllServices() []*clustercache.Service {
	cc := cca.src.load().kubernetes

	var services []*clustercache.Service
	for _, service := range cc.Services {
		services = append(services, clustercache.TransformService(service))
	}
	return services
}

func (cca *ClusterCacheAdapter) GetAllDaemonSets() []*clustercache.DaemonSet {
	cc := cca.src.load().kubernetes

	var daemonsets []*clustercache.DaemonSet
	for _, daemonset := range cc.DaemonSets {
		daemonsets = append(daemonsets, clustercache.TransformDaemonSet(daemonset))
	}
	return daemonsets
}

func (cca *ClusterCacheAdapter) GetAllDeployments() []*clustercache.Deployment {
	cc := cca.src.load().kubernetes

	var deployments []*clustercache.Deployment
	for _, deployment := range cc.Deployments {
		deployments = append(deployments, clustercache.TransformDeployment(deployment))
	}
	return deployments
}

func (cca *ClusterCacheAdapter) GetAllStatefulSets() []*clustercache.StatefulSet {
	cc := cca.src.load().kubernetes

	var statefulsets []*clustercache.StatefulSet
	for _, statefulset := range cc.StatefulSets {
		statefulsets = append(statefulsets, clustercache.TransformStatefulSet(statefulset))
	}
	return statefulsets
}

func (cca *ClusterCacheAdapter) GetAllReplicaSets() []*clustercache.ReplicaSet {
	cc := cca.src.load().kubernetes

	var replicasets []*clustercache.ReplicaSet
	for _, replicaset := range cc.ReplicaSets {
		replicasets = append(replicasets, clustercache.TransformReplicaSet(replicaset))
	}
	return replicasets
}

func (cca *ClusterCacheAdapter) GetAllPersistentVolumes() []*clustercache.PersistentVolume {
	cc := cca.src.load().kubernetes

	var pvs []*clustercache.PersistentVolume
	for _, pv := range cc.PersistentVolumes {
		pvs = append(pvs, clustercache.TransformPersistentVolume(pv))
	}
	return pvs
}

func (cca *ClusterCacheAdapter) GetAllPersistentVolumeClaims() []*clustercache.PersistentVolumeClaim {
	cc := cca.src.load().kubernetes

	var pvcs []*clustercache.PersistentVolumeClaim
	for _, pvc := range cc.PersistentVolumeClaims {
		pvcs = append(pvcs, clustercache.TransformPersistentVolumeClaim(pvc))
	}
	return pvcs
}

func (cca *ClusterCacheAdapter) GetAllStorageClasses() []*clustercache.StorageClass {
	cc := cca.src.load().kubernetes

	var storageClasses []*clustercache.StorageClass
	for _, stc := range cc.StorageClasses {
		storageClasses = append(storageClasses, clustercache.TransformStorageClass(stc))
	}
	return storageClasses
}

func (cca *ClusterCacheAdapter) GetAllJobs() []*clustercache.Job {
	cc := cca.src.load().kubernetes

	var jobs []*clustercache.Job
	for _, job := range cc.Jobs {
		jobs = append(jobs, clustercache.TransformJob(job))
	}
	return jobs
}

func (cca *ClusterCacheAdapter) GetAllCronJobs() []*clustercache.CronJob {
	cc := cca.src.load().kubernetes

	var cronJobs []*clustercache.CronJob
	for _, cronJob := range cc.CronJobs {
		cronJobs = append(cronJobs, clustercache.TransformCronJob(cronJob))
	}
	return cronJobs
}

func (cca *ClusterCacheAdapter) GetAllPodDisruptionBudgets() []*clustercache.PodDisruptionBudget {
	cc := cca.src.load().kubernetes

	var pdbs []*clustercache.PodDisruptionBudget
	for _, pdb := range cc.PodDisruptionBudgets {
		pdbs = append(pdbs, clustercache.TransformPodDisruptionBudget(pdb))
	}
	return pdbs
}

func (cca *ClusterCacheAdapter) GetAllReplicationControllers() []*clustercache.ReplicationController {
	cc := cca.src.load().kubernetes

	var rcs []*clustercache.ReplicationController
	for _, rc := range cc.ReplicationControllers {
		rcs = append(rcs, clustercache.TransformReplicationController(rc))
	}
	return rcs
}

func (cca *ClusterCacheAdapter) GetAllResourceQuotas() []*clustercache.ResourceQuota {
	cc := cca.src.load().kubernetes

	var rqs []*clustercache.ResourceQuota
	for _, rq := range cc.ResourceQuotas {
		rqs = append(rqs, clustercache.TransformResourceQuota(rq))
	}
	return rqs
}
