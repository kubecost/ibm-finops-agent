package cluster

import (
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	stv1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TODO: Cloudy/Turbo needs filtering functionality for specific k8s resources!

// ClusterCache defines an contract for an object which caches components within a cluster, ensuring
// up to date resources using watchers
type ClusterCache interface {

	// Starts the watcher processes
	Start(stopCh <-chan struct{})
	// Shutdown the watcher processes
	Shutdown()

	// GetAllNamespaces returns all the cached namespaces
	GetAllNamespaces() []*v1.Namespace

	// GetAllNodes returns all the cached nodes
	GetAllNodes() []*v1.Node

	// GetAllPods returns all the cached pods
	GetAllPods() []*v1.Pod

	// GetAllShortLivedPods returns all pods with short duration that were recently deleted
	GetAllShortLivedPods() []*v1.Pod

	// GetAllServices returns all the cached services
	GetAllServices() []*v1.Service

	// GetAllDaemonSets returns all the cached DaemonSets
	GetAllDaemonSets() []*appsv1.DaemonSet

	// GetAllDeployments returns all the cached deployments
	GetAllDeployments() []*appsv1.Deployment

	// GetAllStatfulSets returns all the cached StatefulSets
	GetAllStatefulSets() []*appsv1.StatefulSet

	// GetAllReplicaSets returns all the cached ReplicaSets
	GetAllReplicaSets() []*appsv1.ReplicaSet

	// GetAllPersistentVolumes returns all the cached persistent volumes
	GetAllPersistentVolumes() []*v1.PersistentVolume

	// GetAllPersistentVolumeClaims returns all the cached persistent volume claims
	GetAllPersistentVolumeClaims() []*v1.PersistentVolumeClaim

	// GetAllStorageClasses returns all the cached storage classes
	GetAllStorageClasses() []*stv1.StorageClass

	// GetAllJobs returns all the cached jobs
	GetAllJobs() []*batchv1.Job

	// GetAllCronJobs returns all the cached jobs
	GetAllCronJobs() []*batchv1.CronJob

	// GetAllPodDisruptionBudgets returns all cached pod disruption budgets
	GetAllPodDisruptionBudgets() []*policyv1.PodDisruptionBudget

	// GetAllReplicationControllers returns all cached replication controllers
	GetAllReplicationControllers() []*v1.ReplicationController

	// GetAllResourceQuotas returns all cached resource quotas
	GetAllResourceQuotas() []*v1.ResourceQuota

	// ListUnstructuredByGroupVersionResource returns array of unstructured objects by group version resource
	ListUnstructuredByGroupVersionResource(gvr schema.GroupVersionResource) []*unstructured.Unstructured
}
