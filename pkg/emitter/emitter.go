package emitter

import (
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	stv1 "k8s.io/api/storage/v1"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

// KubernetesSnapshot contains the state of a Kubernetes cluster at a given point in time.
type KubernetesSnapshot struct {
	Nodes                  []*v1.Node
	Pods                   []*v1.Pod
	Namespaces             []*v1.Namespace
	Services               []*v1.Service
	DaemonSets             []*appsv1.DaemonSet
	Deployments            []*appsv1.Deployment
	StatefulSets           []*appsv1.StatefulSet
	ReplicaSets            []*appsv1.ReplicaSet
	PersistentVolumes      []*v1.PersistentVolume
	PersistentVolumeClaims []*v1.PersistentVolumeClaim
	StorageClasses         []*stv1.StorageClass
	Jobs                   []*batchv1.Job
	PodDisruptionBudgets   []*policyv1.PodDisruptionBudget
	ReplicationControllers []*v1.ReplicationController
}

// NodeStatsSummary contains summary data sets
type NodeStatsSummary struct {
	Stats []stats.Summary
}

type MetricsSummary struct {
	// TODO: Representation of metrics results required for opencost
}

type ClusterSnapshot struct {
	Kubernetes *KubernetesSnapshot
	NodeStats  *NodeStatsSummary
	Metrics    *MetricsSummary
}

// Emitter is a contract for an implementation which is directly sent cluster data snapshots on
// a regular interval.
type Emitter interface {
	Emit(ClusterSnapshot) error
}
