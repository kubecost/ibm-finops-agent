package emitter

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	stv1 "k8s.io/api/storage/v1"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"

	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
)

// KubernetesSnapshot contains the state of a Kubernetes cluster at a give n point in time.
type KubernetesSnapshot struct {
	Nodes                  []*v1.Node
	Pods                   []*v1.Pod
	ShortLivedPods         []*v1.Pod
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
	Stats []*stats.Summary
}

// MetricsSummary contains the metrics results from opencost data source queries.
type MetricsSummary struct {
	Minutely []*MetricsSnapshot
	Hourly   []*MetricsSnapshot
	Daily    []*MetricsSnapshot
}

// MetricsSnapshot contains the metrics results from opencost data source queries.
type MetricsSnapshot struct {
	Window                               opencost.Window
	PVActiveMinutes                      []*source.PVActiveMinutesResult
	PVUsedAverage                        []*source.PVUsedAvgResult
	PVUsedMax                            []*source.PVUsedMaxResult
	LocalStorageActiveMinutes            []*source.LocalStorageActiveMinutesResult
	LocalStorageCost                     []*source.LocalStorageCostResult
	LocalStorageUsedCost                 []*source.LocalStorageUsedCostResult
	LocalStorageUsedAvg                  []*source.LocalStorageUsedAvgResult
	LocalStorageUsedMax                  []*source.LocalStorageUsedMaxResult
	LocalStorageBytes                    []*source.LocalStorageBytesResult
	NodeActiveMinutes                    []*source.NodeActiveMinutesResult
	NodeCPUCoresCapacity                 []*source.NodeCPUCoresCapacityResult
	NodeCPUCoresAllocatable              []*source.NodeCPUCoresAllocatableResult
	NodeRAMBytesCapacity                 []*source.NodeRAMBytesCapacityResult
	NodeRAMBytesAllocatable              []*source.NodeRAMBytesAllocatableResult
	NodeGPUCount                         []*source.NodeGPUCountResult
	NodeCPUModeTotal                     []*source.NodeCPUModeTotalResult
	NodeIsSpot                           []*source.NodeIsSpotResult
	NodeRAMSystemPercent                 []*source.NodeRAMSystemPercentResult
	NodeRAMUserPercent                   []*source.NodeRAMUserPercentResult
	LBActiveMinutes                      []*source.LBActiveMinutesResult
	LBPricePerHr                         []*source.LBPricePerHrResult
	ClusterManagementDuration            []*source.ClusterManagementDurationResult
	ClusterManagementPricePerHr          []*source.ClusterManagementPricePerHrResult
	Pods                                 []*source.PodsResult
	PodsUID                              []*source.PodsResult
	RAMBytesAllocated                    []*source.RAMBytesAllocatedResult
	RAMRequests                          []*source.RAMRequestsResult
	RAMUsageAvg                          []*source.RAMUsageAvgResult
	RAMUsageMax                          []*source.RAMUsageMaxResult
	NodeRAMPricePerGiBHr                 []*source.NodeRAMPricePerGiBHrResult
	CPUCoresAllocated                    []*source.CPUCoresAllocatedResult
	CPURequests                          []*source.CPURequestsResult
	CPUUsageAvg                          []*source.CPUUsageAvgResult
	CPUUsageMax                          []*source.CPUUsageMaxResult
	NodeCPUPricePerHr                    []*source.NodeCPUPricePerHrResult
	GPUsAllocated                        []*source.GPUsAllocatedResult
	GPUsRequested                        []*source.GPUsRequestedResult
	GPUsUsageAvg                         []*source.GPUsUsageAvgResult
	GPUsUsageMax                         []*source.GPUsUsageMaxResult
	NodeGPUPricePerHr                    []*source.NodeGPUPricePerHrResult
	GPUInfo                              []*source.GPUInfoResult
	IsGPUShared                          []*source.IsGPUSharedResult
	PodPVCAllocation                     []*source.PodPVCAllocationResult
	PVCBytesRequested                    []*source.PVCBytesRequestedResult
	PVCInfo                              []*source.PVCInfoResult
	PVBytes                              []*source.PVBytesResult
	PVPricePerGiBHour                    []*source.PVPricePerGiBHourResult
	PVInfo                               []*source.PVInfoResult
	NetZoneGiB                           []*source.NetZoneGiBResult
	NetZonePricePerGiB                   []*source.NetZonePricePerGiBResult
	NetRegionGiB                         []*source.NetRegionGiBResult
	NetRegionPricePerGiB                 []*source.NetRegionPricePerGiBResult
	NetInternetGiB                       []*source.NetInternetGiBResult
	NetInternetPricePerGiB               []*source.NetInternetPricePerGiBResult
	NetInternetServiceGiB                []*source.NetInternetServiceGiBResult
	NetTransferBytes                     []*source.NetTransferBytesResult
	NetZoneIngressGiB                    []*source.NetZoneIngressGiBResult
	NetRegionIngressGiB                  []*source.NetRegionIngressGiBResult
	NetInternetIngressGiB                []*source.NetInternetIngressGiBResult
	NetInternetServiceIngressGiB         []*source.NetInternetServiceIngressGiBResult
	NetReceiveBytes                      []*source.NetReceiveBytesResult
	NamespaceAnnotations                 []*source.NamespaceAnnotationsResult
	PodAnnotations                       []*source.PodAnnotationsResult
	NodeLabels                           []*source.NodeLabelsResult
	NamespaceLabels                      []*source.NamespaceLabelsResult
	PodLabels                            []*source.PodLabelsResult
	ServiceLabels                        []*source.ServiceLabelsResult
	DeploymentLabels                     []*source.DeploymentLabelsResult
	StatefulSetLabels                    []*source.StatefulSetLabelsResult
	DaemonSetLabels                      []*source.DaemonSetLabelsResult
	JobLabels                            []*source.JobLabelsResult
	PodsWithReplicaSetOwner              []*source.PodsWithReplicaSetOwnerResult
	ReplicaSetsWithoutOwners             []*source.ReplicaSetsWithoutOwnersResult
	ReplicaSetsWithRollout               []*source.ReplicaSetsWithRolloutResult
	ResourceQuotaSpecCPURequestAvg       []*source.ResourceQuotaSpecCPURequestAvgResult
	ResourceQuotaSpecCPURequestMax       []*source.ResourceQuotaSpecCPURequestMaxResult
	ResourceQuotaSpecRAMRequestAvg       []*source.ResourceQuotaSpecRAMRequestAvgResult
	ResourceQuotaSpecRAMRequestMax       []*source.ResourceQuotaSpecRAMRequestMaxResult
	ResourceQuotaSpecCPULimitAvg         []*source.ResourceQuotaSpecCPULimitAvgResult
	ResourceQuotaSpecCPULimitMax         []*source.ResourceQuotaSpecCPULimitMaxResult
	ResourceQuotaSpecRAMLimitAvg         []*source.ResourceQuotaSpecRAMLimitAvgResult
	ResourceQuotaSpecRAMLimitMax         []*source.ResourceQuotaSpecRAMLimitMaxResult
	ResourceQuotaStatusUsedCPURequestAvg []*source.ResourceQuotaStatusUsedCPURequestAvgResult
	ResourceQuotaStatusUsedCPURequestMax []*source.ResourceQuotaStatusUsedCPURequestMaxResult
	ResourceQuotaStatusUsedRAMRequestAvg []*source.ResourceQuotaStatusUsedRAMRequestAvgResult
	ResourceQuotaStatusUsedRAMRequestMax []*source.ResourceQuotaStatusUsedRAMRequestMaxResult
	ResourceQuotaStatusUsedCPULimitAvg   []*source.ResourceQuotaStatusUsedCPULimitAvgResult
	ResourceQuotaStatusUsedCPULimitMax   []*source.ResourceQuotaStatusUsedCPULimitMaxResult
	ResourceQuotaStatusUsedRAMLimitAvg   []*source.ResourceQuotaStatusUsedRAMLimitAvgResult
	ResourceQuotaStatusUsedRAMLimitMax   []*source.ResourceQuotaStatusUsedRAMLimitMaxResult
}

type ClusterSnapshot struct {
	ClusterInfo *clusters.ClusterInfo
	Kubernetes  *KubernetesSnapshot
	NodeStats   *NodeStatsSummary
	Metrics     *MetricsSummary
}

// Emitter is a contract for an implementation which is directly sent cluster data snapshots on
// a regular interval.
type Emitter interface {
	// ID returns the identifier of the emitter for identification purposes.
	ID() EmitterID

	// Init is called with a `ClusterSnapshot` to pre-populate any emitter data before receiving
	// the regular interval snapshots from the exporter.
	Init(*ClusterSnapshot) error

	// Emit emits the `ClusterSnapshot` based on the emitter's implementation.
	Emit(context.Context, *ClusterSnapshot) error
}
