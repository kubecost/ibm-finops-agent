package opencost

import (
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/source"
)

//--------------------------------------------------------------------------
//  No-Op OpenCostDataSource
//--------------------------------------------------------------------------

// NoOpOpenCostDataSource
type NoOpOpenCostDataSource struct {
	MetricsQuerier      *NoOpMetricsQuerier
	ClusterInfoProvider *NoOpClusterInfoProvider
}

func NewNoOpOpenCostDataSource() *NoOpOpenCostDataSource {
	return &NoOpOpenCostDataSource{
		MetricsQuerier:      NewNoOpMetricsQuerier(),
		ClusterInfoProvider: NewNoOpClusterInfoProvider(),
	}
}

// RegisterEndPoints registers any custom endpoints that can be used for diagnostics or debug purposes.
func (mocds *NoOpOpenCostDataSource) RegisterEndPoints(router *httprouter.Router) {
	// No-op
}

func (mocds *NoOpOpenCostDataSource) RegisterDiagnostics(diagService diagnostics.DiagnosticService) {
	// No-op
}

// Metrics returns a MetricsQuerier that can be used to query historical metrics data from the data source.
func (mocds *NoOpOpenCostDataSource) Metrics() source.MetricsQuerier {
	return mocds.MetricsQuerier
}

// ClusterMap returns a mapping of cluster identifier to ClusterInfo for all known clusters (local only for
// single cluster deployments).
func (mocds *NoOpOpenCostDataSource) ClusterMap() clusters.ClusterMap {
	return nil
}

// ClusterInfo returns the ClusterInfoProvider for the local cluster.
func (mocds *NoOpOpenCostDataSource) ClusterInfo() clusters.ClusterInfoProvider {
	return mocds.ClusterInfoProvider
}

func (mocds *NoOpOpenCostDataSource) BatchDuration() time.Duration {
	return 24 * time.Hour
}

func (mocds *NoOpOpenCostDataSource) Resolution() time.Duration {
	return 5 * time.Minute
}

//--------------------------------------------------------------------------
//  No-Op Cluster Info Provider
//--------------------------------------------------------------------------

type NoOpClusterInfoProvider struct{}

func NewNoOpClusterInfoProvider() *NoOpClusterInfoProvider {
	return &NoOpClusterInfoProvider{}
}

func (m *NoOpClusterInfoProvider) GetClusterInfo() map[string]string {
	return map[string]string{
		clusters.ClusterInfoIdKey: "mock-cluster-id",
	}
}

//--------------------------------------------------------------------------
//  No-Op MetricsQuerier (empty query results)
//--------------------------------------------------------------------------

// NoOpMetricsQuerier is a no-op implementation of the source.MetricsQuerier interface
// that returns empty results for all queries.
type NoOpMetricsQuerier struct{}

// NewNoOpMetricsQuerier creates a new mock metrics querier
func NewNoOpMetricsQuerier() *NoOpMetricsQuerier {
	return &NoOpMetricsQuerier{}
}

// Implementation of interface methods
func (m *NoOpMetricsQuerier) QueryPVActiveMinutes(start, end time.Time) *source.Future[source.PVActiveMinutesResult] {
	return newEmptyResult(source.DecodePVActiveMinutesResult)
}

func (m *NoOpMetricsQuerier) QueryPVUsedAverage(start, end time.Time) *source.Future[source.PVUsedAvgResult] {
	return newEmptyResult(source.DecodePVUsedAvgResult)
}

func (m *NoOpMetricsQuerier) QueryPVUsedMax(start, end time.Time) *source.Future[source.PVUsedMaxResult] {
	return newEmptyResult(source.DecodePVUsedMaxResult)
}

func (m *NoOpMetricsQuerier) QueryLocalStorageActiveMinutes(start, end time.Time) *source.Future[source.LocalStorageActiveMinutesResult] {
	return newEmptyResult(source.DecodeLocalStorageActiveMinutesResult)
}

func (m *NoOpMetricsQuerier) QueryLocalStorageCost(start, end time.Time) *source.Future[source.LocalStorageCostResult] {
	return newEmptyResult(source.DecodeLocalStorageCostResult)
}

func (m *NoOpMetricsQuerier) QueryLocalStorageUsedCost(start, end time.Time) *source.Future[source.LocalStorageUsedCostResult] {
	return newEmptyResult(source.DecodeLocalStorageUsedCostResult)
}

func (m *NoOpMetricsQuerier) QueryLocalStorageUsedAvg(start, end time.Time) *source.Future[source.LocalStorageUsedAvgResult] {
	return newEmptyResult(source.DecodeLocalStorageUsedAvgResult)
}

func (m *NoOpMetricsQuerier) QueryLocalStorageUsedMax(start, end time.Time) *source.Future[source.LocalStorageUsedMaxResult] {
	return newEmptyResult(source.DecodeLocalStorageUsedMaxResult)
}

func (m *NoOpMetricsQuerier) QueryLocalStorageBytes(start, end time.Time) *source.Future[source.LocalStorageBytesResult] {
	return newEmptyResult(source.DecodeLocalStorageBytesResult)
}

func (m *NoOpMetricsQuerier) QueryNodeActiveMinutes(start, end time.Time) *source.Future[source.NodeActiveMinutesResult] {
	return newEmptyResult(source.DecodeNodeActiveMinutesResult)
}

func (m *NoOpMetricsQuerier) QueryNodeCPUCoresCapacity(start, end time.Time) *source.Future[source.NodeCPUCoresCapacityResult] {
	return newEmptyResult(source.DecodeNodeCPUCoresCapacityResult)
}

func (m *NoOpMetricsQuerier) QueryNodeCPUCoresAllocatable(start, end time.Time) *source.Future[source.NodeCPUCoresAllocatableResult] {
	return newEmptyResult(source.DecodeNodeCPUCoresAllocatableResult)
}

func (m *NoOpMetricsQuerier) QueryNodeRAMBytesCapacity(start, end time.Time) *source.Future[source.NodeRAMBytesCapacityResult] {
	return newEmptyResult(source.DecodeNodeRAMBytesCapacityResult)
}

func (m *NoOpMetricsQuerier) QueryNodeRAMBytesAllocatable(start, end time.Time) *source.Future[source.NodeRAMBytesAllocatableResult] {
	return newEmptyResult(source.DecodeNodeRAMBytesAllocatableResult)
}

func (m *NoOpMetricsQuerier) QueryNodeGPUCount(start, end time.Time) *source.Future[source.NodeGPUCountResult] {
	return newEmptyResult(source.DecodeNodeGPUCountResult)
}

func (m *NoOpMetricsQuerier) QueryNodeCPUModeTotal(start, end time.Time) *source.Future[source.NodeCPUModeTotalResult] {
	return newEmptyResult(source.DecodeNodeCPUModeTotalResult)
}

func (m *NoOpMetricsQuerier) QueryNodeIsSpot(start, end time.Time) *source.Future[source.NodeIsSpotResult] {
	return newEmptyResult(source.DecodeNodeIsSpotResult)
}

func (m *NoOpMetricsQuerier) QueryNodeRAMSystemPercent(start, end time.Time) *source.Future[source.NodeRAMSystemPercentResult] {
	return newEmptyResult(source.DecodeNodeRAMSystemPercentResult)
}

func (m *NoOpMetricsQuerier) QueryNodeRAMUserPercent(start, end time.Time) *source.Future[source.NodeRAMUserPercentResult] {
	return newEmptyResult(source.DecodeNodeRAMUserPercentResult)
}

func (m *NoOpMetricsQuerier) QueryLBActiveMinutes(start, end time.Time) *source.Future[source.LBActiveMinutesResult] {
	return newEmptyResult(source.DecodeLBActiveMinutesResult)
}

func (m *NoOpMetricsQuerier) QueryLBPricePerHr(start, end time.Time) *source.Future[source.LBPricePerHrResult] {
	return newEmptyResult(source.DecodeLBPricePerHrResult)
}

func (m *NoOpMetricsQuerier) QueryClusterUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return newEmptyResult(source.DecodeUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryClusterManagementDuration(start, end time.Time) *source.Future[source.ClusterManagementDurationResult] {
	return newEmptyResult(source.DecodeClusterManagementDurationResult)
}

func (m *NoOpMetricsQuerier) QueryClusterManagementPricePerHr(start, end time.Time) *source.Future[source.ClusterManagementPricePerHrResult] {
	return newEmptyResult(source.DecodeClusterManagementPricePerHrResult)
}

func (m *NoOpMetricsQuerier) QueryPods(start, end time.Time) *source.Future[source.PodsResult] {
	return newEmptyResult(source.DecodePodsResult)
}

func (m *NoOpMetricsQuerier) QueryPodsUID(start, end time.Time) *source.Future[source.PodsResult] {
	return newEmptyResult(source.DecodePodsResult)
}

func (m *NoOpMetricsQuerier) QueryRAMBytesAllocated(start, end time.Time) *source.Future[source.RAMBytesAllocatedResult] {
	return newEmptyResult(source.DecodeRAMBytesAllocatedResult)
}

func (m *NoOpMetricsQuerier) QueryRAMRequests(start, end time.Time) *source.Future[source.RAMRequestsResult] {
	return newEmptyResult(source.DecodeRAMRequestsResult)
}

func (m *NoOpMetricsQuerier) QueryRAMLimits(start, end time.Time) *source.Future[source.RAMLimitsResult] {
	return newEmptyResult(source.DecodeRAMLimitsResult)
}

func (m *NoOpMetricsQuerier) QueryRAMUsageAvg(start, end time.Time) *source.Future[source.RAMUsageAvgResult] {
	return newEmptyResult(source.DecodeRAMUsageAvgResult)
}

func (m *NoOpMetricsQuerier) QueryRAMUsageMax(start, end time.Time) *source.Future[source.RAMUsageMaxResult] {
	return newEmptyResult(source.DecodeRAMUsageMaxResult)
}

func (m *NoOpMetricsQuerier) QueryNodeRAMPricePerGiBHr(start, end time.Time) *source.Future[source.NodeRAMPricePerGiBHrResult] {
	return newEmptyResult(source.DecodeNodeRAMPricePerGiBHrResult)
}

func (m *NoOpMetricsQuerier) QueryCPUCoresAllocated(start, end time.Time) *source.Future[source.CPUCoresAllocatedResult] {
	return newEmptyResult(source.DecodeCPUCoresAllocatedResult)
}

func (m *NoOpMetricsQuerier) QueryCPURequests(start, end time.Time) *source.Future[source.CPURequestsResult] {
	return newEmptyResult(source.DecodeCPURequestsResult)
}

func (m *NoOpMetricsQuerier) QueryCPULimits(start, end time.Time) *source.Future[source.CPULimitsResult] {
	return newEmptyResult(source.DecodeCPULimitsResult)
}

func (m *NoOpMetricsQuerier) QueryCPUUsageAvg(start, end time.Time) *source.Future[source.CPUUsageAvgResult] {
	return newEmptyResult(source.DecodeCPUUsageAvgResult)
}

func (m *NoOpMetricsQuerier) QueryCPUUsageMax(start, end time.Time) *source.Future[source.CPUUsageMaxResult] {
	return newEmptyResult(source.DecodeCPUUsageMaxResult)
}

func (m *NoOpMetricsQuerier) QueryNodeCPUPricePerHr(start, end time.Time) *source.Future[source.NodeCPUPricePerHrResult] {
	return newEmptyResult(source.DecodeNodeCPUPricePerHrResult)
}

func (m *NoOpMetricsQuerier) QueryGPUsAllocated(start, end time.Time) *source.Future[source.GPUsAllocatedResult] {
	return newEmptyResult(source.DecodeGPUsAllocatedResult)
}

func (m *NoOpMetricsQuerier) QueryGPUsRequested(start, end time.Time) *source.Future[source.GPUsRequestedResult] {
	return newEmptyResult(source.DecodeGPUsRequestedResult)
}

func (m *NoOpMetricsQuerier) QueryGPUsUsageAvg(start, end time.Time) *source.Future[source.GPUsUsageAvgResult] {
	return newEmptyResult(source.DecodeGPUsUsageAvgResult)
}

func (m *NoOpMetricsQuerier) QueryGPUsUsageMax(start, end time.Time) *source.Future[source.GPUsUsageMaxResult] {
	return newEmptyResult(source.DecodeGPUsUsageMaxResult)
}

func (m *NoOpMetricsQuerier) QueryNodeGPUPricePerHr(start, end time.Time) *source.Future[source.NodeGPUPricePerHrResult] {
	return newEmptyResult(source.DecodeNodeGPUPricePerHrResult)
}

func (m *NoOpMetricsQuerier) QueryGPUInfo(start, end time.Time) *source.Future[source.GPUInfoResult] {
	return newEmptyResult(source.DecodeGPUInfoResult)
}

func (m *NoOpMetricsQuerier) QueryIsGPUShared(start, end time.Time) *source.Future[source.IsGPUSharedResult] {
	return newEmptyResult(source.DecodeIsGPUSharedResult)
}

func (m *NoOpMetricsQuerier) QueryPodPVCAllocation(start, end time.Time) *source.Future[source.PodPVCAllocationResult] {
	return newEmptyResult(source.DecodePodPVCAllocationResult)
}

func (m *NoOpMetricsQuerier) QueryPVCBytesRequested(start, end time.Time) *source.Future[source.PVCBytesRequestedResult] {
	return newEmptyResult(source.DecodePVCBytesRequestedResult)
}

func (m *NoOpMetricsQuerier) QueryPVCInfo(start, end time.Time) *source.Future[source.PVCInfoResult] {
	return newEmptyResult(source.DecodePVCInfoResult)
}

func (m *NoOpMetricsQuerier) QueryPVBytes(start, end time.Time) *source.Future[source.PVBytesResult] {
	return newEmptyResult(source.DecodePVBytesResult)
}

func (m *NoOpMetricsQuerier) QueryPVPricePerGiBHour(start, end time.Time) *source.Future[source.PVPricePerGiBHourResult] {
	return newEmptyResult(source.DecodePVPricePerGiBHourResult)
}

func (m *NoOpMetricsQuerier) QueryPVInfo(start, end time.Time) *source.Future[source.PVInfoResult] {
	return newEmptyResult(source.DecodePVInfoResult)
}

func (m *NoOpMetricsQuerier) QueryNetZoneGiB(start, end time.Time) *source.Future[source.NetZoneGiBResult] {
	return newEmptyResult(source.DecodeNetZoneGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetZonePricePerGiB(start, end time.Time) *source.Future[source.NetZonePricePerGiBResult] {
	return newEmptyResult(source.DecodeNetZonePricePerGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetRegionGiB(start, end time.Time) *source.Future[source.NetRegionGiBResult] {
	return newEmptyResult(source.DecodeNetRegionGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetRegionPricePerGiB(start, end time.Time) *source.Future[source.NetRegionPricePerGiBResult] {
	return newEmptyResult(source.DecodeNetRegionPricePerGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetInternetGiB(start, end time.Time) *source.Future[source.NetInternetGiBResult] {
	return newEmptyResult(source.DecodeNetInternetGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetInternetPricePerGiB(start, end time.Time) *source.Future[source.NetInternetPricePerGiBResult] {
	return newEmptyResult(source.DecodeNetInternetPricePerGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetInternetServiceGiB(start, end time.Time) *source.Future[source.NetInternetServiceGiBResult] {
	return newEmptyResult(source.DecodeNetInternetServiceGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetNatGatewayGiB(start, end time.Time) *source.Future[source.NetNatGatewayGiBResult] {
	return newEmptyResult(source.DecodeNetNatGatewayGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetNatGatewayPricePerGiB(start, end time.Time) *source.Future[source.NetNatGatewayPricePerGiBResult] {
	return newEmptyResult(source.DecodeNetNatGatewayPricePerGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetTransferBytes(start, end time.Time) *source.Future[source.NetTransferBytesResult] {
	return newEmptyResult(source.DecodeNetTransferBytesResult)
}

func (m *NoOpMetricsQuerier) QueryNetZoneIngressGiB(start, end time.Time) *source.Future[source.NetZoneIngressGiBResult] {
	return newEmptyResult(source.DecodeNetZoneIngressGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetRegionIngressGiB(start, end time.Time) *source.Future[source.NetRegionIngressGiBResult] {
	return newEmptyResult(source.DecodeNetRegionIngressGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetInternetIngressGiB(start, end time.Time) *source.Future[source.NetInternetIngressGiBResult] {
	return newEmptyResult(source.DecodeNetInternetIngressGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetInternetServiceIngressGiB(start, end time.Time) *source.Future[source.NetInternetServiceIngressGiBResult] {
	return newEmptyResult(source.DecodeNetInternetServiceIngressGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetNatGatewayIngressGiB(start, end time.Time) *source.Future[source.NetNatGatewayIngressGiBResult] {
	return newEmptyResult(source.DecodeNetNatGatewayIngressGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetNatGatewayIngressPricePerGiB(start, end time.Time) *source.Future[source.NetNatGatewayPricePerGiBResult] {
	return newEmptyResult(source.DecodeNetNatGatewayPricePerGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetReceiveBytes(start, end time.Time) *source.Future[source.NetReceiveBytesResult] {
	return newEmptyResult(source.DecodeNetReceiveBytesResult)
}

func (m *NoOpMetricsQuerier) QueryNamespaceUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return newEmptyResult(source.DecodeUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryNamespaceAnnotations(start, end time.Time) *source.Future[source.NamespaceAnnotationsResult] {
	return newEmptyResult(source.DecodeNamespaceAnnotationsResult)
}

func (m *NoOpMetricsQuerier) QueryPodAnnotations(start, end time.Time) *source.Future[source.PodAnnotationsResult] {
	return newEmptyResult(source.DecodePodAnnotationsResult)
}

func (m *NoOpMetricsQuerier) QueryNodeLabels(start, end time.Time) *source.Future[source.NodeLabelsResult] {
	return newEmptyResult(source.DecodeNodeLabelsResult)
}

func (m *NoOpMetricsQuerier) QueryNamespaceLabels(start, end time.Time) *source.Future[source.NamespaceLabelsResult] {
	return newEmptyResult(source.DecodeNamespaceLabelsResult)
}

func (m *NoOpMetricsQuerier) QueryPodLabels(start, end time.Time) *source.Future[source.PodLabelsResult] {
	return newEmptyResult(source.DecodePodLabelsResult)
}

func (m *NoOpMetricsQuerier) QueryServiceLabels(start, end time.Time) *source.Future[source.ServiceLabelsResult] {
	return newEmptyResult(source.DecodeServiceLabelsResult)
}

func (m *NoOpMetricsQuerier) QueryDeploymentLabels(start, end time.Time) *source.Future[source.LabelsResult] {
	return newEmptyResult(source.DecodeLabelsResult)
}

func (m *NoOpMetricsQuerier) QueryStatefulSetLabels(start, end time.Time) *source.Future[source.LabelsResult] {
	return newEmptyResult(source.DecodeLabelsResult)
}

func (m *NoOpMetricsQuerier) QueryDaemonSetLabels(start, end time.Time) *source.Future[source.LabelsResult] {
	return newEmptyResult(source.DecodeLabelsResult)
}

func (m *NoOpMetricsQuerier) QueryJobLabels(start, end time.Time) *source.Future[source.LabelsResult] {
	return newEmptyResult(source.DecodeLabelsResult)
}

func (m *NoOpMetricsQuerier) QueryPodsWithReplicaSetOwner(start, end time.Time) *source.Future[source.PodsWithReplicaSetOwnerResult] {
	return newEmptyResult(source.DecodePodsWithReplicaSetOwnerResult)
}

func (m *NoOpMetricsQuerier) QueryReplicaSetsWithoutOwners(start, end time.Time) *source.Future[source.ReplicaSetsWithoutOwnersResult] {
	return newEmptyResult(source.DecodeReplicaSetsWithoutOwnersResult)
}

func (m *NoOpMetricsQuerier) QueryReplicaSetsWithRollout(start, end time.Time) *source.Future[source.ReplicaSetsWithRolloutResult] {
	return newEmptyResult(source.DecodeReplicaSetsWithRolloutResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return newEmptyResult(source.DecodeUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaSpecCPURequestAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	return newEmptyResult(source.DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaSpecCPURequestMax(start, end time.Time) *source.Future[source.ResourceResult] {
	return newEmptyResult(source.DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaSpecRAMRequestAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	return newEmptyResult(source.DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaSpecRAMRequestMax(start, end time.Time) *source.Future[source.ResourceResult] {
	return newEmptyResult(source.DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaSpecCPULimitAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	return newEmptyResult(source.DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaSpecCPULimitMax(start, end time.Time) *source.Future[source.ResourceResult] {
	return newEmptyResult(source.DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaSpecRAMLimitAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	return newEmptyResult(source.DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaSpecRAMLimitMax(start, end time.Time) *source.Future[source.ResourceResult] {
	return newEmptyResult(source.DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaStatusUsedCPURequestAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	return newEmptyResult(source.DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaStatusUsedCPURequestMax(start, end time.Time) *source.Future[source.ResourceResult] {
	return newEmptyResult(source.DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaStatusUsedRAMRequestAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	return newEmptyResult(source.DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaStatusUsedRAMRequestMax(start, end time.Time) *source.Future[source.ResourceResult] {
	return newEmptyResult(source.DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaStatusUsedCPULimitAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	return newEmptyResult(source.DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaStatusUsedCPULimitMax(start, end time.Time) *source.Future[source.ResourceResult] {
	return newEmptyResult(source.DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaStatusUsedRAMLimitAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	return newEmptyResult(source.DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaStatusUsedRAMLimitMax(start, end time.Time) *source.Future[source.ResourceResult] {
	return newEmptyResult(source.DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryNodeInfo(start, end time.Time) *source.Future[source.NodeInfoResult] {
	return newEmptyResult(source.DecodeNodeInfoResult)
}

func (m *NoOpMetricsQuerier) QueryNodeUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return newEmptyResult(source.DecodeUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryNodeResourceCapacities(start, end time.Time) *source.Future[source.ResourceResult] {
	return newEmptyResult(source.DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryNodeResourcesAllocatable(start, end time.Time) *source.Future[source.ResourceResult] {
	return newEmptyResult(source.DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryClusterInfo(start, end time.Time) *source.Future[source.ClusterInfoResult] {
	return newEmptyResult(source.DecodeClusterInfoResult)
}

func (m *NoOpMetricsQuerier) QueryPodInfo(start, end time.Time) *source.Future[source.PodInfoResult] {
	return newEmptyResult(source.DecodePodInfoResult)
}

func (m *NoOpMetricsQuerier) QueryPodUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return newEmptyResult(source.DecodeUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryPodOwners(start, end time.Time) *source.Future[source.OwnerResult] {
	return newEmptyResult(source.DecodeOwnerResult)
}

func (m *NoOpMetricsQuerier) QueryPodPVCVolumes(start, end time.Time) *source.Future[source.PodPVCVolumeResult] {
	return newEmptyResult(source.DecodePodPVCVolumeResult)
}

func (m *NoOpMetricsQuerier) QueryPodNetworkEgressBytes(start, end time.Time) *source.Future[source.PodNetworkBytesResult] {
	return newEmptyResult(source.DecodePodNetworkBytesResult)
}

func (m *NoOpMetricsQuerier) QueryPodNetworkIngressBytes(start, end time.Time) *source.Future[source.PodNetworkBytesResult] {
	return newEmptyResult(source.DecodePodNetworkBytesResult)
}

func (m *NoOpMetricsQuerier) QueryContainerUptime(start, end time.Time) *source.Future[source.ContainerUptimeResult] {
	return newEmptyResult(source.DecodeContainerUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryContainerResourceRequests(start, end time.Time) *source.Future[source.ContainerResourceResult] {
	return newEmptyResult(source.DecodeContainerResourceResult)
}

func (m *NoOpMetricsQuerier) QueryContainerResourceLimits(start, end time.Time) *source.Future[source.ContainerResourceResult] {
	return newEmptyResult(source.DecodeContainerResourceResult)
}

func (m *NoOpMetricsQuerier) QueryDCGMDeviceInfo(start, end time.Time) *source.Future[source.DCGMDeviceInfoResult] {
	return newEmptyResult(source.DecodeDCGMDeviceInfoResult)
}

func (m *NoOpMetricsQuerier) QueryDCGMDeviceUptime(start, end time.Time) *source.Future[source.DCGMDeviceUptimeResult] {
	return newEmptyResult(source.DecodeDCGMDeviceUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryDCGMContainerUsageAvg(start, end time.Time) *source.Future[source.DCGMDeviceContainerUsageResult] {
	return newEmptyResult(source.DecodeDCGMDeviceContainerUsageResult)
}

func (m *NoOpMetricsQuerier) QueryDCGMContainerUsageMax(start, end time.Time) *source.Future[source.DCGMDeviceContainerUsageResult] {
	return newEmptyResult(source.DecodeDCGMDeviceContainerUsageResult)
}

func (m *NoOpMetricsQuerier) QueryPVCUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return newEmptyResult(source.DecodeUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryPVUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return newEmptyResult(source.DecodeUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryDeploymentInfo(start, end time.Time) *source.Future[source.DeploymentInfoResult] {
	return newEmptyResult(source.DecodeDeploymentInfoResult)
}

func (m *NoOpMetricsQuerier) QueryDeploymentUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return newEmptyResult(source.DecodeUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryDeploymentAnnotations(start, end time.Time) *source.Future[source.AnnotationsResult] {
	return newEmptyResult(source.DecodeAnnotationsResult)
}

func (m *NoOpMetricsQuerier) QueryDeploymentMatchLabels(start, end time.Time) *source.Future[source.DeploymentLabelsResult] {
	return newEmptyResult(source.DecodeDeploymentLabelsResult)
}

func (m *NoOpMetricsQuerier) QueryStatefulSetInfo(start, end time.Time) *source.Future[source.StatefulSetInfoResult] {
	return newEmptyResult(source.DecodeStatefulSetInfoResult)
}

func (m *NoOpMetricsQuerier) QueryStatefulSetUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return newEmptyResult(source.DecodeUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryStatefulSetAnnotations(start, end time.Time) *source.Future[source.AnnotationsResult] {
	return newEmptyResult(source.DecodeAnnotationsResult)
}

func (m *NoOpMetricsQuerier) QueryStatefulSetMatchLabels(start, end time.Time) *source.Future[source.StatefulSetLabelsResult] {
	return newEmptyResult(source.DecodeStatefulSetLabelsResult)
}

func (m *NoOpMetricsQuerier) QueryDaemonSetInfo(start, end time.Time) *source.Future[source.DaemonSetInfoResult] {
	return newEmptyResult(source.DecodeDaemonSetInfoResult)
}

func (m *NoOpMetricsQuerier) QueryDaemonSetUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return newEmptyResult(source.DecodeUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryDaemonSetAnnotations(start, end time.Time) *source.Future[source.AnnotationsResult] {
	return newEmptyResult(source.DecodeAnnotationsResult)
}

func (m *NoOpMetricsQuerier) QueryJobInfo(start, end time.Time) *source.Future[source.JobInfoResult] {
	return newEmptyResult(source.DecodeJobInfoResult)
}

func (m *NoOpMetricsQuerier) QueryJobUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return newEmptyResult(source.DecodeUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryJobAnnotations(start, end time.Time) *source.Future[source.AnnotationsResult] {
	return newEmptyResult(source.DecodeAnnotationsResult)
}

func (m *NoOpMetricsQuerier) QueryCronJobInfo(start, end time.Time) *source.Future[source.CronJobInfoResult] {
	return newEmptyResult(source.DecodeCronJobInfoResult)
}

func (m *NoOpMetricsQuerier) QueryCronJobUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return newEmptyResult(source.DecodeUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryCronJobLabels(start, end time.Time) *source.Future[source.LabelsResult] {
	return newEmptyResult(source.DecodeLabelsResult)
}

func (m *NoOpMetricsQuerier) QueryCronJobAnnotations(start, end time.Time) *source.Future[source.AnnotationsResult] {
	return newEmptyResult(source.DecodeAnnotationsResult)
}

func (m *NoOpMetricsQuerier) QueryReplicaSetInfo(start, end time.Time) *source.Future[source.ReplicaSetInfoResult] {
	return newEmptyResult(source.DecodeReplicaSetInfoResult)
}

func (m *NoOpMetricsQuerier) QueryReplicaSetUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return newEmptyResult(source.DecodeUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryReplicaSetLabels(start, end time.Time) *source.Future[source.LabelsResult] {
	return newEmptyResult(source.DecodeLabelsResult)
}

func (m *NoOpMetricsQuerier) QueryReplicaSetAnnotations(start, end time.Time) *source.Future[source.AnnotationsResult] {
	return newEmptyResult(source.DecodeAnnotationsResult)
}

func (m *NoOpMetricsQuerier) QueryReplicaSetOwners(start, end time.Time) *source.Future[source.OwnerResult] {
	return newEmptyResult(source.DecodeOwnerResult)
}

func (m *NoOpMetricsQuerier) QueryNamespaceInfo(start, end time.Time) *source.Future[source.NamespaceInfoResult] {
	return newEmptyResult(source.DecodeNamespaceInfoResult)
}

func (m *NoOpMetricsQuerier) QueryServiceInfo(start, end time.Time) *source.Future[source.ServiceInfoResult] {
	return newEmptyResult(source.DecodeServiceInfoResult)
}

func (m *NoOpMetricsQuerier) QueryServiceUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return newEmptyResult(source.DecodeUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryServiceSelectorLabels(start, end time.Time) *source.Future[source.ServiceLabelsResult] {
	return newEmptyResult(source.DecodeServiceLabelsResult)
}

func (m *NoOpMetricsQuerier) QueryPodsWithDaemonSetOwner(start, end time.Time) *source.Future[source.PodsWithDaemonSetOwnerResult] {
	return newEmptyResult(source.DecodePodsWithDaemonSetOwnerResult)
}

func (m *NoOpMetricsQuerier) QueryPodsWithJobOwner(start, end time.Time) *source.Future[source.PodsWithJobOwnerResult] {
	return newEmptyResult(source.DecodePodsWithJobOwnerResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaInfo(start, end time.Time) *source.Future[source.ResourceQuotaInfoResult] {
	return newEmptyResult(source.DecodeResourceQuotaInfoResult)
}

func (m *NoOpMetricsQuerier) QueryDataCoverage(_ int) (time.Time, time.Time, error) {
	return time.Time{}, time.Time{}, nil
}

func newEmptyResult[T any](decoder source.ResultDecoder[T]) *source.Future[T] {
	ch := make(source.QueryResultsChan)
	go func() {
		results := source.NewQueryResults("")
		ch <- results
	}()

	return source.NewFuture(decoder, ch)
}
