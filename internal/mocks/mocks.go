package mocks

import (
	"sync"
	"time"

	"github.com/ibm/finops-agent/pkg/cluster"
	"github.com/ibm/finops-agent/pkg/nodes"
	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/pkg/cloud/models"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	stv1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

//--------------------------------------------------------------------------
//  Mock ClusterCache (empty)
//--------------------------------------------------------------------------

// MockDataSource contains mock implementations of the interfaces returned by the data source.
type MockDataSource struct {
	OCDataSource           *MockOpenCostDataSource
	OCCloudProvider        models.Provider
	ClusterCache           *MockClusterCache
	MetricsQuerier         *MockMetricsQuerier
	NodeStatsSummaryClient *MockStatsSummaryClient
	CMetadata              *MockMetadata
}

// NewMockDataSource creates a new mock data source implementation with services that track
// method calls only (empty responses).
func NewMockDataSource() *MockDataSource {
	ocDataSource := NewMockOpenCostDataSource()
	metrics := ocDataSource.Metrics().(*MockMetricsQuerier)
	return &MockDataSource{
		OCDataSource:           ocDataSource,
		OCCloudProvider:        nil,
		ClusterCache:           NewMockClusterCache(),
		MetricsQuerier:         metrics,
		NodeStatsSummaryClient: NewMockStatsSummaryClient(),
		CMetadata:              NewMockMetadata(),
	}
}

func (mds *MockDataSource) OpenCostSource() source.OpenCostDataSource {
	return mds.OCDataSource
}

func (mds *MockDataSource) OpenCostCloudCostProvider() models.Provider {
	return mds.OCCloudProvider
}

func (mds *MockDataSource) Cluster() cluster.ClusterCache {
	return mds.ClusterCache
}

func (mds *MockDataSource) Metrics() source.MetricsQuerier {
	return mds.MetricsQuerier
}

func (mds *MockDataSource) StatsSummary() nodes.StatSummaryClient {
	return mds.NodeStatsSummaryClient
}

func (mds *MockDataSource) ClusterMetadata() cluster.Metadata {
	return mds.CMetadata
}

//--------------------------------------------------------------------------
//  Mock OpenCostDataSource (cluster info provider)
//--------------------------------------------------------------------------

type MockClusterInfoProvider struct{}

func (m *MockClusterInfoProvider) GetClusterInfo() map[string]string {
	return map[string]string{
		clusters.ClusterInfoIdKey: "mock-cluster-id",
	}
}

// MockOpenCostDataSource
type MockOpenCostDataSource struct {
	MetricsQuerier      *MockMetricsQuerier
	ClusterInfoProvider *MockClusterInfoProvider
}

func NewMockOpenCostDataSource() *MockOpenCostDataSource {
	return &MockOpenCostDataSource{
		MetricsQuerier:      NewMockMetricsQuerier(),
		ClusterInfoProvider: &MockClusterInfoProvider{},
	}
}

// RegisterEndPoints registers any custom endpoints that can be used for diagnostics or debug purposes.
func (mocds *MockOpenCostDataSource) RegisterEndPoints(router *httprouter.Router) {
	// No-op
}

func (mocds *MockOpenCostDataSource) RegisterDiagnostics(diag diagnostics.DiagnosticService) {
	// No-op
}

// Metrics returns a MetricsQuerier that can be used to query historical metrics data from the data source.
func (mocds *MockOpenCostDataSource) Metrics() source.MetricsQuerier {
	return mocds.MetricsQuerier
}

// ClusterMap returns a mapping of cluster identifier to ClusterInfo for all known clusters (local only for
// single cluster deployments).
func (mocds *MockOpenCostDataSource) ClusterMap() clusters.ClusterMap {
	return nil
}

// ClusterInfo returns the ClusterInfoProvider for the local cluster.
func (mocds *MockOpenCostDataSource) ClusterInfo() clusters.ClusterInfoProvider {
	return mocds.ClusterInfoProvider
}

func (mocds *MockOpenCostDataSource) BatchDuration() time.Duration {
	return 24 * time.Hour
}

func (mocds *MockOpenCostDataSource) Resolution() time.Duration {
	return 5 * time.Minute
}

//--------------------------------------------------------------------------
//  Mock ClusterCache (empty resources)
//--------------------------------------------------------------------------

// MockClusterCache implements the ClusterCache interface for testing
type MockClusterCache struct {
	// Track method calls
	mu    sync.Mutex
	Calls map[string]int

	// Mock data to return
	Namespaces             []*v1.Namespace
	Nodes                  []*v1.Node
	Pods                   []*v1.Pod
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
	ResourceQuotas         []*v1.ResourceQuota
	UnstructuredObjects    map[schema.GroupVersionResource][]*unstructured.Unstructured
}

// NewMockClusterCache creates a new mock cluster cache
func NewMockClusterCache() *MockClusterCache {
	return &MockClusterCache{
		Calls:               make(map[string]int),
		UnstructuredObjects: make(map[schema.GroupVersionResource][]*unstructured.Unstructured),
	}
}

// Helper to record method calls
func (m *MockClusterCache) recordCall(method string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls[method]++
}

func (m *MockClusterCache) GetCalls(method string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.Calls[method]
}

// Start implements ClusterCache interface
func (m *MockClusterCache) Start(stopCh <-chan struct{}) {
	m.recordCall("Start")
}

// Shutdown implements ClusterCache interface
func (m *MockClusterCache) Shutdown() {
	m.recordCall("Shutdown")
}

// GetAllNamespaces implements ClusterCache interface
func (m *MockClusterCache) GetAllNamespaces() []*v1.Namespace {
	m.recordCall("GetAllNamespaces")
	return m.Namespaces
}

// GetAllNodes implements ClusterCache interface
func (m *MockClusterCache) GetAllNodes() []*v1.Node {
	m.recordCall("GetAllNodes")
	return m.Nodes
}

// GetAllPods implements ClusterCache interface
func (m *MockClusterCache) GetAllPods() []*v1.Pod {
	m.recordCall("GetAllPods")
	return m.Pods
}

// GetAllShortLivedPods implements ClusterCache interface
func (m *MockClusterCache) GetAllShortLivedPods() []*v1.Pod {
	m.recordCall("GetAllShortLivedPods")
	return m.Pods
}

// AcknowledgeShortLivedPods implements ClusterCache interface
func (m *MockClusterCache) AcknowledgeShortLivedPods() {
	m.recordCall("AcknowledgeShortLivedPods")
}

// GetAllServices implements ClusterCache interface
func (m *MockClusterCache) GetAllServices() []*v1.Service {
	m.recordCall("GetAllServices")
	return m.Services
}

// GetAllDaemonSets implements ClusterCache interface
func (m *MockClusterCache) GetAllDaemonSets() []*appsv1.DaemonSet {
	m.recordCall("GetAllDaemonSets")
	return m.DaemonSets
}

// GetAllDeployments implements ClusterCache interface
func (m *MockClusterCache) GetAllDeployments() []*appsv1.Deployment {
	m.recordCall("GetAllDeployments")
	return m.Deployments
}

// GetAllStatefulSets implements ClusterCache interface
func (m *MockClusterCache) GetAllStatefulSets() []*appsv1.StatefulSet {
	m.recordCall("GetAllStatefulSets")
	return m.StatefulSets
}

// GetAllReplicaSets implements ClusterCache interface
func (m *MockClusterCache) GetAllReplicaSets() []*appsv1.ReplicaSet {
	m.recordCall("GetAllReplicaSets")
	return m.ReplicaSets
}

// GetAllPersistentVolumes implements ClusterCache interface
func (m *MockClusterCache) GetAllPersistentVolumes() []*v1.PersistentVolume {
	m.recordCall("GetAllPersistentVolumes")
	return m.PersistentVolumes
}

// GetAllPersistentVolumeClaims implements ClusterCache interface
func (m *MockClusterCache) GetAllPersistentVolumeClaims() []*v1.PersistentVolumeClaim {
	m.recordCall("GetAllPersistentVolumeClaims")
	return m.PersistentVolumeClaims
}

// GetAllStorageClasses implements ClusterCache interface
func (m *MockClusterCache) GetAllStorageClasses() []*stv1.StorageClass {
	m.recordCall("GetAllStorageClasses")
	return m.StorageClasses
}

// GetAllJobs implements ClusterCache interface
func (m *MockClusterCache) GetAllJobs() []*batchv1.Job {
	m.recordCall("GetAllJobs")
	return m.Jobs
}

// GetAllPodDisruptionBudgets implements ClusterCache interface
func (m *MockClusterCache) GetAllPodDisruptionBudgets() []*policyv1.PodDisruptionBudget {
	m.recordCall("GetAllPodDisruptionBudgets")
	return m.PodDisruptionBudgets
}

// GetAllReplicationControllers implements ClusterCache interface
func (m *MockClusterCache) GetAllReplicationControllers() []*v1.ReplicationController {
	m.recordCall("GetAllReplicationControllers")
	return m.ReplicationControllers
}

// GetAllResourceQuotas implements ClusterCache interface
func (m *MockClusterCache) GetAllResourceQuotas() []*v1.ResourceQuota {
	m.recordCall("GetAllResourceQuotas")
	return m.ResourceQuotas
}

// ListUnstructuredByGroupVersionResource implements ClusterCache interface
func (m *MockClusterCache) ListUnstructuredByGroupVersionResource(gvr schema.GroupVersionResource) []*unstructured.Unstructured {
	m.recordCall("ListUnstructuredByGroupVersionResource")
	return m.UnstructuredObjects[gvr]
}

//--------------------------------------------------------------------------
//  Mock MetricsQuerier (empty query results)
//--------------------------------------------------------------------------

// MockMetricsQuerier is a mock implementation of the source.MetricsQuerier interface
// that records the number of times each method is called. It returns empty results for all queries.
type MockMetricsQuerier struct {
	Calls map[string]int
}

// NewMockMetricsQuerier creates a new mock metrics querier
func NewMockMetricsQuerier() *MockMetricsQuerier {
	return &MockMetricsQuerier{
		Calls: make(map[string]int),
	}
}

// Helper to record method calls
func (m *MockMetricsQuerier) recordCall(method string) {
	m.Calls[method]++
}

// Implementation of interface methods
func (m *MockMetricsQuerier) QueryPVActiveMinutes(start, end time.Time) *source.Future[source.PVActiveMinutesResult] {
	m.recordCall("QueryPVActiveMinutes")
	return newEmptyResult(source.DecodePVActiveMinutesResult)
}

func (m *MockMetricsQuerier) QueryPVUsedAverage(start, end time.Time) *source.Future[source.PVUsedAvgResult] {
	m.recordCall("QueryPVUsedAverage")
	return newEmptyResult(source.DecodePVUsedAvgResult)
}

func (m *MockMetricsQuerier) QueryPVUsedMax(start, end time.Time) *source.Future[source.PVUsedMaxResult] {
	m.recordCall("QueryPVUsedMax")
	return newEmptyResult(source.DecodePVUsedMaxResult)
}

func (m *MockMetricsQuerier) QueryLocalStorageActiveMinutes(start, end time.Time) *source.Future[source.LocalStorageActiveMinutesResult] {
	m.recordCall("QueryLocalStorageActiveMinutes")
	return newEmptyResult(source.DecodeLocalStorageActiveMinutesResult)
}

func (m *MockMetricsQuerier) QueryLocalStorageCost(start, end time.Time) *source.Future[source.LocalStorageCostResult] {
	m.recordCall("QueryLocalStorageCost")
	return newEmptyResult(source.DecodeLocalStorageCostResult)
}

func (m *MockMetricsQuerier) QueryLocalStorageUsedCost(start, end time.Time) *source.Future[source.LocalStorageUsedCostResult] {
	m.recordCall("QueryLocalStorageUsedCost")
	return newEmptyResult(source.DecodeLocalStorageUsedCostResult)
}

func (m *MockMetricsQuerier) QueryLocalStorageUsedAvg(start, end time.Time) *source.Future[source.LocalStorageUsedAvgResult] {
	m.recordCall("QueryLocalStorageUsedAvg")
	return newEmptyResult(source.DecodeLocalStorageUsedAvgResult)
}

func (m *MockMetricsQuerier) QueryLocalStorageUsedMax(start, end time.Time) *source.Future[source.LocalStorageUsedMaxResult] {
	m.recordCall("QueryLocalStorageUsedMax")
	return newEmptyResult(source.DecodeLocalStorageUsedMaxResult)
}

func (m *MockMetricsQuerier) QueryLocalStorageBytes(start, end time.Time) *source.Future[source.LocalStorageBytesResult] {
	m.recordCall("QueryLocalStorageBytes")
	return newEmptyResult(source.DecodeLocalStorageBytesResult)
}

func (m *MockMetricsQuerier) QueryNodeActiveMinutes(start, end time.Time) *source.Future[source.NodeActiveMinutesResult] {
	m.recordCall("QueryNodeActiveMinutes")
	return newEmptyResult(source.DecodeNodeActiveMinutesResult)
}

func (m *MockMetricsQuerier) QueryNodeCPUCoresCapacity(start, end time.Time) *source.Future[source.NodeCPUCoresCapacityResult] {
	m.recordCall("QueryNodeCPUCoresCapacity")
	return newEmptyResult(source.DecodeNodeCPUCoresCapacityResult)
}

func (m *MockMetricsQuerier) QueryNodeCPUCoresAllocatable(start, end time.Time) *source.Future[source.NodeCPUCoresAllocatableResult] {
	m.recordCall("QueryNodeCPUCoresAllocatable")
	return newEmptyResult(source.DecodeNodeCPUCoresAllocatableResult)
}

func (m *MockMetricsQuerier) QueryNodeRAMBytesCapacity(start, end time.Time) *source.Future[source.NodeRAMBytesCapacityResult] {
	m.recordCall("QueryNodeRAMBytesCapacity")
	return newEmptyResult(source.DecodeNodeRAMBytesCapacityResult)
}

func (m *MockMetricsQuerier) QueryNodeRAMBytesAllocatable(start, end time.Time) *source.Future[source.NodeRAMBytesAllocatableResult] {
	m.recordCall("QueryNodeRAMBytesAllocatable")
	return newEmptyResult(source.DecodeNodeRAMBytesAllocatableResult)
}

func (m *MockMetricsQuerier) QueryNodeGPUCount(start, end time.Time) *source.Future[source.NodeGPUCountResult] {
	m.recordCall("QueryNodeGPUCount")
	return newEmptyResult(source.DecodeNodeGPUCountResult)
}

func (m *MockMetricsQuerier) QueryNodeCPUModeTotal(start, end time.Time) *source.Future[source.NodeCPUModeTotalResult] {
	m.recordCall("QueryNodeCPUModeTotal")
	return newEmptyResult(source.DecodeNodeCPUModeTotalResult)
}

func (m *MockMetricsQuerier) QueryNodeIsSpot(start, end time.Time) *source.Future[source.NodeIsSpotResult] {
	m.recordCall("QueryNodeIsSpot")
	return newEmptyResult(source.DecodeNodeIsSpotResult)
}

func (m *MockMetricsQuerier) QueryNodeRAMSystemPercent(start, end time.Time) *source.Future[source.NodeRAMSystemPercentResult] {
	m.recordCall("QueryNodeRAMSystemPercent")
	return newEmptyResult(source.DecodeNodeRAMSystemPercentResult)
}

func (m *MockMetricsQuerier) QueryNodeRAMUserPercent(start, end time.Time) *source.Future[source.NodeRAMUserPercentResult] {
	m.recordCall("QueryNodeRAMUserPercent")
	return newEmptyResult(source.DecodeNodeRAMUserPercentResult)
}

func (m *MockMetricsQuerier) QueryLBActiveMinutes(start, end time.Time) *source.Future[source.LBActiveMinutesResult] {
	m.recordCall("QueryLBActiveMinutes")
	return newEmptyResult(source.DecodeLBActiveMinutesResult)
}

func (m *MockMetricsQuerier) QueryLBPricePerHr(start, end time.Time) *source.Future[source.LBPricePerHrResult] {
	m.recordCall("QueryLBPricePerHr")
	return newEmptyResult(source.DecodeLBPricePerHrResult)
}

func (m *MockMetricsQuerier) QueryClusterUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	m.recordCall("QueryClusterUptime")
	return newEmptyResult(source.DecodeUptimeResult)
}

func (m *MockMetricsQuerier) QueryClusterManagementDuration(start, end time.Time) *source.Future[source.ClusterManagementDurationResult] {
	m.recordCall("QueryClusterManagementDuration")
	return newEmptyResult(source.DecodeClusterManagementDurationResult)
}

func (m *MockMetricsQuerier) QueryClusterManagementPricePerHr(start, end time.Time) *source.Future[source.ClusterManagementPricePerHrResult] {
	m.recordCall("QueryClusterManagementPricePerHr")
	return newEmptyResult(source.DecodeClusterManagementPricePerHrResult)
}

func (m *MockMetricsQuerier) QueryPods(start, end time.Time) *source.Future[source.PodsResult] {
	m.recordCall("QueryPods")
	return newEmptyResult(source.DecodePodsResult)
}

func (m *MockMetricsQuerier) QueryPodsUID(start, end time.Time) *source.Future[source.PodsResult] {
	m.recordCall("QueryPodsUID")
	return newEmptyResult(source.DecodePodsResult)
}

func (m *MockMetricsQuerier) QueryRAMBytesAllocated(start, end time.Time) *source.Future[source.RAMBytesAllocatedResult] {
	m.recordCall("QueryRAMBytesAllocated")
	return newEmptyResult(source.DecodeRAMBytesAllocatedResult)
}

func (m *MockMetricsQuerier) QueryRAMRequests(start, end time.Time) *source.Future[source.RAMRequestsResult] {
	m.recordCall("QueryRAMRequests")
	return newEmptyResult(source.DecodeRAMRequestsResult)
}

func (m *MockMetricsQuerier) QueryRAMLimits(start, end time.Time) *source.Future[source.RAMLimitsResult] {
	m.recordCall("QueryRAMLimits")
	return newEmptyResult(source.DecodeRAMLimitsResult)
}

func (m *MockMetricsQuerier) QueryRAMUsageAvg(start, end time.Time) *source.Future[source.RAMUsageAvgResult] {
	m.recordCall("QueryRAMUsageAvg")
	return newEmptyResult(source.DecodeRAMUsageAvgResult)
}

func (m *MockMetricsQuerier) QueryRAMUsageMax(start, end time.Time) *source.Future[source.RAMUsageMaxResult] {
	m.recordCall("QueryRAMUsageMax")
	return newEmptyResult(source.DecodeRAMUsageMaxResult)
}

func (m *MockMetricsQuerier) QueryNodeRAMPricePerGiBHr(start, end time.Time) *source.Future[source.NodeRAMPricePerGiBHrResult] {
	m.recordCall("QueryNodeRAMPricePerGiBHr")
	return newEmptyResult(source.DecodeNodeRAMPricePerGiBHrResult)
}

func (m *MockMetricsQuerier) QueryCPUCoresAllocated(start, end time.Time) *source.Future[source.CPUCoresAllocatedResult] {
	m.recordCall("QueryCPUCoresAllocated")
	return newEmptyResult(source.DecodeCPUCoresAllocatedResult)
}

func (m *MockMetricsQuerier) QueryCPURequests(start, end time.Time) *source.Future[source.CPURequestsResult] {
	m.recordCall("QueryCPURequests")
	return newEmptyResult(source.DecodeCPURequestsResult)
}

func (m *MockMetricsQuerier) QueryCPULimits(start, end time.Time) *source.Future[source.CPULimitsResult] {
	m.recordCall("QueryCPULimits")
	return newEmptyResult(source.DecodeCPULimitsResult)
}

func (m *MockMetricsQuerier) QueryCPUUsageAvg(start, end time.Time) *source.Future[source.CPUUsageAvgResult] {
	m.recordCall("QueryCPUUsageAvg")
	return newEmptyResult(source.DecodeCPUUsageAvgResult)
}

func (m *MockMetricsQuerier) QueryCPUUsageMax(start, end time.Time) *source.Future[source.CPUUsageMaxResult] {
	m.recordCall("QueryCPUUsageMax")
	return newEmptyResult(source.DecodeCPUUsageMaxResult)
}

func (m *MockMetricsQuerier) QueryNodeCPUPricePerHr(start, end time.Time) *source.Future[source.NodeCPUPricePerHrResult] {
	m.recordCall("QueryNodeCPUPricePerHr")
	return newEmptyResult(source.DecodeNodeCPUPricePerHrResult)
}

func (m *MockMetricsQuerier) QueryGPUsAllocated(start, end time.Time) *source.Future[source.GPUsAllocatedResult] {
	m.recordCall("QueryGPUsAllocated")
	return newEmptyResult(source.DecodeGPUsAllocatedResult)
}

func (m *MockMetricsQuerier) QueryGPUsRequested(start, end time.Time) *source.Future[source.GPUsRequestedResult] {
	m.recordCall("QueryGPUsRequested")
	return newEmptyResult(source.DecodeGPUsRequestedResult)
}

func (m *MockMetricsQuerier) QueryGPUsUsageAvg(start, end time.Time) *source.Future[source.GPUsUsageAvgResult] {
	m.recordCall("QueryGPUsUsageAvg")
	return newEmptyResult(source.DecodeGPUsUsageAvgResult)
}

func (m *MockMetricsQuerier) QueryGPUsUsageMax(start, end time.Time) *source.Future[source.GPUsUsageMaxResult] {
	m.recordCall("QueryGPUsUsageMax")
	return newEmptyResult(source.DecodeGPUsUsageMaxResult)
}

func (m *MockMetricsQuerier) QueryNodeGPUPricePerHr(start, end time.Time) *source.Future[source.NodeGPUPricePerHrResult] {
	m.recordCall("QueryNodeGPUPricePerHr")
	return newEmptyResult(source.DecodeNodeGPUPricePerHrResult)
}

func (m *MockMetricsQuerier) QueryGPUInfo(start, end time.Time) *source.Future[source.GPUInfoResult] {
	m.recordCall("QueryGPUInfo")
	return newEmptyResult(source.DecodeGPUInfoResult)
}

func (m *MockMetricsQuerier) QueryIsGPUShared(start, end time.Time) *source.Future[source.IsGPUSharedResult] {
	m.recordCall("QueryIsGPUShared")
	return newEmptyResult(source.DecodeIsGPUSharedResult)
}

func (m *MockMetricsQuerier) QueryPodPVCAllocation(start, end time.Time) *source.Future[source.PodPVCAllocationResult] {
	m.recordCall("QueryPodPVCAllocation")
	return newEmptyResult(source.DecodePodPVCAllocationResult)
}

func (m *MockMetricsQuerier) QueryPVCBytesRequested(start, end time.Time) *source.Future[source.PVCBytesRequestedResult] {
	m.recordCall("QueryPVCBytesRequested")
	return newEmptyResult(source.DecodePVCBytesRequestedResult)
}

func (m *MockMetricsQuerier) QueryPVCInfo(start, end time.Time) *source.Future[source.PVCInfoResult] {
	m.recordCall("QueryPVCInfo")
	return newEmptyResult(source.DecodePVCInfoResult)
}

func (m *MockMetricsQuerier) QueryPVBytes(start, end time.Time) *source.Future[source.PVBytesResult] {
	m.recordCall("QueryPVBytes")
	return newEmptyResult(source.DecodePVBytesResult)
}

func (m *MockMetricsQuerier) QueryPVPricePerGiBHour(start, end time.Time) *source.Future[source.PVPricePerGiBHourResult] {
	m.recordCall("QueryPVPricePerGiBHour")
	return newEmptyResult(source.DecodePVPricePerGiBHourResult)
}

func (m *MockMetricsQuerier) QueryPVInfo(start, end time.Time) *source.Future[source.PVInfoResult] {
	m.recordCall("QueryPVInfo")
	return newEmptyResult(source.DecodePVInfoResult)
}

func (m *MockMetricsQuerier) QueryNetZoneGiB(start, end time.Time) *source.Future[source.NetZoneGiBResult] {
	m.recordCall("QueryNetZoneGiB")
	return newEmptyResult(source.DecodeNetZoneGiBResult)
}

func (m *MockMetricsQuerier) QueryNetZonePricePerGiB(start, end time.Time) *source.Future[source.NetZonePricePerGiBResult] {
	m.recordCall("QueryNetZonePricePerGiB")
	return newEmptyResult(source.DecodeNetZonePricePerGiBResult)
}

func (m *MockMetricsQuerier) QueryNetRegionGiB(start, end time.Time) *source.Future[source.NetRegionGiBResult] {
	m.recordCall("QueryNetRegionGiB")
	return newEmptyResult(source.DecodeNetRegionGiBResult)
}

func (m *MockMetricsQuerier) QueryNetRegionPricePerGiB(start, end time.Time) *source.Future[source.NetRegionPricePerGiBResult] {
	m.recordCall("QueryNetRegionPricePerGiB")
	return newEmptyResult(source.DecodeNetRegionPricePerGiBResult)
}

func (m *MockMetricsQuerier) QueryNetInternetGiB(start, end time.Time) *source.Future[source.NetInternetGiBResult] {
	m.recordCall("QueryNetInternetGiB")
	return newEmptyResult(source.DecodeNetInternetGiBResult)
}

func (m *MockMetricsQuerier) QueryNetInternetPricePerGiB(start, end time.Time) *source.Future[source.NetInternetPricePerGiBResult] {
	m.recordCall("QueryNetInternetPricePerGiB")
	return newEmptyResult(source.DecodeNetInternetPricePerGiBResult)
}

func (m *MockMetricsQuerier) QueryNetInternetServiceGiB(start, end time.Time) *source.Future[source.NetInternetServiceGiBResult] {
	m.recordCall("QueryNetInternetServiceGiB")
	return newEmptyResult(source.DecodeNetInternetServiceGiBResult)
}

func (m *MockMetricsQuerier) QueryNetNatGatewayGiB(start, end time.Time) *source.Future[source.NetNatGatewayGiBResult] {
	m.recordCall("QueryNetNatGatewayGiB")
	return newEmptyResult(source.DecodeNetNatGatewayGiBResult)
}

func (m *MockMetricsQuerier) QueryNetNatGatewayPricePerGiB(start, end time.Time) *source.Future[source.NetNatGatewayPricePerGiBResult] {
	m.recordCall("QueryNetNatGatewayPricePerGiB")
	return newEmptyResult(source.DecodeNetNatGatewayPricePerGiBResult)
}

func (m *MockMetricsQuerier) QueryNetTransferBytes(start, end time.Time) *source.Future[source.NetTransferBytesResult] {
	m.recordCall("QueryNetTransferBytes")
	return newEmptyResult(source.DecodeNetTransferBytesResult)
}

func (m *MockMetricsQuerier) QueryNetZoneIngressGiB(start, end time.Time) *source.Future[source.NetZoneIngressGiBResult] {
	m.recordCall("QueryNetZoneIngressGiB")
	return newEmptyResult(source.DecodeNetZoneIngressGiBResult)
}

func (m *MockMetricsQuerier) QueryNetRegionIngressGiB(start, end time.Time) *source.Future[source.NetRegionIngressGiBResult] {
	m.recordCall("QueryNetRegionIngressGiB")
	return newEmptyResult(source.DecodeNetRegionIngressGiBResult)
}

func (m *MockMetricsQuerier) QueryNetInternetIngressGiB(start, end time.Time) *source.Future[source.NetInternetIngressGiBResult] {
	m.recordCall("QueryNetInternetIngressGiB")
	return newEmptyResult(source.DecodeNetInternetIngressGiBResult)
}

func (m *MockMetricsQuerier) QueryNetInternetServiceIngressGiB(start, end time.Time) *source.Future[source.NetInternetServiceIngressGiBResult] {
	m.recordCall("QueryNetInternetServiceIngressGiB")
	return newEmptyResult(source.DecodeNetInternetServiceIngressGiBResult)
}

func (m *MockMetricsQuerier) QueryNetNatGatewayIngressGiB(start, end time.Time) *source.Future[source.NetNatGatewayIngressGiBResult] {
	m.recordCall("QueryNetNatGatewayIngressGiB")
	return newEmptyResult(source.DecodeNetNatGatewayIngressGiBResult)
}

func (m *MockMetricsQuerier) QueryNetNatGatewayIngressPricePerGiB(start, end time.Time) *source.Future[source.NetNatGatewayPricePerGiBResult] {
	m.recordCall("QueryNetNatGatewayIngressPricePerGiB")
	return newEmptyResult(source.DecodeNetNatGatewayPricePerGiBResult)
}

func (m *MockMetricsQuerier) QueryNetReceiveBytes(start, end time.Time) *source.Future[source.NetReceiveBytesResult] {
	m.recordCall("QueryNetReceiveBytes")
	return newEmptyResult(source.DecodeNetReceiveBytesResult)
}

func (m *MockMetricsQuerier) QueryNamespaceUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	m.recordCall("QueryNamespaceUptime")
	return newEmptyResult(source.DecodeUptimeResult)
}

func (m *MockMetricsQuerier) QueryNamespaceAnnotations(start, end time.Time) *source.Future[source.NamespaceAnnotationsResult] {
	m.recordCall("QueryNamespaceAnnotations")
	return newEmptyResult(source.DecodeNamespaceAnnotationsResult)
}

func (m *MockMetricsQuerier) QueryPodAnnotations(start, end time.Time) *source.Future[source.PodAnnotationsResult] {
	m.recordCall("QueryPodAnnotations")
	return newEmptyResult(source.DecodePodAnnotationsResult)
}

func (m *MockMetricsQuerier) QueryNodeLabels(start, end time.Time) *source.Future[source.NodeLabelsResult] {
	m.recordCall("QueryNodeLabels")
	return newEmptyResult(source.DecodeNodeLabelsResult)
}

func (m *MockMetricsQuerier) QueryNamespaceLabels(start, end time.Time) *source.Future[source.NamespaceLabelsResult] {
	m.recordCall("QueryNamespaceLabels")
	return newEmptyResult(source.DecodeNamespaceLabelsResult)
}

func (m *MockMetricsQuerier) QueryPodLabels(start, end time.Time) *source.Future[source.PodLabelsResult] {
	m.recordCall("QueryPodLabels")
	return newEmptyResult(source.DecodePodLabelsResult)
}

func (m *MockMetricsQuerier) QueryServiceLabels(start, end time.Time) *source.Future[source.ServiceLabelsResult] {
	m.recordCall("QueryServiceLabels")
	return newEmptyResult(source.DecodeServiceLabelsResult)
}

func (m *MockMetricsQuerier) QueryDeploymentLabels(start, end time.Time) *source.Future[source.DeploymentLabelsResult] {
	m.recordCall("QueryDeploymentLabels")
	return newEmptyResult(source.DecodeDeploymentLabelsResult)
}

func (m *MockMetricsQuerier) QueryStatefulSetLabels(start, end time.Time) *source.Future[source.StatefulSetLabelsResult] {
	m.recordCall("QueryStatefulSetLabels")
	return newEmptyResult(source.DecodeStatefulSetLabelsResult)
}

func (m *MockMetricsQuerier) QueryDaemonSetLabels(start, end time.Time) *source.Future[source.DaemonSetLabelsResult] {
	m.recordCall("QueryDaemonSetLabels")
	return newEmptyResult(source.DecodeDaemonSetLabelsResult)
}

func (m *MockMetricsQuerier) QueryJobLabels(start, end time.Time) *source.Future[source.JobLabelsResult] {
	m.recordCall("QueryJobLabels")
	return newEmptyResult(source.DecodeJobLabelsResult)
}

func (m *MockMetricsQuerier) QueryPodsWithReplicaSetOwner(start, end time.Time) *source.Future[source.PodsWithReplicaSetOwnerResult] {
	m.recordCall("QueryPodsWithReplicaSetOwner")
	return newEmptyResult(source.DecodePodsWithReplicaSetOwnerResult)
}

func (m *MockMetricsQuerier) QueryReplicaSetsWithoutOwners(start, end time.Time) *source.Future[source.ReplicaSetsWithoutOwnersResult] {
	m.recordCall("QueryReplicaSetsWithoutOwners")
	return newEmptyResult(source.DecodeReplicaSetsWithoutOwnersResult)
}

func (m *MockMetricsQuerier) QueryReplicaSetsWithRollout(start, end time.Time) *source.Future[source.ReplicaSetsWithRolloutResult] {
	m.recordCall("QueryReplicaSetsWithRollout")
	return newEmptyResult(source.DecodeReplicaSetsWithRolloutResult)
}

func (m *MockMetricsQuerier) QueryResourceQuotaUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	m.recordCall("QueryResourceQuotaUptime")
	return newEmptyResult(source.DecodeUptimeResult)
}

func (m *MockMetricsQuerier) QueryResourceQuotaSpecCPURequestAverage(start, end time.Time) *source.Future[source.ResourceQuotaSpecCPURequestAvgResult] {
	m.recordCall("QueryResourceQuotaSpecCPURequestAverage")
	return newEmptyResult(source.DecodeResourceQuotaSpecCPURequestAvgResult)
}

func (m *MockMetricsQuerier) QueryResourceQuotaSpecCPURequestMax(start, end time.Time) *source.Future[source.ResourceQuotaSpecCPURequestMaxResult] {
	m.recordCall("QueryResourceQuotaSpecCPURequestMax")
	return newEmptyResult(source.DecodeResourceQuotaSpecCPURequestMaxResult)
}

func (m *MockMetricsQuerier) QueryResourceQuotaSpecRAMRequestAverage(start, end time.Time) *source.Future[source.ResourceQuotaSpecRAMRequestAvgResult] {
	m.recordCall("QueryResourceQuotaSpecRAMRequestAverage")
	return newEmptyResult(source.DecodeResourceQuotaSpecRAMRequestAvgResult)
}

func (m *MockMetricsQuerier) QueryResourceQuotaSpecRAMRequestMax(start, end time.Time) *source.Future[source.ResourceQuotaSpecRAMRequestMaxResult] {
	m.recordCall("QueryResourceQuotaSpecRAMRequestMax")
	return newEmptyResult(source.DecodeResourceQuotaSpecRAMRequestMaxResult)
}

func (m *MockMetricsQuerier) QueryResourceQuotaSpecCPULimitAverage(start, end time.Time) *source.Future[source.ResourceQuotaSpecCPULimitAvgResult] {
	m.recordCall("QueryResourceQuotaSpecCPULimitAverage")
	return newEmptyResult(source.DecodeResourceQuotaSpecCPULimitAvgResult)
}

func (m *MockMetricsQuerier) QueryResourceQuotaSpecCPULimitMax(start, end time.Time) *source.Future[source.ResourceQuotaSpecCPULimitMaxResult] {
	m.recordCall("QueryResourceQuotaSpecCPULimitMax")
	return newEmptyResult(source.DecodeResourceQuotaSpecCPULimitMaxResult)
}

func (m *MockMetricsQuerier) QueryResourceQuotaSpecRAMLimitAverage(start, end time.Time) *source.Future[source.ResourceQuotaSpecRAMLimitAvgResult] {
	m.recordCall("QueryResourceQuotaSpecRAMLimitAverage")
	return newEmptyResult(source.DecodeResourceQuotaSpecRAMLimitAvgResult)
}

func (m *MockMetricsQuerier) QueryResourceQuotaSpecRAMLimitMax(start, end time.Time) *source.Future[source.ResourceQuotaSpecRAMLimitMaxResult] {
	m.recordCall("QueryResourceQuotaSpecRAMLimitMax")
	return newEmptyResult(source.DecodeResourceQuotaSpecRAMLimitMaxResult)
}

func (m *MockMetricsQuerier) QueryResourceQuotaStatusUsedCPURequestAverage(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedCPURequestAvgResult] {
	m.recordCall("QueryResourceQuotaStatusUsedCPURequestAverage")
	return newEmptyResult(source.DecodeResourceQuotaStatusUsedCPURequestAvgResult)
}

func (m *MockMetricsQuerier) QueryResourceQuotaStatusUsedCPURequestMax(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedCPURequestMaxResult] {
	m.recordCall("QueryResourceQuotaStatusUsedCPURequestMax")
	return newEmptyResult(source.DecodeResourceQuotaStatusUsedCPURequestMaxResult)
}

func (m *MockMetricsQuerier) QueryResourceQuotaStatusUsedRAMRequestAverage(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedRAMRequestAvgResult] {
	m.recordCall("QueryResourceQuotaStatusUsedRAMRequestAverage")
	return newEmptyResult(source.DecodeResourceQuotaStatusUsedRAMRequestAvgResult)
}

func (m *MockMetricsQuerier) QueryResourceQuotaStatusUsedRAMRequestMax(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedRAMRequestMaxResult] {
	m.recordCall("QueryResourceQuotaStatusUsedRAMRequestMax")
	return newEmptyResult(source.DecodeResourceQuotaStatusUsedRAMRequestMaxResult)
}

func (m *MockMetricsQuerier) QueryResourceQuotaStatusUsedCPULimitAverage(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedCPULimitAvgResult] {
	m.recordCall("QueryResourceQuotaStatusUsedCPULimitAverage")
	return newEmptyResult(source.DecodeResourceQuotaStatusUsedCPULimitAvgResult)
}

func (m *MockMetricsQuerier) QueryResourceQuotaStatusUsedCPULimitMax(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedCPULimitMaxResult] {
	m.recordCall("QueryResourceQuotaStatusUsedCPULimitMax")
	return newEmptyResult(source.DecodeResourceQuotaStatusUsedCPULimitMaxResult)
}

func (m *MockMetricsQuerier) QueryResourceQuotaStatusUsedRAMLimitAverage(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedRAMLimitAvgResult] {
	m.recordCall("QueryResourceQuotaStatusUsedRAMLimitAverage")
	return newEmptyResult(source.DecodeResourceQuotaStatusUsedRAMLimitAvgResult)
}

func (m *MockMetricsQuerier) QueryResourceQuotaStatusUsedRAMLimitMax(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedRAMLimitMaxResult] {
	m.recordCall("QueryResourceQuotaStatusUsedRAMLimitMax")
	return newEmptyResult(source.DecodeResourceQuotaStatusUsedRAMLimitMaxResult)
}

func (m *MockMetricsQuerier) QueryDataCoverage(_ int) (time.Time, time.Time, error) {
	m.recordCall("QueryDataCoverage")
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

//--------------------------------------------------------------------------
//  Mock StatsSummaryClient
//--------------------------------------------------------------------------

// MockStatsSummaryClient is a mock implementation of the nodes.StatsSummaryClient interface
// that records the number of times each method is called.
type MockStatsSummaryClient struct {
	Calls map[string]int
}

// NewMockStatsSummaryClient creates a new mock metrics client
func NewMockStatsSummaryClient() *MockStatsSummaryClient {
	return &MockStatsSummaryClient{
		Calls: make(map[string]int),
	}
}

// Helper to record method calls
func (m *MockStatsSummaryClient) recordCall(method string) {
	m.Calls[method]++
}

// Implementation of interface methods
func (m *MockStatsSummaryClient) GetNodeData() ([]*stats.Summary, error) {
	m.recordCall("GetNodeData")
	return nil, nil
}

//--------------------------------------------------------------------------
//  Mock Metadata
//--------------------------------------------------------------------------

// MockMetadata is a mock implementation of the version.Metadata interface
// that records the number of times each method is called.
type MockMetadata struct {
	Calls map[string]int
}

// NewMockMetadata creates a new mock metadata
func NewMockMetadata() *MockMetadata {
	return &MockMetadata{
		Calls: make(map[string]int),
	}
}

// Helper to record method calls
func (m *MockMetadata) recordCall(method string) {
	m.Calls[method]++
}

// Implementation of interface methods
func (m *MockMetadata) GetClusterInfo() *cluster.Info {
	m.recordCall("GetVersionInfo")
	return nil
}
