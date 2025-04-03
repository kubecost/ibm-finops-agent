package emitter

import (
	"testing"
	"time"
	"unsafe"

	"github.com/ibm/finops-agent/pkg/cluster"
	"github.com/opencost/opencost/core/pkg/source"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	stv1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// NOTE: When the metrics caching is removed, this test can also be removed!
func TestSnapshottingTemporaryCache(t *testing.T) {
	previousCacheDuration := metricsSummaryCacheDuration
	metricsSummaryCacheDuration = time.Second

	// reinstate the previous value after the test
	defer func() {
		metricsSummaryCacheDuration = previousCacheDuration
	}()

	ds := NewMockDataSource()
	snapshotProvider := NewConcurrentSnapshotProvider()

	snapshot, err := snapshotProvider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// get the snapshot of metrics data
	metricsSnapshot := snapshot.Metrics

	// wait a bit
	time.Sleep(250 * time.Millisecond)

	newSnapshot, err := snapshotProvider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	newMetricsSnapshot := newSnapshot.Metrics

	// compare the two snapshots (should be cached)
	// does go perform reference equality checks for non-comparable types??
	// we can be safe and just compare ptr values
	p1 := uintptr(unsafe.Pointer(metricsSnapshot))
	p2 := uintptr(unsafe.Pointer(newMetricsSnapshot))

	t.Logf("Snapshot 1: %d, Snapshot 2: %d", p1, p2)
	if p1 != p2 {
		t.Fatalf("Expected the same snapshot to be returned, got different pointers")
	}

	// wait beyond cache duration
	time.Sleep(time.Second)

	newestSnapshot, err := snapshotProvider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	newestMetricsSnapshot := newestSnapshot.Metrics

	// compare the two snapshots (should be different)
	p3 := uintptr(unsafe.Pointer(newestMetricsSnapshot))

	t.Logf("Snapshot 1: %d, Snapshot 3: %d", p1, p3)
	if p1 == p3 {
		t.Fatalf("Expected different snapshots to be returned, got same pointers")
	}
}

//--------------------------------------------------------------------------
//  Mock ClusterCache (empty)
//--------------------------------------------------------------------------

// MockDataSource contains mock implementations of the interfaces returned by the data source.
type MockDataSource struct {
	ClusterCache   *MockClusterCache
	MetricsQuerier *MockMetricsQuerier
}

// NewMockDataSource creates a new mock data source implementation with services that track
// method calls only (empty responses).
func NewMockDataSource() *MockDataSource {
	return &MockDataSource{
		ClusterCache:   NewMockClusterCache(),
		MetricsQuerier: NewMockMetricsQuerier(),
	}
}

func (mds *MockDataSource) Cluster() cluster.ClusterCache {
	return mds.ClusterCache
}

func (mds *MockDataSource) Metrics() source.MetricsQuerier {
	return mds.MetricsQuerier
}

//--------------------------------------------------------------------------
//  Mock ClusterCache (empty resources)
//--------------------------------------------------------------------------

// MockClusterCache implements the ClusterCache interface for testing
type MockClusterCache struct {
	// Track method calls
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
	m.Calls[method]++
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

func (m *MockMetricsQuerier) QueryNetReceiveBytes(start, end time.Time) *source.Future[source.NetReceiveBytesResult] {
	m.recordCall("QueryNetReceiveBytes")
	return newEmptyResult(source.DecodeNetReceiveBytesResult)
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
