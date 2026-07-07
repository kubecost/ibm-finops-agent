package mocks

import (
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
	MetricsQuerier         *source.RecordMetricsQuerier
	NodeStatsSummaryClient *MockStatsSummaryClient
	CMetadata              *MockMetadata
}

// NewMockDataSource creates a new mock data source implementation with services that track
// method calls only (empty responses).
func NewMockDataSource() *MockDataSource {
	ocDataSource := NewMockOpenCostDataSource()
	metrics := ocDataSource.Metrics().(*source.RecordMetricsQuerier)
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

func (mds *MockDataSource) NodeStatsProvider() *nodes.NodeStatsSummaryProvider {
	return nil
}

// MockOpenCostDataSource
type MockOpenCostDataSource struct {
	MetricsQuerier      *source.RecordMetricsQuerier
	ClusterInfoProvider clusters.ClusterInfoProvider
}

func NewMockOpenCostDataSource() *MockOpenCostDataSource {
	return &MockOpenCostDataSource{
		MetricsQuerier: source.NewRecordMetricsQuerier(source.NewNoOpMetricsQuerier()),
		ClusterInfoProvider: clusters.NewMockClusterInfoProvider(
			map[string]string{
				clusters.ClusterInfoIdKey: "mock-cluster-id",
			},
		),
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
	CronJobs               []*batchv1.CronJob
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

// GetAllShortLivedPods implements ClusterCache interface
func (m *MockClusterCache) GetAllShortLivedPods() []*v1.Pod {
	m.recordCall("GetAllShortLivedPods")
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

// GetAllCronJobs implements ClusterCache interface
func (m *MockClusterCache) GetAllCronJobs() []*batchv1.CronJob {
	m.recordCall("GetAllCronJobs")
	return m.CronJobs
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
func (m *MockStatsSummaryClient) GetNodeData() ([]*stats.Summary, []nodes.NodeCollectionResult, error) {
	m.recordCall("GetNodeData")
	return nil, nil, nil
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
