package emitter

import (
	"fmt"
	"time"

	"github.com/hashicorp/go-multierror"
	clustercache "github.com/ibm/finops-agent/pkg/cluster"
	"github.com/ibm/finops-agent/pkg/core"
	"github.com/ibm/finops-agent/pkg/nodes"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
)

// SnapshotProvider is an interface that defines a prototype for generating `ClusterSnapshot` instances
// leveraging the agent `DataSource`
type SnapshotProvider interface {
	// SnapshotOf generates a `ClusterSnapshot` from the provided `core.DataSource` and returns it.
	// If the snapshot generation fails, an error is returned.
	SnapshotOf(core.DataSource) (*ClusterSnapshot, error)
}

// FIXME: (bolt) use a metrics summary cache duration of 5 minutes while we're using a prometheus data source.
// FIXME: (bolt) this should be fine to run on a much faster frequency with a non-promethues metrics querier.
var metricsSummaryCacheDuration time.Duration = 5 * time.Minute

// ConcurrentSnapshotProvider is a struct that implements the `SnapshotProvider` interface and executes the
// snapshot generation process concurrently.
type ConcurrentSnapshotProvider struct {
	config             *SnapshotConfig
	metricsSummary     *MetricsSummary
	lastMetricsSummary time.Time
}

// NewConcurrentSnapshotProvider creates a new instance of `ConcurrentSnapshotProvider`.
func NewConcurrentSnapshotProvider(config *SnapshotConfig) SnapshotProvider {
	if config == nil {
		config = DefaultSnapshotConfig()
	}

	return &ConcurrentSnapshotProvider{
		config: config,
	}
}

// SnapshotOf generates a `ClusterSnapshot` from the provided `core.DataSource` and returns it.
func (csp *ConcurrentSnapshotProvider) SnapshotOf(ds core.DataSource) (*ClusterSnapshot, error) {
	var group multierror.Group

	// Cluster Info Snapshot
	var clusterInfo *clusters.ClusterInfo
	group.Go(func() error {
		var err error
		clusterInfo, err = snapshotClusterInfo(ds.OpenCostSource().ClusterInfo())
		return err
	})

	// Kubernetes Snapshot
	var k8sSnapshot *KubernetesSnapshot
	group.Go(func() error {
		var err error
		k8sSnapshot, err = snapshotKubernetes(ds.Cluster(), csp.config)
		return err
	})

	// Node Stats Snapshot
	var nodeStats *NodeStatsSummary
	group.Go(func() error {
		var err error
		nodeStats, err = snapshotNodeStats(ds.StatsSummary())
		return err
	})

	// Metrics Snapshot
	var metricsSnapshot *MetricsSummary
	group.Go(func() error {
		var err error
		metricsSnapshot, err = csp.cachedMetricsSummary(ds.Metrics(), csp.config)
		return err
	})

	err := group.Wait()
	if err != nil {
		return nil, fmt.Errorf("failed to generate cluster snapshot: %w", err)
	}

	return &ClusterSnapshot{
		ClusterInfo: clusterInfo,
		Kubernetes:  k8sSnapshot,
		NodeStats:   nodeStats,
		Metrics:     metricsSnapshot,
	}, nil
}

// temporary caching of metrics summary every 5 minutes to avoid overloading the prometheus data source until
// prometheus can be replaced.
func (csp *ConcurrentSnapshotProvider) cachedMetricsSummary(querier source.MetricsQuerier, config *SnapshotConfig) (*MetricsSummary, error) {
	if !config.UseMetricsCache {
		return snapshotMetricsSummary(querier, config)
	}

	now := time.Now().UTC()

	// FIXME: (bolt) use a metrics summary cache duration of 5 minutes while we're using a prometheus data source.
	// FIXME: (bolt) this should be fine to run on a much faster frequency with a non-promethues metrics querier.
	if !csp.lastMetricsSummary.IsZero() && time.Since(csp.lastMetricsSummary) < metricsSummaryCacheDuration {
		return csp.metricsSummary, nil
	}

	metricsSummary, err := snapshotMetricsSummary(querier, config)
	if err != nil {
		return nil, err
	}

	// Note: (bolt) assuming we're not calling the SnapshotOf() in multiple goroutines [which there shouldn't be],
	// Note: (bolt) there's no need to lock on cache updates. Temporary solution until we can drop prom fully.
	csp.lastMetricsSummary = now
	csp.metricsSummary = metricsSummary

	return metricsSummary, nil
}

func snapshotClusterInfo(infoProvider clusters.ClusterInfoProvider) (*clusters.ClusterInfo, error) {
	clusterInfoMap := infoProvider.GetClusterInfo()
	clusterInfo, err := clusters.MapToClusterInfo(clusterInfoMap)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster info: %w", err)
	}

	return clusterInfo, nil
}

func snapshotKubernetes(cluster clustercache.ClusterCache, config *SnapshotConfig) (*KubernetesSnapshot, error) {
	// if we get here, and a kubernetes snapshot config hasn't been provided, enable all resource snapshots by
	// default
	if config.KubernetesSnapshot == nil {
		config.KubernetesSnapshot = NewKubernetesSnapshotConfig().EnableAll()
	}

	kconfig := config.KubernetesSnapshot
	return &KubernetesSnapshot{
		Nodes:                  snapshotResource(kconfig.Nodes, cluster.GetAllNodes),
		Pods:                   snapshotResource(kconfig.Pods, cluster.GetAllPods),
		ShortLivedPods:         cluster.GetAllShortLivedPods(), // always snapshot short-lived pods to reset buffer
		Namespaces:             snapshotResource(kconfig.Namespaces, cluster.GetAllNamespaces),
		Services:               snapshotResource(kconfig.Services, cluster.GetAllServices),
		DaemonSets:             snapshotResource(kconfig.DaemonSets, cluster.GetAllDaemonSets),
		Deployments:            snapshotResource(kconfig.Deployments, cluster.GetAllDeployments),
		StatefulSets:           snapshotResource(kconfig.StatefulSets, cluster.GetAllStatefulSets),
		ReplicaSets:            snapshotResource(kconfig.ReplicaSets, cluster.GetAllReplicaSets),
		PersistentVolumes:      snapshotResource(kconfig.PersistentVolumes, cluster.GetAllPersistentVolumes),
		PersistentVolumeClaims: snapshotResource(kconfig.PersistentVolumeClaims, cluster.GetAllPersistentVolumeClaims),
		StorageClasses:         snapshotResource(kconfig.StorageClasses, cluster.GetAllStorageClasses),
		Jobs:                   snapshotResource(kconfig.Jobs, cluster.GetAllJobs),
		PodDisruptionBudgets:   snapshotResource(kconfig.PodDisruptionBudgets, cluster.GetAllPodDisruptionBudgets),
		ReplicationControllers: snapshotResource(kconfig.ReplicationControllers, cluster.GetAllReplicationControllers),
		ResourceQuotas:         snapshotResource(kconfig.ResourceQuotas, cluster.GetAllResourceQuotas),
	}, nil
}

// snapshots a specific resource based on the provided flag.
func snapshotResource[T any](flag bool, resourceGetter func() []T) []T {
	if !flag {
		return []T{}
	}
	return resourceGetter()
}

func snapshotNodeStats(client nodes.StatSummaryClient) (*NodeStatsSummary, error) {
	data, err := client.GetNodeData()
	if err != nil {
		return nil, fmt.Errorf("failed to generate node stats snapshot: %w", err)
	}

	return &NodeStatsSummary{
		Stats: data,
	}, nil
}

func snapshotMetricsSummary(querier source.MetricsQuerier, config *SnapshotConfig) (*MetricsSummary, error) {
	var minutelySnapshot *MetricsSnapshot
	if config.MinutelyMetricsEnabled {
		start, end := windowFor(10 * time.Minute)
		snapshot, err := snapshotMetrics(querier, start, end)
		if err != nil {
			return nil, fmt.Errorf("failed to generate minutely metrics snapshot: %w", err)
		}
		minutelySnapshot = snapshot
	}

	start, end := windowFor(time.Hour)
	hourlySnapshot, err := snapshotMetrics(querier, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to generate hourly metrics snapshot: %w", err)
	}

	start, end = windowFor(24 * time.Hour)
	dailySnapshot, err := snapshotMetrics(querier, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to generate daily metrics snapshot: %w", err)
	}

	return &MetricsSummary{
		Minutely: minutelySnapshot,
		Hourly:   hourlySnapshot,
		Daily:    dailySnapshot,
	}, nil
}

func snapshotMetrics(mq source.MetricsQuerier, start, end time.Time) (*MetricsSnapshot, error) {
	grp := source.NewQueryGroup()

	pvActiveMinutesFuture := source.WithGroup(grp, mq.QueryPVActiveMinutes(start, end))
	pvUsedAverageFuture := source.WithGroup(grp, mq.QueryPVUsedAverage(start, end))
	pvUsedMaxFuture := source.WithGroup(grp, mq.QueryPVUsedMax(start, end))
	localStorageActiveMinutesFuture := source.WithGroup(grp, mq.QueryLocalStorageActiveMinutes(start, end))
	localStorageCostFuture := source.WithGroup(grp, mq.QueryLocalStorageCost(start, end))
	localStorageUsedCostFuture := source.WithGroup(grp, mq.QueryLocalStorageUsedCost(start, end))
	localStorageUsedAvgFuture := source.WithGroup(grp, mq.QueryLocalStorageUsedAvg(start, end))
	localStorageUsedMaxFuture := source.WithGroup(grp, mq.QueryLocalStorageUsedMax(start, end))
	localStorageBytesFuture := source.WithGroup(grp, mq.QueryLocalStorageBytes(start, end))
	nodeActiveMinutesFuture := source.WithGroup(grp, mq.QueryNodeActiveMinutes(start, end))
	nodeCPUCoresCapacityFuture := source.WithGroup(grp, mq.QueryNodeCPUCoresCapacity(start, end))
	nodeCPUCoresAllocatableFuture := source.WithGroup(grp, mq.QueryNodeCPUCoresAllocatable(start, end))
	nodeRAMBytesCapacityFuture := source.WithGroup(grp, mq.QueryNodeRAMBytesCapacity(start, end))
	nodeRAMBytesAllocatableFuture := source.WithGroup(grp, mq.QueryNodeRAMBytesAllocatable(start, end))
	nodeGPUCountFuture := source.WithGroup(grp, mq.QueryNodeGPUCount(start, end))
	nodeCPUModeTotalFuture := source.WithGroup(grp, mq.QueryNodeCPUModeTotal(start, end))
	nodeIsSpotFuture := source.WithGroup(grp, mq.QueryNodeIsSpot(start, end))
	nodeRAMSystemPercentFuture := source.WithGroup(grp, mq.QueryNodeRAMSystemPercent(start, end))
	nodeRAMUserPercentFuture := source.WithGroup(grp, mq.QueryNodeRAMUserPercent(start, end))
	lbActiveMinutesFuture := source.WithGroup(grp, mq.QueryLBActiveMinutes(start, end))
	lbPricePerHrFuture := source.WithGroup(grp, mq.QueryLBPricePerHr(start, end))
	clusterManagementDurationFuture := source.WithGroup(grp, mq.QueryClusterManagementDuration(start, end))
	clusterManagementPricePerHrFuture := source.WithGroup(grp, mq.QueryClusterManagementPricePerHr(start, end))
	podsFuture := source.WithGroup(grp, mq.QueryPods(start, end))
	podsUIDFuture := source.WithGroup(grp, mq.QueryPodsUID(start, end))
	ramBytesAllocatedFuture := source.WithGroup(grp, mq.QueryRAMBytesAllocated(start, end))
	ramRequestsFuture := source.WithGroup(grp, mq.QueryRAMRequests(start, end))
	ramUsageAvgFuture := source.WithGroup(grp, mq.QueryRAMUsageAvg(start, end))
	ramUsageMaxFuture := source.WithGroup(grp, mq.QueryRAMUsageMax(start, end))
	nodeRAMPricePerGiBHrFuture := source.WithGroup(grp, mq.QueryNodeRAMPricePerGiBHr(start, end))
	cpuCoresAllocatedFuture := source.WithGroup(grp, mq.QueryCPUCoresAllocated(start, end))
	cpuRequestsFuture := source.WithGroup(grp, mq.QueryCPURequests(start, end))
	cpuUsageAvgFuture := source.WithGroup(grp, mq.QueryCPUUsageAvg(start, end))
	cpuUsageMaxFuture := source.WithGroup(grp, mq.QueryCPUUsageMax(start, end))
	nodeCPUPricePerHrFuture := source.WithGroup(grp, mq.QueryNodeCPUPricePerHr(start, end))
	gpusAllocatedFuture := source.WithGroup(grp, mq.QueryGPUsAllocated(start, end))
	gpusRequestedFuture := source.WithGroup(grp, mq.QueryGPUsRequested(start, end))
	gpusUsageAvgFuture := source.WithGroup(grp, mq.QueryGPUsUsageAvg(start, end))
	gpusUsageMaxFuture := source.WithGroup(grp, mq.QueryGPUsUsageMax(start, end))
	nodeGPUPricePerHrFuture := source.WithGroup(grp, mq.QueryNodeGPUPricePerHr(start, end))
	gpuInfoFuture := source.WithGroup(grp, mq.QueryGPUInfo(start, end))
	isGPUSharedFuture := source.WithGroup(grp, mq.QueryIsGPUShared(start, end))
	podPVCAllocationFuture := source.WithGroup(grp, mq.QueryPodPVCAllocation(start, end))
	pvcBytesRequestedFuture := source.WithGroup(grp, mq.QueryPVCBytesRequested(start, end))
	pvcInfoFuture := source.WithGroup(grp, mq.QueryPVCInfo(start, end))
	pvBytesFuture := source.WithGroup(grp, mq.QueryPVBytes(start, end))
	pvPricePerGiBHourFuture := source.WithGroup(grp, mq.QueryPVPricePerGiBHour(start, end))
	pvInfoFuture := source.WithGroup(grp, mq.QueryPVInfo(start, end))
	netZoneGiBFuture := source.WithGroup(grp, mq.QueryNetZoneGiB(start, end))
	netZonePricePerGiBFuture := source.WithGroup(grp, mq.QueryNetZonePricePerGiB(start, end))
	netRegionGiBFuture := source.WithGroup(grp, mq.QueryNetRegionGiB(start, end))
	netRegionPricePerGiBFuture := source.WithGroup(grp, mq.QueryNetRegionPricePerGiB(start, end))
	netInternetGiBFuture := source.WithGroup(grp, mq.QueryNetInternetGiB(start, end))
	netInternetPricePerGiBFuture := source.WithGroup(grp, mq.QueryNetInternetPricePerGiB(start, end))
	netInternetServiceGiBFuture := source.WithGroup(grp, mq.QueryNetInternetServiceGiB(start, end))
	netTransferBytesFuture := source.WithGroup(grp, mq.QueryNetTransferBytes(start, end))
	netZoneIngressGiBFuture := source.WithGroup(grp, mq.QueryNetZoneIngressGiB(start, end))
	netRegionIngressGiBFuture := source.WithGroup(grp, mq.QueryNetRegionIngressGiB(start, end))
	netInternetIngressGiBFuture := source.WithGroup(grp, mq.QueryNetInternetIngressGiB(start, end))
	netInternetServiceIngressGiBFuture := source.WithGroup(grp, mq.QueryNetInternetServiceIngressGiB(start, end))
	netReceiveBytesFuture := source.WithGroup(grp, mq.QueryNetReceiveBytes(start, end))
	namespaceAnnotationsFuture := source.WithGroup(grp, mq.QueryNamespaceAnnotations(start, end))
	podAnnotationsFuture := source.WithGroup(grp, mq.QueryPodAnnotations(start, end))
	nodeLabelsFuture := source.WithGroup(grp, mq.QueryNodeLabels(start, end))
	namespaceLabelsFuture := source.WithGroup(grp, mq.QueryNamespaceLabels(start, end))
	podLabelsFuture := source.WithGroup(grp, mq.QueryPodLabels(start, end))
	serviceLabelsFuture := source.WithGroup(grp, mq.QueryServiceLabels(start, end))
	deploymentLabelsFuture := source.WithGroup(grp, mq.QueryDeploymentLabels(start, end))
	statefulSetLabelsFuture := source.WithGroup(grp, mq.QueryStatefulSetLabels(start, end))
	daemonSetLabelsFuture := source.WithGroup(grp, mq.QueryDaemonSetLabels(start, end))
	jobLabelsFuture := source.WithGroup(grp, mq.QueryJobLabels(start, end))
	podsWithReplicaSetOwnerFuture := source.WithGroup(grp, mq.QueryPodsWithReplicaSetOwner(start, end))
	replicaSetsWithoutOwnersFuture := source.WithGroup(grp, mq.QueryReplicaSetsWithoutOwners(start, end))
	replicaSetsWithRolloutFuture := source.WithGroup(grp, mq.QueryReplicaSetsWithRollout(start, end))

	pvActiveMinutes, _ := pvActiveMinutesFuture.Await()
	pvUsedAverage, _ := pvUsedAverageFuture.Await()
	pvUsedMax, _ := pvUsedMaxFuture.Await()
	localStorageActiveMinutes, _ := localStorageActiveMinutesFuture.Await()
	localStorageCost, _ := localStorageCostFuture.Await()
	localStorageUsedCost, _ := localStorageUsedCostFuture.Await()
	localStorageUsedAvg, _ := localStorageUsedAvgFuture.Await()
	localStorageUsedMax, _ := localStorageUsedMaxFuture.Await()
	localStorageBytes, _ := localStorageBytesFuture.Await()
	nodeActiveMinutes, _ := nodeActiveMinutesFuture.Await()
	nodeCPUCoresCapacity, _ := nodeCPUCoresCapacityFuture.Await()
	nodeCPUCoresAllocatable, _ := nodeCPUCoresAllocatableFuture.Await()
	nodeRAMBytesCapacity, _ := nodeRAMBytesCapacityFuture.Await()
	nodeRAMBytesAllocatable, _ := nodeRAMBytesAllocatableFuture.Await()
	nodeGPUCount, _ := nodeGPUCountFuture.Await()
	nodeCPUModeTotal, _ := nodeCPUModeTotalFuture.Await()
	nodeIsSpot, _ := nodeIsSpotFuture.Await()
	nodeRAMSystemPercent, _ := nodeRAMSystemPercentFuture.Await()
	nodeRAMUserPercent, _ := nodeRAMUserPercentFuture.Await()
	lbActiveMinutes, _ := lbActiveMinutesFuture.Await()
	lbPricePerHr, _ := lbPricePerHrFuture.Await()
	clusterManagementDuration, _ := clusterManagementDurationFuture.Await()
	clusterManagementPricePerHr, _ := clusterManagementPricePerHrFuture.Await()
	pods, _ := podsFuture.Await()
	podsUID, _ := podsUIDFuture.Await()
	ramBytesAllocated, _ := ramBytesAllocatedFuture.Await()
	ramRequests, _ := ramRequestsFuture.Await()
	ramUsageAvg, _ := ramUsageAvgFuture.Await()
	ramUsageMax, _ := ramUsageMaxFuture.Await()
	nodeRAMPricePerGiBHr, _ := nodeRAMPricePerGiBHrFuture.Await()
	cpuCoresAllocated, _ := cpuCoresAllocatedFuture.Await()
	cpuRequests, _ := cpuRequestsFuture.Await()
	cpuUsageAvg, _ := cpuUsageAvgFuture.Await()
	cpuUsageMax, _ := cpuUsageMaxFuture.Await()
	nodeCPUPricePerHr, _ := nodeCPUPricePerHrFuture.Await()
	gpusAllocated, _ := gpusAllocatedFuture.Await()
	gpusRequested, _ := gpusRequestedFuture.Await()
	gpusUsageAvg, _ := gpusUsageAvgFuture.Await()
	gpusUsageMax, _ := gpusUsageMaxFuture.Await()
	nodeGPUPricePerHr, _ := nodeGPUPricePerHrFuture.Await()
	gpuInfo, _ := gpuInfoFuture.Await()
	isGPUShared, _ := isGPUSharedFuture.Await()
	podPVCAllocation, _ := podPVCAllocationFuture.Await()
	pvcBytesRequested, _ := pvcBytesRequestedFuture.Await()
	pvcInfo, _ := pvcInfoFuture.Await()
	pvBytes, _ := pvBytesFuture.Await()
	pvPricePerGiBHour, _ := pvPricePerGiBHourFuture.Await()
	pvInfo, _ := pvInfoFuture.Await()
	netZoneGiB, _ := netZoneGiBFuture.Await()
	netZonePricePerGiB, _ := netZonePricePerGiBFuture.Await()
	netRegionGiB, _ := netRegionGiBFuture.Await()
	netRegionPricePerGiB, _ := netRegionPricePerGiBFuture.Await()
	netInternetGiB, _ := netInternetGiBFuture.Await()
	netInternetPricePerGiB, _ := netInternetPricePerGiBFuture.Await()
	netInternetServiceGiB, _ := netInternetServiceGiBFuture.Await()
	netTransferBytes, _ := netTransferBytesFuture.Await()
	netZoneIngressGiB, _ := netZoneIngressGiBFuture.Await()
	netRegionIngressGiB, _ := netRegionIngressGiBFuture.Await()
	netInternetIngressGiB, _ := netInternetIngressGiBFuture.Await()
	netInternetServiceIngressGiB, _ := netInternetServiceIngressGiBFuture.Await()
	netReceiveBytes, _ := netReceiveBytesFuture.Await()
	namespaceAnnotations, _ := namespaceAnnotationsFuture.Await()
	podAnnotations, _ := podAnnotationsFuture.Await()
	nodeLabels, _ := nodeLabelsFuture.Await()
	namespaceLabels, _ := namespaceLabelsFuture.Await()
	podLabels, _ := podLabelsFuture.Await()
	serviceLabels, _ := serviceLabelsFuture.Await()
	deploymentLabels, _ := deploymentLabelsFuture.Await()
	statefulSetLabels, _ := statefulSetLabelsFuture.Await()
	daemonSetLabels, _ := daemonSetLabelsFuture.Await()
	jobLabels, _ := jobLabelsFuture.Await()
	podsWithReplicaSetOwner, _ := podsWithReplicaSetOwnerFuture.Await()
	replicaSetsWithoutOwners, _ := replicaSetsWithoutOwnersFuture.Await()
	replicaSetsWithRollout, _ := replicaSetsWithRolloutFuture.Await()

	if grp.HasErrors() {
		return nil, grp.Error()
	}

	return &MetricsSnapshot{
		Window:                       opencost.NewClosedWindow(start, end),
		PVActiveMinutes:              pvActiveMinutes,
		PVUsedAverage:                pvUsedAverage,
		PVUsedMax:                    pvUsedMax,
		LocalStorageActiveMinutes:    localStorageActiveMinutes,
		LocalStorageCost:             localStorageCost,
		LocalStorageUsedCost:         localStorageUsedCost,
		LocalStorageUsedAvg:          localStorageUsedAvg,
		LocalStorageUsedMax:          localStorageUsedMax,
		LocalStorageBytes:            localStorageBytes,
		NodeActiveMinutes:            nodeActiveMinutes,
		NodeCPUCoresCapacity:         nodeCPUCoresCapacity,
		NodeCPUCoresAllocatable:      nodeCPUCoresAllocatable,
		NodeRAMBytesCapacity:         nodeRAMBytesCapacity,
		NodeRAMBytesAllocatable:      nodeRAMBytesAllocatable,
		NodeGPUCount:                 nodeGPUCount,
		NodeCPUModeTotal:             nodeCPUModeTotal,
		NodeIsSpot:                   nodeIsSpot,
		NodeRAMSystemPercent:         nodeRAMSystemPercent,
		NodeRAMUserPercent:           nodeRAMUserPercent,
		LBActiveMinutes:              lbActiveMinutes,
		LBPricePerHr:                 lbPricePerHr,
		ClusterManagementDuration:    clusterManagementDuration,
		ClusterManagementPricePerHr:  clusterManagementPricePerHr,
		Pods:                         pods,
		PodsUID:                      podsUID,
		RAMBytesAllocated:            ramBytesAllocated,
		RAMRequests:                  ramRequests,
		RAMUsageAvg:                  ramUsageAvg,
		RAMUsageMax:                  ramUsageMax,
		NodeRAMPricePerGiBHr:         nodeRAMPricePerGiBHr,
		CPUCoresAllocated:            cpuCoresAllocated,
		CPURequests:                  cpuRequests,
		CPUUsageAvg:                  cpuUsageAvg,
		CPUUsageMax:                  cpuUsageMax,
		NodeCPUPricePerHr:            nodeCPUPricePerHr,
		GPUsAllocated:                gpusAllocated,
		GPUsRequested:                gpusRequested,
		GPUsUsageAvg:                 gpusUsageAvg,
		GPUsUsageMax:                 gpusUsageMax,
		NodeGPUPricePerHr:            nodeGPUPricePerHr,
		GPUInfo:                      gpuInfo,
		IsGPUShared:                  isGPUShared,
		PodPVCAllocation:             podPVCAllocation,
		PVCBytesRequested:            pvcBytesRequested,
		PVCInfo:                      pvcInfo,
		PVBytes:                      pvBytes,
		PVPricePerGiBHour:            pvPricePerGiBHour,
		PVInfo:                       pvInfo,
		NetZoneGiB:                   netZoneGiB,
		NetZonePricePerGiB:           netZonePricePerGiB,
		NetRegionGiB:                 netRegionGiB,
		NetRegionPricePerGiB:         netRegionPricePerGiB,
		NetInternetGiB:               netInternetGiB,
		NetInternetPricePerGiB:       netInternetPricePerGiB,
		NetInternetServiceGiB:        netInternetServiceGiB,
		NetTransferBytes:             netTransferBytes,
		NetZoneIngressGiB:            netZoneIngressGiB,
		NetRegionIngressGiB:          netRegionIngressGiB,
		NetInternetIngressGiB:        netInternetIngressGiB,
		NetInternetServiceIngressGiB: netInternetServiceIngressGiB,
		NetReceiveBytes:              netReceiveBytes,
		NamespaceAnnotations:         namespaceAnnotations,
		PodAnnotations:               podAnnotations,
		NodeLabels:                   nodeLabels,
		NamespaceLabels:              namespaceLabels,
		PodLabels:                    podLabels,
		ServiceLabels:                serviceLabels,
		DeploymentLabels:             deploymentLabels,
		StatefulSetLabels:            statefulSetLabels,
		DaemonSetLabels:              daemonSetLabels,
		JobLabels:                    jobLabels,
		PodsWithReplicaSetOwner:      podsWithReplicaSetOwner,
		ReplicaSetsWithoutOwners:     replicaSetsWithoutOwners,
		ReplicaSetsWithRollout:       replicaSetsWithRollout,
	}, nil
}

func windowFor(boundary time.Duration) (time.Time, time.Time) {
	now := time.Now().UTC()
	start := now.Truncate(boundary)
	end := start.Add(boundary)
	return start, end
}
