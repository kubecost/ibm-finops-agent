package emitter

import (
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/go-multierror"
	clustercache "github.com/ibm/finops-agent/pkg/cluster"
	"github.com/ibm/finops-agent/pkg/core"
	"github.com/ibm/finops-agent/pkg/nodes"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// metricQueryFailures tracks the number of failed metric queries by query name
	metricQueryFailures = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "finops_agent_metric_query_failures_total",
			Help: "Total number of failed metric queries by query name",
		},
		[]string{"query_name"},
	)
)

// queryFuture is an interface that matches the Await method signature
type queryFuture[T any] interface {
	Await() ([]*T, error)
}

// awaitWithLog awaits a QueryGroupFuture and logs any errors, incrementing the failure counter
// Returns the result slice
func awaitWithLog[T any](name string, future queryFuture[T], failedQueries *[]string) []*T {
	result, err := future.Await()
	if err != nil {
		log.Warnf("metric query failed for %s: %v", name, err)
		metricQueryFailures.WithLabelValues(name).Inc()
		*failedQueries = append(*failedQueries, name)
	}
	return result
}

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
	lastSnapshot       time.Time
	lastMetricsSummary time.Time
	now                Now
}

// NewConcurrentSnapshotProvider creates a new instance of `ConcurrentSnapshotProvider`.
func NewConcurrentSnapshotProvider(config *SnapshotConfig) SnapshotProvider {
	if config == nil {
		config = DefaultSnapshotConfig()
	}

	now := config.Now
	if now == nil {
		now = defaultNow
	}

	return &ConcurrentSnapshotProvider{
		now:    now,
		config: config,
	}
}

// SnapshotOf generates a `ClusterSnapshot` from the provided `core.DataSource` and returns it.
func (csp *ConcurrentSnapshotProvider) SnapshotOf(ds core.DataSource) (*ClusterSnapshot, error) {
	var group multierror.Group
	now := csp.now()

	// we _always_ want to set the last snapshot time upon completion, success or failure
	defer func() {
		csp.lastSnapshot = now
	}()

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
		metricsSnapshot, err = csp.cachedMetricsSummary(ds.Metrics(), now, csp.config)
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
func (csp *ConcurrentSnapshotProvider) cachedMetricsSummary(querier source.MetricsQuerier, now time.Time, config *SnapshotConfig) (*MetricsSummary, error) {
	if !config.UseMetricsCache {
		return snapshotMetricsSummary(querier, now, csp.lastSnapshot, config)
	}

	// FIXME: (bolt) use a metrics summary cache duration of 5 minutes while we're using a prometheus data source.
	// FIXME: (bolt) this should be fine to run on a much faster frequency with a non-promethues metrics querier.
	if !csp.lastMetricsSummary.IsZero() && time.Since(csp.lastMetricsSummary) < metricsSummaryCacheDuration {
		return csp.metricsSummary, nil
	}

	metricsSummary, err := snapshotMetricsSummary(querier, now, csp.lastSnapshot, config)
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

// if the error returned from node stats summary is a multi-error, unwrap and return the inner errors,
// otherwise, just wrap the error in a slice
func unwrapNodeError(err error) []error {
	if multiErr, ok := err.(interface{ Unwrap() []error }); ok {
		return multiErr.Unwrap()
	}
	return []error{err}
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
		// log each node's error as a warning, as we still may have gotten a partial response
		for _, e := range unwrapNodeError(err) {
			log.Warnf("%s", e)
		}

		// only return an error if the data result was empty AND err != nil
		if len(data) == 0 {
			return nil, fmt.Errorf("failed to generate node stats snapshot: %w", err)
		}
	}

	return &NodeStatsSummary{
		Stats: data,
	}, nil
}

func snapshotMetricsSummary(querier source.MetricsQuerier, now time.Time, lastSnapshot time.Time, config *SnapshotConfig) (*MetricsSummary, error) {
	var snapshotErrors []error

	// 10m metrics snapshot
	var minutelySnapshots []*MetricsSnapshot
	if config.MinutelyMetricsEnabled {
		snapshots, errs := snapshotWindowedMetrics(querier, 10*time.Minute, now, lastSnapshot)
		if len(errs) > 0 {
			snapshotErrors = append(snapshotErrors, errs...)
		}

		minutelySnapshots = snapshots
	}

	// 1h metrics snapshot
	hourlySnapshots, errs := snapshotWindowedMetrics(querier, time.Hour, now, lastSnapshot)
	if len(errs) > 0 {
		snapshotErrors = append(snapshotErrors, errs...)
	}

	// 24h metrics snapshot
	dailySnapshots, errs := snapshotWindowedMetrics(querier, 24*time.Hour, now, lastSnapshot)
	if len(errs) > 0 {
		snapshotErrors = append(snapshotErrors, errs...)
	}

	// collect errors and return all errors joined
	var err error
	if len(snapshotErrors) > 0 {
		return nil, errors.Join(snapshotErrors...)
	}

	return &MetricsSummary{
		Minutely: minutelySnapshots,
		Hourly:   hourlySnapshots,
		Daily:    dailySnapshots,
	}, err
}

// snapshots the metrics based on the current snapshot time, and possibly the previous snapshot window if we've rolled into a new time frame.
// for example, if we are snapshotting 10m metrics (9:00-9:10, 9:10-9:20, etc...), and the last snapshot we take is at 9:09:14. The _current_
// time would be 9:10:14 (meaning that the previous snapshot would be missing 56s of data). To account for the moments where the time crosses
// into the next window, we return 2 snapshots: one for the previous _full_ window (9:00:00 - 9:10:00) and one for the current window
// (9:10:00 - 9:10:14).
func snapshotWindowedMetrics(
	querier source.MetricsQuerier,
	resolution time.Duration,
	now time.Time,
	lastSnapshot time.Time,
) ([]*MetricsSnapshot, []error) {
	var snapshots []*MetricsSnapshot
	var errors []error

	// based on the previous snapshot time and the current time, calculate the snapshot windows we should
	// query to ensure we do not omit any data over the elapsed time period
	windows := snapshotWindowsFor(now, lastSnapshot, resolution)
	for _, window := range windows {
		snapshot, err := snapshotMetrics(querier, *window.Start(), *window.End())
		if err != nil {
			errors = append(errors, fmt.Errorf("failed to generate metrics snapshot for resolution: %d minutes: %w", int(resolution.Minutes()), err))
			continue
		}

		snapshots = append(snapshots, snapshot)
	}

	return snapshots, errors
}

// exportWindows uses the last export time to determine the current time windows to
// export. This will, at most, return 2 windows: the previous resolution window and
// the current resolution window.
func snapshotWindowsFor(now time.Time, lastSnapshot time.Time, resolution time.Duration) []opencost.Window {
	start := now.Truncate(resolution)
	end := start.Add(resolution)

	if lastSnapshot.IsZero() {
		return []opencost.Window{
			opencost.NewClosedWindow(start, end),
		}
	}

	lastStart := lastSnapshot.Truncate(resolution)
	if lastStart.Equal(start) {
		return []opencost.Window{
			opencost.NewClosedWindow(start, end),
		}
	}
	lastEnd := lastStart.Add(resolution)

	// we've identified that the last snapshot window is not the same as the current,
	// so we should export the previous resolution window as well as the current one
	return []opencost.Window{
		opencost.NewClosedWindow(lastStart, lastEnd),
		opencost.NewClosedWindow(start, end),
	}
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
	clusterUptimeFuture := source.WithGroup(grp, mq.QueryClusterUptime(start, end))
	clusterManagementDurationFuture := source.WithGroup(grp, mq.QueryClusterManagementDuration(start, end))
	clusterManagementPricePerHrFuture := source.WithGroup(grp, mq.QueryClusterManagementPricePerHr(start, end))
	podsFuture := source.WithGroup(grp, mq.QueryPods(start, end))
	podsUIDFuture := source.WithGroup(grp, mq.QueryPodsUID(start, end))
	ramBytesAllocatedFuture := source.WithGroup(grp, mq.QueryRAMBytesAllocated(start, end))
	ramRequestsFuture := source.WithGroup(grp, mq.QueryRAMRequests(start, end))
	ramLimitsFuture := source.WithGroup(grp, mq.QueryRAMLimits(start, end))
	ramUsageAvgFuture := source.WithGroup(grp, mq.QueryRAMUsageAvg(start, end))
	ramUsageMaxFuture := source.WithGroup(grp, mq.QueryRAMUsageMax(start, end))
	nodeRAMPricePerGiBHrFuture := source.WithGroup(grp, mq.QueryNodeRAMPricePerGiBHr(start, end))
	cpuCoresAllocatedFuture := source.WithGroup(grp, mq.QueryCPUCoresAllocated(start, end))
	cpuRequestsFuture := source.WithGroup(grp, mq.QueryCPURequests(start, end))
	cpuLimitsFuture := source.WithGroup(grp, mq.QueryCPULimits(start, end))
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
	netNatGatewayPricePerGiBFuture := source.WithGroup(grp, mq.QueryNetNatGatewayPricePerGiB(start, end))
	netNatGatewayGiBFuture := source.WithGroup(grp, mq.QueryNetNatGatewayGiB(start, end))
	netTransferBytesFuture := source.WithGroup(grp, mq.QueryNetTransferBytes(start, end))
	netZoneIngressGiBFuture := source.WithGroup(grp, mq.QueryNetZoneIngressGiB(start, end))
	netRegionIngressGiBFuture := source.WithGroup(grp, mq.QueryNetRegionIngressGiB(start, end))
	netInternetIngressGiBFuture := source.WithGroup(grp, mq.QueryNetInternetIngressGiB(start, end))
	netInternetServiceIngressGiBFuture := source.WithGroup(grp, mq.QueryNetInternetServiceIngressGiB(start, end))
	netNatGatewayIngressPricePerGiBFuture := source.WithGroup(grp, mq.QueryNetNatGatewayIngressPricePerGiB(start, end))
	netNatGatewayIngressGiBFuture := source.WithGroup(grp, mq.QueryNetNatGatewayIngressGiB(start, end))
	netReceiveBytesFuture := source.WithGroup(grp, mq.QueryNetReceiveBytes(start, end))
	namespaceUptimeFuture := source.WithGroup(grp, mq.QueryNamespaceUptime(start, end))
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
	resourceQuotaUptimeFuture := source.WithGroup(grp, mq.QueryResourceQuotaUptime(start, end))
	resourceQuotaSpecCpuRequestAvgFuture := source.WithGroup(grp, mq.QueryResourceQuotaSpecCPURequestAverage(start, end))
	resourceQuotaSpecCpuRequestMaxFuture := source.WithGroup(grp, mq.QueryResourceQuotaSpecCPURequestMax(start, end))
	resourceQuotaSpecRamRequestAvgFuture := source.WithGroup(grp, mq.QueryResourceQuotaSpecRAMRequestAverage(start, end))
	resourceQuotaSpecRamRequestMaxFuture := source.WithGroup(grp, mq.QueryResourceQuotaSpecRAMRequestMax(start, end))
	resourceQuotaSpecCpuLimitAvgFuture := source.WithGroup(grp, mq.QueryResourceQuotaSpecCPULimitAverage(start, end))
	resourceQuotaSpecCpuLimitMaxFuture := source.WithGroup(grp, mq.QueryResourceQuotaSpecCPULimitMax(start, end))
	resourceQuotaSpecRamLimitAvgFuture := source.WithGroup(grp, mq.QueryResourceQuotaSpecRAMLimitAverage(start, end))
	resourceQuotaSpecRamLimitMaxFuture := source.WithGroup(grp, mq.QueryResourceQuotaSpecRAMLimitMax(start, end))
	resourceQuotaStatusUsedCpuRequestAvgFuture := source.WithGroup(grp, mq.QueryResourceQuotaStatusUsedCPURequestAverage(start, end))
	resourceQuotaStatusUsedCpuRequestMaxFuture := source.WithGroup(grp, mq.QueryResourceQuotaStatusUsedCPURequestMax(start, end))
	resourceQuotaStatusUsedRamRequestAvgFuture := source.WithGroup(grp, mq.QueryResourceQuotaStatusUsedRAMRequestAverage(start, end))
	resourceQuotaStatusUsedRamRequestMaxFuture := source.WithGroup(grp, mq.QueryResourceQuotaStatusUsedRAMRequestMax(start, end))
	resourceQuotaStatusUsedCpuLimitAvgFuture := source.WithGroup(grp, mq.QueryResourceQuotaStatusUsedCPULimitAverage(start, end))
	resourceQuotaStatusUsedCpuLimitMaxFuture := source.WithGroup(grp, mq.QueryResourceQuotaStatusUsedCPULimitMax(start, end))
	resourceQuotaStatusUsedRamLimitAvgFuture := source.WithGroup(grp, mq.QueryResourceQuotaStatusUsedRAMLimitAverage(start, end))
	resourceQuotaStatusUsedRamLimitMaxFuture := source.WithGroup(grp, mq.QueryResourceQuotaStatusUsedRAMLimitMax(start, end))

	// Track failed queries for observability
	var failedQueries []string

	pvActiveMinutes := awaitWithLog("pvActiveMinutes", pvActiveMinutesFuture, &failedQueries)
	pvUsedAverage := awaitWithLog("pvUsedAverage", pvUsedAverageFuture, &failedQueries)
	pvUsedMax := awaitWithLog("pvUsedMax", pvUsedMaxFuture, &failedQueries)
	localStorageActiveMinutes := awaitWithLog("localStorageActiveMinutes", localStorageActiveMinutesFuture, &failedQueries)
	localStorageCost := awaitWithLog("localStorageCost", localStorageCostFuture, &failedQueries)
	localStorageUsedCost := awaitWithLog("localStorageUsedCost", localStorageUsedCostFuture, &failedQueries)
	localStorageUsedAvg := awaitWithLog("localStorageUsedAvg", localStorageUsedAvgFuture, &failedQueries)
	localStorageUsedMax := awaitWithLog("localStorageUsedMax", localStorageUsedMaxFuture, &failedQueries)
	localStorageBytes := awaitWithLog("localStorageBytes", localStorageBytesFuture, &failedQueries)
	nodeActiveMinutes := awaitWithLog("nodeActiveMinutes", nodeActiveMinutesFuture, &failedQueries)
	nodeCPUCoresCapacity := awaitWithLog("nodeCPUCoresCapacity", nodeCPUCoresCapacityFuture, &failedQueries)
	nodeCPUCoresAllocatable := awaitWithLog("nodeCPUCoresAllocatable", nodeCPUCoresAllocatableFuture, &failedQueries)
	nodeRAMBytesCapacity := awaitWithLog("nodeRAMBytesCapacity", nodeRAMBytesCapacityFuture, &failedQueries)
	nodeRAMBytesAllocatable := awaitWithLog("nodeRAMBytesAllocatable", nodeRAMBytesAllocatableFuture, &failedQueries)
	nodeGPUCount := awaitWithLog("nodeGPUCount", nodeGPUCountFuture, &failedQueries)
	nodeCPUModeTotal := awaitWithLog("nodeCPUModeTotal", nodeCPUModeTotalFuture, &failedQueries)
	nodeIsSpot := awaitWithLog("nodeIsSpot", nodeIsSpotFuture, &failedQueries)
	nodeRAMSystemPercent := awaitWithLog("nodeRAMSystemPercent", nodeRAMSystemPercentFuture, &failedQueries)
	nodeRAMUserPercent := awaitWithLog("nodeRAMUserPercent", nodeRAMUserPercentFuture, &failedQueries)
	lbActiveMinutes := awaitWithLog("lbActiveMinutes", lbActiveMinutesFuture, &failedQueries)
	lbPricePerHr := awaitWithLog("lbPricePerHr", lbPricePerHrFuture, &failedQueries)
	clusterUptime := awaitWithLog("clusterUptime", clusterUptimeFuture, &failedQueries)
	clusterManagementDuration := awaitWithLog("clusterManagementDuration", clusterManagementDurationFuture, &failedQueries)
	clusterManagementPricePerHr := awaitWithLog("clusterManagementPricePerHr", clusterManagementPricePerHrFuture, &failedQueries)
	pods := awaitWithLog("pods", podsFuture, &failedQueries)
	podsUID := awaitWithLog("podsUID", podsUIDFuture, &failedQueries)
	ramBytesAllocated := awaitWithLog("ramBytesAllocated", ramBytesAllocatedFuture, &failedQueries)
	ramRequests := awaitWithLog("ramRequests", ramRequestsFuture, &failedQueries)
	ramLimits := awaitWithLog("ramLimits", ramLimitsFuture, &failedQueries)
	ramUsageAvg := awaitWithLog("ramUsageAvg", ramUsageAvgFuture, &failedQueries)
	ramUsageMax := awaitWithLog("ramUsageMax", ramUsageMaxFuture, &failedQueries)
	nodeRAMPricePerGiBHr := awaitWithLog("nodeRAMPricePerGiBHr", nodeRAMPricePerGiBHrFuture, &failedQueries)
	cpuCoresAllocated := awaitWithLog("cpuCoresAllocated", cpuCoresAllocatedFuture, &failedQueries)
	cpuRequests := awaitWithLog("cpuRequests", cpuRequestsFuture, &failedQueries)
	cpuLimits := awaitWithLog("cpuLimits", cpuLimitsFuture, &failedQueries)
	cpuUsageAvg := awaitWithLog("cpuUsageAvg", cpuUsageAvgFuture, &failedQueries)
	cpuUsageMax := awaitWithLog("cpuUsageMax", cpuUsageMaxFuture, &failedQueries)
	nodeCPUPricePerHr := awaitWithLog("nodeCPUPricePerHr", nodeCPUPricePerHrFuture, &failedQueries)
	gpusAllocated := awaitWithLog("gpusAllocated", gpusAllocatedFuture, &failedQueries)
	gpusRequested := awaitWithLog("gpusRequested", gpusRequestedFuture, &failedQueries)
	gpusUsageAvg := awaitWithLog("gpusUsageAvg", gpusUsageAvgFuture, &failedQueries)
	gpusUsageMax := awaitWithLog("gpusUsageMax", gpusUsageMaxFuture, &failedQueries)
	nodeGPUPricePerHr := awaitWithLog("nodeGPUPricePerHr", nodeGPUPricePerHrFuture, &failedQueries)
	gpuInfo := awaitWithLog("gpuInfo", gpuInfoFuture, &failedQueries)
	isGPUShared := awaitWithLog("isGPUShared", isGPUSharedFuture, &failedQueries)
	podPVCAllocation := awaitWithLog("podPVCAllocation", podPVCAllocationFuture, &failedQueries)
	pvcBytesRequested := awaitWithLog("pvcBytesRequested", pvcBytesRequestedFuture, &failedQueries)
	pvcInfo := awaitWithLog("pvcInfo", pvcInfoFuture, &failedQueries)
	pvBytes := awaitWithLog("pvBytes", pvBytesFuture, &failedQueries)
	pvPricePerGiBHour := awaitWithLog("pvPricePerGiBHour", pvPricePerGiBHourFuture, &failedQueries)
	pvInfo := awaitWithLog("pvInfo", pvInfoFuture, &failedQueries)
	netZoneGiB := awaitWithLog("netZoneGiB", netZoneGiBFuture, &failedQueries)
	netZonePricePerGiB := awaitWithLog("netZonePricePerGiB", netZonePricePerGiBFuture, &failedQueries)
	netRegionGiB := awaitWithLog("netRegionGiB", netRegionGiBFuture, &failedQueries)
	netRegionPricePerGiB := awaitWithLog("netRegionPricePerGiB", netRegionPricePerGiBFuture, &failedQueries)
	netInternetGiB := awaitWithLog("netInternetGiB", netInternetGiBFuture, &failedQueries)
	netInternetPricePerGiB := awaitWithLog("netInternetPricePerGiB", netInternetPricePerGiBFuture, &failedQueries)
	netInternetServiceGiB := awaitWithLog("netInternetServiceGiB", netInternetServiceGiBFuture, &failedQueries)
	netNatGatewayPricePerGiB := awaitWithLog("netNatGatewayPricePerGiB", netNatGatewayPricePerGiBFuture, &failedQueries)
	netNatGatewayGiB := awaitWithLog("netNatGatewayGiB", netNatGatewayGiBFuture, &failedQueries)
	netTransferBytes := awaitWithLog("netTransferBytes", netTransferBytesFuture, &failedQueries)
	netZoneIngressGiB := awaitWithLog("netZoneIngressGiB", netZoneIngressGiBFuture, &failedQueries)
	netRegionIngressGiB := awaitWithLog("netRegionIngressGiB", netRegionIngressGiBFuture, &failedQueries)
	netInternetIngressGiB := awaitWithLog("netInternetIngressGiB", netInternetIngressGiBFuture, &failedQueries)
	netInternetServiceIngressGiB := awaitWithLog("netInternetServiceIngressGiB", netInternetServiceIngressGiBFuture, &failedQueries)
	netNatGatewayIngressPricePerGiB := awaitWithLog("netNatGatewayIngressPricePerGiB", netNatGatewayIngressPricePerGiBFuture, &failedQueries)
	netNatGatewayIngressGiB := awaitWithLog("netNatGatewayIngressGiB", netNatGatewayIngressGiBFuture, &failedQueries)
	netReceiveBytes := awaitWithLog("netReceiveBytes", netReceiveBytesFuture, &failedQueries)
	namespaceUptime := awaitWithLog("namespaceUptime", namespaceUptimeFuture, &failedQueries)
	namespaceAnnotations := awaitWithLog("namespaceAnnotations", namespaceAnnotationsFuture, &failedQueries)
	podAnnotations := awaitWithLog("podAnnotations", podAnnotationsFuture, &failedQueries)
	nodeLabels := awaitWithLog("nodeLabels", nodeLabelsFuture, &failedQueries)
	namespaceLabels := awaitWithLog("namespaceLabels", namespaceLabelsFuture, &failedQueries)
	podLabels := awaitWithLog("podLabels", podLabelsFuture, &failedQueries)
	serviceLabels := awaitWithLog("serviceLabels", serviceLabelsFuture, &failedQueries)
	deploymentLabels := awaitWithLog("deploymentLabels", deploymentLabelsFuture, &failedQueries)
	statefulSetLabels := awaitWithLog("statefulSetLabels", statefulSetLabelsFuture, &failedQueries)
	daemonSetLabels := awaitWithLog("daemonSetLabels", daemonSetLabelsFuture, &failedQueries)
	jobLabels := awaitWithLog("jobLabels", jobLabelsFuture, &failedQueries)
	podsWithReplicaSetOwner := awaitWithLog("podsWithReplicaSetOwner", podsWithReplicaSetOwnerFuture, &failedQueries)
	replicaSetsWithoutOwners := awaitWithLog("replicaSetsWithoutOwners", replicaSetsWithoutOwnersFuture, &failedQueries)
	replicaSetsWithRollout := awaitWithLog("replicaSetsWithRollout", replicaSetsWithRolloutFuture, &failedQueries)
	resourceQuotaUptime := awaitWithLog("resourceQuotaUptime", resourceQuotaUptimeFuture, &failedQueries)
	resourceQuotaSpecCpuRequestAvg := awaitWithLog("resourceQuotaSpecCpuRequestAvg", resourceQuotaSpecCpuRequestAvgFuture, &failedQueries)
	resourceQuotaSpecCpuRequestMax := awaitWithLog("resourceQuotaSpecCpuRequestMax", resourceQuotaSpecCpuRequestMaxFuture, &failedQueries)
	resourceQuotaSpecRamRequestAvg := awaitWithLog("resourceQuotaSpecRamRequestAvg", resourceQuotaSpecRamRequestAvgFuture, &failedQueries)
	resourceQuotaSpecRamRequestMax := awaitWithLog("resourceQuotaSpecRamRequestMax", resourceQuotaSpecRamRequestMaxFuture, &failedQueries)
	resourceQuotaSpecCpuLimitAvg := awaitWithLog("resourceQuotaSpecCpuLimitAvg", resourceQuotaSpecCpuLimitAvgFuture, &failedQueries)
	resourceQuotaSpecCpuLimitMax := awaitWithLog("resourceQuotaSpecCpuLimitMax", resourceQuotaSpecCpuLimitMaxFuture, &failedQueries)
	resourceQuotaSpecRamLimitAvg := awaitWithLog("resourceQuotaSpecRamLimitAvg", resourceQuotaSpecRamLimitAvgFuture, &failedQueries)
	resourceQuotaSpecRamLimitMax := awaitWithLog("resourceQuotaSpecRamLimitMax", resourceQuotaSpecRamLimitMaxFuture, &failedQueries)
	resourceQuotaStatusUsedCpuRequestAvg := awaitWithLog("resourceQuotaStatusUsedCpuRequestAvg", resourceQuotaStatusUsedCpuRequestAvgFuture, &failedQueries)
	resourceQuotaStatusUsedCpuRequestMax := awaitWithLog("resourceQuotaStatusUsedCpuRequestMax", resourceQuotaStatusUsedCpuRequestMaxFuture, &failedQueries)
	resourceQuotaStatusUsedRamRequestAvg := awaitWithLog("resourceQuotaStatusUsedRamRequestAvg", resourceQuotaStatusUsedRamRequestAvgFuture, &failedQueries)
	resourceQuotaStatusUsedRamRequestMax := awaitWithLog("resourceQuotaStatusUsedRamRequestMax", resourceQuotaStatusUsedRamRequestMaxFuture, &failedQueries)
	resourceQuotaStatusUsedCpuLimitAvg := awaitWithLog("resourceQuotaStatusUsedCpuLimitAvg", resourceQuotaStatusUsedCpuLimitAvgFuture, &failedQueries)
	resourceQuotaStatusUsedCpuLimitMax := awaitWithLog("resourceQuotaStatusUsedCpuLimitMax", resourceQuotaStatusUsedCpuLimitMaxFuture, &failedQueries)
	resourceQuotaStatusUsedRamLimitAvg := awaitWithLog("resourceQuotaStatusUsedRamLimitAvg", resourceQuotaStatusUsedRamLimitAvgFuture, &failedQueries)
	resourceQuotaStatusUsedRamLimitMax := awaitWithLog("resourceQuotaStatusUsedRamLimitMax", resourceQuotaStatusUsedRamLimitMaxFuture, &failedQueries)

	if grp.HasErrors() {
		return nil, grp.Error()
	}

	return &MetricsSnapshot{
		Window:                               opencost.NewClosedWindow(start, end),
		FailedQueries:                        failedQueries,
		PVActiveMinutes:                      pvActiveMinutes,
		PVUsedAverage:                        pvUsedAverage,
		PVUsedMax:                            pvUsedMax,
		LocalStorageActiveMinutes:            localStorageActiveMinutes,
		LocalStorageCost:                     localStorageCost,
		LocalStorageUsedCost:                 localStorageUsedCost,
		LocalStorageUsedAvg:                  localStorageUsedAvg,
		LocalStorageUsedMax:                  localStorageUsedMax,
		LocalStorageBytes:                    localStorageBytes,
		NodeActiveMinutes:                    nodeActiveMinutes,
		NodeCPUCoresCapacity:                 nodeCPUCoresCapacity,
		NodeCPUCoresAllocatable:              nodeCPUCoresAllocatable,
		NodeRAMBytesCapacity:                 nodeRAMBytesCapacity,
		NodeRAMBytesAllocatable:              nodeRAMBytesAllocatable,
		NodeGPUCount:                         nodeGPUCount,
		NodeCPUModeTotal:                     nodeCPUModeTotal,
		NodeIsSpot:                           nodeIsSpot,
		NodeRAMSystemPercent:                 nodeRAMSystemPercent,
		NodeRAMUserPercent:                   nodeRAMUserPercent,
		LBActiveMinutes:                      lbActiveMinutes,
		LBPricePerHr:                         lbPricePerHr,
		ClusterUptime:                        clusterUptime,
		ClusterManagementDuration:            clusterManagementDuration,
		ClusterManagementPricePerHr:          clusterManagementPricePerHr,
		Pods:                                 pods,
		PodsUID:                              podsUID,
		RAMBytesAllocated:                    ramBytesAllocated,
		RAMRequests:                          ramRequests,
		RAMLimits:                            ramLimits,
		RAMUsageAvg:                          ramUsageAvg,
		RAMUsageMax:                          ramUsageMax,
		NodeRAMPricePerGiBHr:                 nodeRAMPricePerGiBHr,
		CPUCoresAllocated:                    cpuCoresAllocated,
		CPURequests:                          cpuRequests,
		CPULimits:                            cpuLimits,
		CPUUsageAvg:                          cpuUsageAvg,
		CPUUsageMax:                          cpuUsageMax,
		NodeCPUPricePerHr:                    nodeCPUPricePerHr,
		GPUsAllocated:                        gpusAllocated,
		GPUsRequested:                        gpusRequested,
		GPUsUsageAvg:                         gpusUsageAvg,
		GPUsUsageMax:                         gpusUsageMax,
		NodeGPUPricePerHr:                    nodeGPUPricePerHr,
		GPUInfo:                              gpuInfo,
		IsGPUShared:                          isGPUShared,
		PodPVCAllocation:                     podPVCAllocation,
		PVCBytesRequested:                    pvcBytesRequested,
		PVCInfo:                              pvcInfo,
		PVBytes:                              pvBytes,
		PVPricePerGiBHour:                    pvPricePerGiBHour,
		PVInfo:                               pvInfo,
		NetZoneGiB:                           netZoneGiB,
		NetZonePricePerGiB:                   netZonePricePerGiB,
		NetRegionGiB:                         netRegionGiB,
		NetRegionPricePerGiB:                 netRegionPricePerGiB,
		NetInternetGiB:                       netInternetGiB,
		NetInternetPricePerGiB:               netInternetPricePerGiB,
		NetInternetServiceGiB:                netInternetServiceGiB,
		NetNatGatewayPricePerGiB:             netNatGatewayPricePerGiB,
		NetNatGatewayGiB:                     netNatGatewayGiB,
		NetTransferBytes:                     netTransferBytes,
		NetZoneIngressGiB:                    netZoneIngressGiB,
		NetRegionIngressGiB:                  netRegionIngressGiB,
		NetInternetIngressGiB:                netInternetIngressGiB,
		NetInternetServiceIngressGiB:         netInternetServiceIngressGiB,
		NetNatGatewayIngressPricePerGiB:      netNatGatewayIngressPricePerGiB,
		NetNatGatewayIngressGiB:              netNatGatewayIngressGiB,
		NetReceiveBytes:                      netReceiveBytes,
		NamespaceUptime:                      namespaceUptime,
		NamespaceAnnotations:                 namespaceAnnotations,
		PodAnnotations:                       podAnnotations,
		NodeLabels:                           nodeLabels,
		NamespaceLabels:                      namespaceLabels,
		PodLabels:                            podLabels,
		ServiceLabels:                        serviceLabels,
		DeploymentLabels:                     deploymentLabels,
		StatefulSetLabels:                    statefulSetLabels,
		DaemonSetLabels:                      daemonSetLabels,
		JobLabels:                            jobLabels,
		PodsWithReplicaSetOwner:              podsWithReplicaSetOwner,
		ReplicaSetsWithoutOwners:             replicaSetsWithoutOwners,
		ReplicaSetsWithRollout:               replicaSetsWithRollout,
		ResourceQuotaUptime:                  resourceQuotaUptime,
		ResourceQuotaSpecCPURequestAvg:       resourceQuotaSpecCpuRequestAvg,
		ResourceQuotaSpecCPURequestMax:       resourceQuotaSpecCpuRequestMax,
		ResourceQuotaSpecRAMRequestAvg:       resourceQuotaSpecRamRequestAvg,
		ResourceQuotaSpecRAMRequestMax:       resourceQuotaSpecRamRequestMax,
		ResourceQuotaSpecCPULimitAvg:         resourceQuotaSpecCpuLimitAvg,
		ResourceQuotaSpecCPULimitMax:         resourceQuotaSpecCpuLimitMax,
		ResourceQuotaSpecRAMLimitAvg:         resourceQuotaSpecRamLimitAvg,
		ResourceQuotaSpecRAMLimitMax:         resourceQuotaSpecRamLimitMax,
		ResourceQuotaStatusUsedCPURequestAvg: resourceQuotaStatusUsedCpuRequestAvg,
		ResourceQuotaStatusUsedCPURequestMax: resourceQuotaStatusUsedCpuRequestMax,
		ResourceQuotaStatusUsedRAMRequestAvg: resourceQuotaStatusUsedRamRequestAvg,
		ResourceQuotaStatusUsedRAMRequestMax: resourceQuotaStatusUsedRamRequestMax,
		ResourceQuotaStatusUsedCPULimitAvg:   resourceQuotaStatusUsedCpuLimitAvg,
		ResourceQuotaStatusUsedCPULimitMax:   resourceQuotaStatusUsedCpuLimitMax,
		ResourceQuotaStatusUsedRAMLimitAvg:   resourceQuotaStatusUsedRamLimitAvg,
		ResourceQuotaStatusUsedRAMLimitMax:   resourceQuotaStatusUsedRamLimitMax,
	}, nil
}
