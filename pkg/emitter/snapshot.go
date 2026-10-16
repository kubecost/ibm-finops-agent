package emitter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	clustercache "github.com/ibm/finops-agent/pkg/cluster"
	"github.com/ibm/finops-agent/pkg/core"
	"github.com/ibm/finops-agent/pkg/nodes"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
	corev1 "k8s.io/api/core/v1"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
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
//
// Metrics windows are planned from per-resolution watermarks, which advance only when the
// exporter reports a snapshot's metrics delivered (CommitWindows). The exporter runs at most two
// snapshots at once (one that outlived its deadline plus the current one), so the fields under mu
// are read at the start of a snapshot and only ever advanced.
type ConcurrentSnapshotProvider struct {
	config *SnapshotConfig
	now    Now

	mu            sync.Mutex
	metricsCache  *cachedMetrics
	watermarks    map[time.Duration]time.Time
	windowGaps    map[time.Duration]uint64
	persistMu     sync.Mutex
	persistErr    error
	discardedPods atomic.Uint64
}

// cachedMetrics is a metrics summary kept for metricsSummaryCacheDuration in Prometheus mode, with
// the windows it covers and when it was taken.
type cachedMetrics struct {
	summary *MetricsSummary
	windows []windowCommit
	at      time.Time
}

// DiscardedShortLivedPods returns the number of short-lived pods drained from a cluster cache that
// can't be peeked by snapshots that then failed, so they were never emitted.
func (csp *ConcurrentSnapshotProvider) DiscardedShortLivedPods() uint64 {
	return csp.discardedPods.Load()
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

	// set the default here rather than in snapshotKubernetes, which may run concurrently
	if config.KubernetesSnapshot == nil {
		config.KubernetesSnapshot = NewKubernetesSnapshotConfig().EnableAll()
	}

	csp := &ConcurrentSnapshotProvider{
		now:        now,
		config:     config,
		watermarks: map[time.Duration]time.Time{},
		windowGaps: map[time.Duration]uint64{},
	}
	csp.loadWatermarks()
	return csp
}

// SnapshotOf generates a `ClusterSnapshot` from the provided `core.DataSource` and returns it.
func (csp *ConcurrentSnapshotProvider) SnapshotOf(ds core.DataSource) (*ClusterSnapshot, error) {
	return csp.SnapshotOfContext(context.Background(), ds)
}

// SnapshotOfContext generates a `ClusterSnapshot` like SnapshotOf. ctx bounds the node-stats
// collection, which returns partial results at its deadline; the other components don't take
// a context yet.
//
// The components are collected independently. A component that fails (or panics) is left nil
// and recorded in ComponentErrors; the snapshot is still returned. Only when every component
// fails does SnapshotOfContext return an error.
func (csp *ConcurrentSnapshotProvider) SnapshotOfContext(ctx context.Context, ds core.DataSource) (*ClusterSnapshot, error) {
	now := csp.now()

	var (
		wg              sync.WaitGroup
		errs            [4]error
		clusterInfo     *clusters.ClusterInfo
		k8sSnapshot     *KubernetesSnapshot
		nodeStats       *NodeStatsSummary
		metricsSnapshot *MetricsSummary
		windows         []windowCommit
	)
	collect := func(i int, f func() error) {
		wg.Go(func() {
			defer func() {
				if r := recover(); r != nil {
					errs[i] = panicError(r)
				}
			}()
			errs[i] = f()
		})
	}

	// indexes follow AllComponents
	collect(0, func() (err error) {
		clusterInfo, err = snapshotClusterInfo(ds.OpenCostSource().ClusterInfo())
		return err
	})
	collect(1, func() (err error) {
		k8sSnapshot, err = snapshotKubernetes(ds.Cluster(), csp.config)
		return err
	})
	collect(2, func() (err error) {
		nodeStats, err = snapshotNodeStats(ctx, ds.StatsSummary())
		return err
	})
	collect(3, func() (err error) {
		metricsSnapshot, windows, err = csp.cachedMetricsSummary(ds.Metrics(), now, csp.config)
		return err
	})
	wg.Wait()

	var componentErrors map[SnapshotComponent]error
	for i, err := range errs {
		if err == nil {
			continue
		}
		if componentErrors == nil {
			componentErrors = map[SnapshotComponent]error{}
		}
		componentErrors[AllComponents[i]] = err
	}

	if len(componentErrors) == len(AllComponents) {
		// A drained short-lived-pod buffer is lost with the snapshot; count and report it (I1).
		if k8sSnapshot != nil && k8sSnapshot.shortLivedPodsDrained && len(k8sSnapshot.ShortLivedPods) > 0 {
			dropped := len(k8sSnapshot.ShortLivedPods)
			csp.discardedPods.Add(uint64(dropped))
			log.Errorf("snapshot failed after draining %d short-lived pods; they are dropped", dropped)
		}
		return nil, fmt.Errorf("failed to generate cluster snapshot: %w", errors.Join(errs[:]...))
	}

	return &ClusterSnapshot{
		ClusterInfo:     clusterInfo,
		Kubernetes:      k8sSnapshot,
		NodeStats:       nodeStats,
		Metrics:         metricsSnapshot,
		ComponentErrors: componentErrors,
		windows:         windows,
	}, nil
}

// cachedMetricsSummary returns the metrics summary for now, with the windows it covers. In
// Prometheus mode (UseMetricsCache) the summary is cached for metricsSummaryCacheDuration to
// avoid overloading Prometheus, but never across a window rollover: the first snapshot after any
// resolution's window closes re-queries, so every closed window's final snapshot is taken at or
// after its end.
func (csp *ConcurrentSnapshotProvider) cachedMetricsSummary(querier source.MetricsQuerier, now time.Time, config *SnapshotConfig) (*MetricsSummary, []windowCommit, error) {
	if config.UseMetricsCache {
		csp.mu.Lock()
		cached := csp.metricsCache
		csp.mu.Unlock()
		if cached != nil && now.Sub(cached.at) < metricsSummaryCacheDuration && !rolledOver(cached.windows, now) {
			return cached.summary, cached.windows, nil
		}
	}

	summary, windows, err := csp.snapshotMetricsSummary(querier, now, config)
	if err != nil {
		return nil, nil, err
	}

	if config.UseMetricsCache {
		// A snapshot abandoned by the exporter may finish after a newer one; never move the cache back.
		csp.mu.Lock()
		if csp.metricsCache == nil || now.After(csp.metricsCache.at) {
			csp.metricsCache = &cachedMetrics{summary: summary, windows: windows, at: now}
		}
		csp.mu.Unlock()
	}

	return summary, windows, nil
}

// rolledOver reports whether any resolution's current window has changed since windows were
// planned.
func rolledOver(windows []windowCommit, now time.Time) bool {
	for _, wc := range windows {
		if !now.Truncate(wc.resolution).Equal(wc.current) {
			return true
		}
	}
	return false
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
	kconfig := config.KubernetesSnapshot
	if kconfig == nil {
		kconfig = NewKubernetesSnapshotConfig().EnableAll()
	}

	// Short-lived pods stay buffered until the emitter that writes them commits them (see
	// ShortLivedPodWriter). A cache that can't be peeked is drained, as before.
	var shortLivedPods []*corev1.Pod
	drained := false
	if buffer, ok := cluster.(clustercache.ShortLivedPodBuffer); ok {
		shortLivedPods = buffer.PeekShortLivedPods()
	} else {
		shortLivedPods = cluster.GetAllShortLivedPods()
		drained = true
	}

	return &KubernetesSnapshot{
		Nodes:                  snapshotResource(kconfig.Nodes, cluster.GetAllNodes),
		Pods:                   snapshotResource(kconfig.Pods, cluster.GetAllPods),
		ShortLivedPods:         shortLivedPods,
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
		shortLivedPodsDrained:  drained,
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

func snapshotNodeStats(ctx context.Context, client nodes.StatSummaryClient) (*NodeStatsSummary, error) {
	var data []*stats.Summary
	var err error
	if cc, ok := client.(nodes.ContextStatSummaryClient); ok {
		data, err = cc.GetNodeDataContext(ctx)
	} else {
		data, err = client.GetNodeData()
	}
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
		Stats:         data,
		CollectionErr: err,
	}, nil
}

// metricsResolutions returns the resolutions snapshotted under config.
func metricsResolutions(config *SnapshotConfig) []time.Duration {
	if config.MinutelyMetricsEnabled {
		return []time.Duration{10 * time.Minute, time.Hour, 24 * time.Hour}
	}
	return []time.Duration{time.Hour, 24 * time.Hour}
}

// snapshotMetricsSummary queries every window planned from the watermarks at each resolution. It
// fails if any window fails, so the watermarks only advance for a complete summary.
func (csp *ConcurrentSnapshotProvider) snapshotMetricsSummary(querier source.MetricsQuerier, now time.Time, config *SnapshotConfig) (*MetricsSummary, []windowCommit, error) {
	var snapshotErrors []error
	var plans []windowCommit
	summary := &MetricsSummary{}

	for _, resolution := range metricsResolutions(config) {
		csp.mu.Lock()
		watermark := csp.watermarks[resolution]
		csp.mu.Unlock()

		windows, plan := planWindows(now, watermark, resolution)
		snapshots, errs := snapshotWindowedMetrics(querier, resolution, windows)
		snapshotErrors = append(snapshotErrors, errs...)
		plans = append(plans, plan)

		switch resolution {
		case 10 * time.Minute:
			summary.Minutely = snapshots
		case time.Hour:
			summary.Hourly = snapshots
		default:
			summary.Daily = snapshots
		}
	}

	if len(snapshotErrors) > 0 {
		return nil, nil, errors.Join(snapshotErrors...)
	}
	return summary, plans, nil
}

// snapshotWindowedMetrics queries each of windows at resolution.
func snapshotWindowedMetrics(
	querier source.MetricsQuerier,
	resolution time.Duration,
	windows []opencost.Window,
) ([]*MetricsSnapshot, []error) {
	var snapshots []*MetricsSnapshot
	var errors []error

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

func snapshotMetrics(mq source.MetricsQuerier, start, end time.Time) (*MetricsSnapshot, error) {
	grp := source.NewQueryGroup()

	kmPVInfoFuture := source.WithGroup(grp, mq.QueryKMPVInfo(start, end))
	pvcBytesUsedAvgFuture := source.WithGroup(grp, mq.QueryPVCBytesUsedAverage(start, end))
	pvcBytesUsedMaxFuture := source.WithGroup(grp, mq.QueryPVCBytesUsedMax(start, end))
	pvActiveMinutesFuture := source.WithGroup(grp, mq.QueryPVActiveMinutes(start, end))
	pvUsedAverageFuture := source.WithGroup(grp, mq.QueryPVUsedAverage(start, end))
	pvUsedMaxFuture := source.WithGroup(grp, mq.QueryPVUsedMax(start, end))
	localStorageActiveMinutesFuture := source.WithGroup(grp, mq.QueryLocalStorageActiveMinutes(start, end))
	localStorageUsedAvgFuture := source.WithGroup(grp, mq.QueryLocalStorageUsedAvg(start, end))
	localStorageUsedMaxFuture := source.WithGroup(grp, mq.QueryLocalStorageUsedMax(start, end))
	localStorageBytesFuture := source.WithGroup(grp, mq.QueryLocalStorageBytes(start, end))
	kmLocalStorageUsedAvgFuture := source.WithGroup(grp, mq.QueryKMLocalStorageUsedAvg(start, end))
	kmLocalStorageUsedMaxFuture := source.WithGroup(grp, mq.QueryKMLocalStorageUsedMax(start, end))
	kmLocalStorageBytesFuture := source.WithGroup(grp, mq.QueryKMLocalStorageBytes(start, end))
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
	clusterInfoFuture := source.WithGroup(grp, mq.QueryClusterInfo(start, end))
	clusterKubeModelVersionFuture := source.WithGroup(grp, mq.QueryClusterKubeModelVersion(start, end))
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
	kmPVCInfoFuture := source.WithGroup(grp, mq.QueryKMPVCInfo(start, end))
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
	nodeInfoFuture := source.WithGroup(grp, mq.QueryNodeInfo(start, end))
	nodeUptimeFuture := source.WithGroup(grp, mq.QueryNodeUptime(start, end))
	nodeResourceCapacitiesFuture := source.WithGroup(grp, mq.QueryNodeResourceCapacities(start, end))
	nodeResourcesAllocatableFuture := source.WithGroup(grp, mq.QueryNodeResourcesAllocatable(start, end))
	podInfoFuture := source.WithGroup(grp, mq.QueryPodInfo(start, end))
	podUptimeFuture := source.WithGroup(grp, mq.QueryPodUptime(start, end))
	podOwnersFuture := source.WithGroup(grp, mq.QueryPodOwners(start, end))
	podPVCVolumesFuture := source.WithGroup(grp, mq.QueryPodPVCVolumes(start, end))
	podNetworkEgressBytesFuture := source.WithGroup(grp, mq.QueryPodNetworkEgressBytes(start, end))
	podNetworkIngressBytesFuture := source.WithGroup(grp, mq.QueryPodNetworkIngressBytes(start, end))
	containerUptimeFuture := source.WithGroup(grp, mq.QueryContainerUptime(start, end))
	containerResourceRequestsFuture := source.WithGroup(grp, mq.QueryContainerResourceRequests(start, end))
	containerResourceLimitsFuture := source.WithGroup(grp, mq.QueryContainerResourceLimits(start, end))
	dcgmDeviceInfoFuture := source.WithGroup(grp, mq.QueryDCGMDeviceInfo(start, end))
	dcgmDeviceUptimeFuture := source.WithGroup(grp, mq.QueryDCGMDeviceUptime(start, end))
	dcgmContainerUsageAvgFuture := source.WithGroup(grp, mq.QueryDCGMContainerUsageAvg(start, end))
	dcgmContainerUsageMaxFuture := source.WithGroup(grp, mq.QueryDCGMContainerUsageMax(start, end))
	pvcUptimeFuture := source.WithGroup(grp, mq.QueryPVCUptime(start, end))
	pvUptimeFuture := source.WithGroup(grp, mq.QueryPVUptime(start, end))
	deploymentInfoFuture := source.WithGroup(grp, mq.QueryDeploymentInfo(start, end))
	deploymentUptimeFuture := source.WithGroup(grp, mq.QueryDeploymentUptime(start, end))
	deploymentAnnotationsFuture := source.WithGroup(grp, mq.QueryDeploymentAnnotations(start, end))
	deploymentMatchLabelsFuture := source.WithGroup(grp, mq.QueryDeploymentMatchLabels(start, end))
	statefulSetInfoFuture := source.WithGroup(grp, mq.QueryStatefulSetInfo(start, end))
	statefulSetUptimeFuture := source.WithGroup(grp, mq.QueryStatefulSetUptime(start, end))
	statefulSetAnnotationsFuture := source.WithGroup(grp, mq.QueryStatefulSetAnnotations(start, end))
	statefulSetMatchLabelsFuture := source.WithGroup(grp, mq.QueryStatefulSetMatchLabels(start, end))
	daemonSetInfoFuture := source.WithGroup(grp, mq.QueryDaemonSetInfo(start, end))
	daemonSetUptimeFuture := source.WithGroup(grp, mq.QueryDaemonSetUptime(start, end))
	daemonSetAnnotationsFuture := source.WithGroup(grp, mq.QueryDaemonSetAnnotations(start, end))
	daemonSetArgumentsFuture := source.WithGroup(grp, mq.QueryDaemonSetArguments(start, end))
	jobInfoFuture := source.WithGroup(grp, mq.QueryJobInfo(start, end))
	jobUptimeFuture := source.WithGroup(grp, mq.QueryJobUptime(start, end))
	jobAnnotationsFuture := source.WithGroup(grp, mq.QueryJobAnnotations(start, end))
	cronJobInfoFuture := source.WithGroup(grp, mq.QueryCronJobInfo(start, end))
	cronJobUptimeFuture := source.WithGroup(grp, mq.QueryCronJobUptime(start, end))
	cronJobLabelsFuture := source.WithGroup(grp, mq.QueryCronJobLabels(start, end))
	cronJobAnnotationsFuture := source.WithGroup(grp, mq.QueryCronJobAnnotations(start, end))
	replicaSetInfoFuture := source.WithGroup(grp, mq.QueryReplicaSetInfo(start, end))
	replicaSetUptimeFuture := source.WithGroup(grp, mq.QueryReplicaSetUptime(start, end))
	replicaSetLabelsFuture := source.WithGroup(grp, mq.QueryReplicaSetLabels(start, end))
	replicaSetAnnotationsFuture := source.WithGroup(grp, mq.QueryReplicaSetAnnotations(start, end))
	replicaSetOwnersFuture := source.WithGroup(grp, mq.QueryReplicaSetOwners(start, end))
	namespaceInfoFuture := source.WithGroup(grp, mq.QueryNamespaceInfo(start, end))
	serviceInfoFuture := source.WithGroup(grp, mq.QueryServiceInfo(start, end))
	serviceUptimeFuture := source.WithGroup(grp, mq.QueryServiceUptime(start, end))
	serviceSelectorLabelsFuture := source.WithGroup(grp, mq.QueryServiceSelectorLabels(start, end))
	podsWithDaemonSetOwnerFuture := source.WithGroup(grp, mq.QueryPodsWithDaemonSetOwner(start, end))
	podsWithJobOwnerFuture := source.WithGroup(grp, mq.QueryPodsWithJobOwner(start, end))
	resourceQuotaInfoFuture := source.WithGroup(grp, mq.QueryResourceQuotaInfo(start, end))

	kmPVInfo, _ := kmPVInfoFuture.Await()
	pvcBytesUsedAvg, _ := pvcBytesUsedAvgFuture.Await()
	pvcBytesUsedMax, _ := pvcBytesUsedMaxFuture.Await()
	pvActiveMinutes, _ := pvActiveMinutesFuture.Await()
	pvUsedAverage, _ := pvUsedAverageFuture.Await()
	pvUsedMax, _ := pvUsedMaxFuture.Await()
	localStorageActiveMinutes, _ := localStorageActiveMinutesFuture.Await()
	localStorageUsedAvg, _ := localStorageUsedAvgFuture.Await()
	localStorageUsedMax, _ := localStorageUsedMaxFuture.Await()
	localStorageBytes, _ := localStorageBytesFuture.Await()
	kmLocalStorageUsedAvg, _ := kmLocalStorageUsedAvgFuture.Await()
	kmLocalStorageUsedMax, _ := kmLocalStorageUsedMaxFuture.Await()
	kmLocalStorageBytes, _ := kmLocalStorageBytesFuture.Await()
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
	clusterInfo, _ := clusterInfoFuture.Await()
	clusterKubeModelVersion, _ := clusterKubeModelVersionFuture.Await()
	clusterUptime, _ := clusterUptimeFuture.Await()
	clusterManagementDuration, _ := clusterManagementDurationFuture.Await()
	clusterManagementPricePerHr, _ := clusterManagementPricePerHrFuture.Await()
	pods, _ := podsFuture.Await()
	podsUID, _ := podsUIDFuture.Await()
	ramBytesAllocated, _ := ramBytesAllocatedFuture.Await()
	ramRequests, _ := ramRequestsFuture.Await()
	ramLimits, _ := ramLimitsFuture.Await()
	ramUsageAvg, _ := ramUsageAvgFuture.Await()
	ramUsageMax, _ := ramUsageMaxFuture.Await()
	nodeRAMPricePerGiBHr, _ := nodeRAMPricePerGiBHrFuture.Await()
	cpuCoresAllocated, _ := cpuCoresAllocatedFuture.Await()
	cpuRequests, _ := cpuRequestsFuture.Await()
	cpuLimits, _ := cpuLimitsFuture.Await()
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
	kmPVCInfo, _ := kmPVCInfoFuture.Await()
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
	netNatGatewayPricePerGiB, _ := netNatGatewayPricePerGiBFuture.Await()
	netNatGatewayGiB, _ := netNatGatewayGiBFuture.Await()
	netTransferBytes, _ := netTransferBytesFuture.Await()
	netZoneIngressGiB, _ := netZoneIngressGiBFuture.Await()
	netRegionIngressGiB, _ := netRegionIngressGiBFuture.Await()
	netInternetIngressGiB, _ := netInternetIngressGiBFuture.Await()
	netInternetServiceIngressGiB, _ := netInternetServiceIngressGiBFuture.Await()
	netNatGatewayIngressPricePerGiB, _ := netNatGatewayIngressPricePerGiBFuture.Await()
	netNatGatewayIngressGiB, _ := netNatGatewayIngressGiBFuture.Await()
	netReceiveBytes, _ := netReceiveBytesFuture.Await()
	namespaceUptime, _ := namespaceUptimeFuture.Await()
	namespaceAnnotations, _ := namespaceAnnotationsFuture.Await()
	podAnnotations, _ := podAnnotationsFuture.Await()
	nodeLabels, _ := nodeLabelsFuture.Await()
	namespaceLabels, _ := namespaceLabelsFuture.Await()
	podLabels, _ := podLabelsFuture.Await()
	deploymentLabels, _ := deploymentLabelsFuture.Await()
	statefulSetLabels, _ := statefulSetLabelsFuture.Await()
	daemonSetLabels, _ := daemonSetLabelsFuture.Await()
	jobLabels, _ := jobLabelsFuture.Await()
	podsWithReplicaSetOwner, _ := podsWithReplicaSetOwnerFuture.Await()
	replicaSetsWithoutOwners, _ := replicaSetsWithoutOwnersFuture.Await()
	replicaSetsWithRollout, _ := replicaSetsWithRolloutFuture.Await()
	resourceQuotaUptime, _ := resourceQuotaUptimeFuture.Await()
	resourceQuotaSpecCpuRequestAvg, _ := resourceQuotaSpecCpuRequestAvgFuture.Await()
	resourceQuotaSpecCpuRequestMax, _ := resourceQuotaSpecCpuRequestMaxFuture.Await()
	resourceQuotaSpecRamRequestAvg, _ := resourceQuotaSpecRamRequestAvgFuture.Await()
	resourceQuotaSpecRamRequestMax, _ := resourceQuotaSpecRamRequestMaxFuture.Await()
	resourceQuotaSpecCpuLimitAvg, _ := resourceQuotaSpecCpuLimitAvgFuture.Await()
	resourceQuotaSpecCpuLimitMax, _ := resourceQuotaSpecCpuLimitMaxFuture.Await()
	resourceQuotaSpecRamLimitAvg, _ := resourceQuotaSpecRamLimitAvgFuture.Await()
	resourceQuotaSpecRamLimitMax, _ := resourceQuotaSpecRamLimitMaxFuture.Await()
	resourceQuotaStatusUsedCpuRequestAvg, _ := resourceQuotaStatusUsedCpuRequestAvgFuture.Await()
	resourceQuotaStatusUsedCpuRequestMax, _ := resourceQuotaStatusUsedCpuRequestMaxFuture.Await()
	resourceQuotaStatusUsedRamRequestAvg, _ := resourceQuotaStatusUsedRamRequestAvgFuture.Await()
	resourceQuotaStatusUsedRamRequestMax, _ := resourceQuotaStatusUsedRamRequestMaxFuture.Await()
	resourceQuotaStatusUsedCpuLimitAvg, _ := resourceQuotaStatusUsedCpuLimitAvgFuture.Await()
	resourceQuotaStatusUsedCpuLimitMax, _ := resourceQuotaStatusUsedCpuLimitMaxFuture.Await()
	resourceQuotaStatusUsedRamLimitAvg, _ := resourceQuotaStatusUsedRamLimitAvgFuture.Await()
	resourceQuotaStatusUsedRamLimitMax, _ := resourceQuotaStatusUsedRamLimitMaxFuture.Await()
	nodeInfo, _ := nodeInfoFuture.Await()
	nodeUptime, _ := nodeUptimeFuture.Await()
	nodeResourceCapacities, _ := nodeResourceCapacitiesFuture.Await()
	nodeResourcesAllocatable, _ := nodeResourcesAllocatableFuture.Await()
	podInfo, _ := podInfoFuture.Await()
	podUptime, _ := podUptimeFuture.Await()
	podOwners, _ := podOwnersFuture.Await()
	podPVCVolumes, _ := podPVCVolumesFuture.Await()
	podNetworkEgressBytes, _ := podNetworkEgressBytesFuture.Await()
	podNetworkIngressBytes, _ := podNetworkIngressBytesFuture.Await()
	containerUptime, _ := containerUptimeFuture.Await()
	containerResourceRequests, _ := containerResourceRequestsFuture.Await()
	containerResourceLimits, _ := containerResourceLimitsFuture.Await()
	dcgmDeviceInfo, _ := dcgmDeviceInfoFuture.Await()
	dcgmDeviceUptime, _ := dcgmDeviceUptimeFuture.Await()
	dcgmContainerUsageAvg, _ := dcgmContainerUsageAvgFuture.Await()
	dcgmContainerUsageMax, _ := dcgmContainerUsageMaxFuture.Await()
	pvcUptime, _ := pvcUptimeFuture.Await()
	pvUptime, _ := pvUptimeFuture.Await()
	deploymentInfo, _ := deploymentInfoFuture.Await()
	deploymentUptime, _ := deploymentUptimeFuture.Await()
	deploymentAnnotations, _ := deploymentAnnotationsFuture.Await()
	deploymentMatchLabels, _ := deploymentMatchLabelsFuture.Await()
	statefulSetInfo, _ := statefulSetInfoFuture.Await()
	statefulSetUptime, _ := statefulSetUptimeFuture.Await()
	statefulSetAnnotations, _ := statefulSetAnnotationsFuture.Await()
	statefulSetMatchLabels, _ := statefulSetMatchLabelsFuture.Await()
	daemonSetInfo, _ := daemonSetInfoFuture.Await()
	daemonSetUptime, _ := daemonSetUptimeFuture.Await()
	daemonSetAnnotations, _ := daemonSetAnnotationsFuture.Await()
	daemonSetArguments, _ := daemonSetArgumentsFuture.Await()
	jobInfo, _ := jobInfoFuture.Await()
	jobUptime, _ := jobUptimeFuture.Await()
	jobAnnotations, _ := jobAnnotationsFuture.Await()
	cronJobInfo, _ := cronJobInfoFuture.Await()
	cronJobUptime, _ := cronJobUptimeFuture.Await()
	cronJobLabels, _ := cronJobLabelsFuture.Await()
	cronJobAnnotations, _ := cronJobAnnotationsFuture.Await()
	replicaSetInfo, _ := replicaSetInfoFuture.Await()
	replicaSetUptime, _ := replicaSetUptimeFuture.Await()
	replicaSetLabels, _ := replicaSetLabelsFuture.Await()
	replicaSetAnnotations, _ := replicaSetAnnotationsFuture.Await()
	replicaSetOwners, _ := replicaSetOwnersFuture.Await()
	namespaceInfo, _ := namespaceInfoFuture.Await()
	serviceInfo, _ := serviceInfoFuture.Await()
	serviceUptime, _ := serviceUptimeFuture.Await()
	serviceSelectorLabels, _ := serviceSelectorLabelsFuture.Await()
	podsWithDaemonSetOwner, _ := podsWithDaemonSetOwnerFuture.Await()
	podsWithJobOwner, _ := podsWithJobOwnerFuture.Await()
	resourceQuotaInfo, _ := resourceQuotaInfoFuture.Await()

	if grp.HasErrors() {
		return nil, grp.Error()
	}

	return &MetricsSnapshot{
		Window:                          opencost.NewClosedWindow(start, end),
		PVActiveMinutes:                 pvActiveMinutes,
		PVUsedAverage:                   pvUsedAverage,
		PVUsedMax:                       pvUsedMax,
		LocalStorageActiveMinutes:       localStorageActiveMinutes,
		LocalStorageUsedAvg:             localStorageUsedAvg,
		LocalStorageUsedMax:             localStorageUsedMax,
		LocalStorageBytes:               localStorageBytes,
		KMLocalStorageUsedAvg:           kmLocalStorageUsedAvg,
		KMLocalStorageUsedMax:           kmLocalStorageUsedMax,
		KMLocalStorageBytes:             kmLocalStorageBytes,
		NodeActiveMinutes:               nodeActiveMinutes,
		NodeCPUCoresCapacity:            nodeCPUCoresCapacity,
		NodeCPUCoresAllocatable:         nodeCPUCoresAllocatable,
		NodeRAMBytesCapacity:            nodeRAMBytesCapacity,
		NodeRAMBytesAllocatable:         nodeRAMBytesAllocatable,
		NodeGPUCount:                    nodeGPUCount,
		NodeCPUModeTotal:                nodeCPUModeTotal,
		NodeIsSpot:                      nodeIsSpot,
		NodeRAMSystemPercent:            nodeRAMSystemPercent,
		NodeRAMUserPercent:              nodeRAMUserPercent,
		LBActiveMinutes:                 lbActiveMinutes,
		LBPricePerHr:                    lbPricePerHr,
		ClusterInfo:                     clusterInfo,
		ClusterKubeModelVersion:         clusterKubeModelVersion,
		ClusterUptime:                   clusterUptime,
		ClusterManagementDuration:       clusterManagementDuration,
		ClusterManagementPricePerHr:     clusterManagementPricePerHr,
		Pods:                            pods,
		PodsUID:                         podsUID,
		RAMBytesAllocated:               ramBytesAllocated,
		RAMRequests:                     ramRequests,
		RAMLimits:                       ramLimits,
		RAMUsageAvg:                     ramUsageAvg,
		RAMUsageMax:                     ramUsageMax,
		NodeRAMPricePerGiBHr:            nodeRAMPricePerGiBHr,
		CPUCoresAllocated:               cpuCoresAllocated,
		CPURequests:                     cpuRequests,
		CPULimits:                       cpuLimits,
		CPUUsageAvg:                     cpuUsageAvg,
		CPUUsageMax:                     cpuUsageMax,
		NodeCPUPricePerHr:               nodeCPUPricePerHr,
		GPUsAllocated:                   gpusAllocated,
		GPUsRequested:                   gpusRequested,
		GPUsUsageAvg:                    gpusUsageAvg,
		GPUsUsageMax:                    gpusUsageMax,
		NodeGPUPricePerHr:               nodeGPUPricePerHr,
		GPUInfo:                         gpuInfo,
		IsGPUShared:                     isGPUShared,
		PodPVCAllocation:                podPVCAllocation,
		PVCBytesRequested:               pvcBytesRequested,
		PVCInfo:                         pvcInfo,
		KMPVCInfo:                       kmPVCInfo,
		PVCBytesUsedAvg:                 pvcBytesUsedAvg,
		PVCBytesUsedMax:                 pvcBytesUsedMax,
		PVBytes:                         pvBytes,
		PVPricePerGiBHour:               pvPricePerGiBHour,
		PVInfo:                          pvInfo,
		KMPVInfo:                        kmPVInfo,
		NetZoneGiB:                      netZoneGiB,
		NetZonePricePerGiB:              netZonePricePerGiB,
		NetRegionGiB:                    netRegionGiB,
		NetRegionPricePerGiB:            netRegionPricePerGiB,
		NetInternetGiB:                  netInternetGiB,
		NetInternetPricePerGiB:          netInternetPricePerGiB,
		NetInternetServiceGiB:           netInternetServiceGiB,
		NetNatGatewayPricePerGiB:        netNatGatewayPricePerGiB,
		NetNatGatewayGiB:                netNatGatewayGiB,
		NetTransferBytes:                netTransferBytes,
		NetZoneIngressGiB:               netZoneIngressGiB,
		NetRegionIngressGiB:             netRegionIngressGiB,
		NetInternetIngressGiB:           netInternetIngressGiB,
		NetInternetServiceIngressGiB:    netInternetServiceIngressGiB,
		NetNatGatewayIngressPricePerGiB: netNatGatewayIngressPricePerGiB,
		NetNatGatewayIngressGiB:         netNatGatewayIngressGiB,
		NetReceiveBytes:                 netReceiveBytes,
		NamespaceUptime:                 namespaceUptime,
		NamespaceAnnotations:            namespaceAnnotations,
		PodAnnotations:                  podAnnotations,
		NodeLabels:                      nodeLabels,
		NamespaceLabels:                 namespaceLabels,
		PodLabels:                       podLabels,
		DeploymentLabels:                deploymentLabels,
		StatefulSetLabels:               statefulSetLabels,
		DaemonSetLabels:                 daemonSetLabels,
		JobLabels:                       jobLabels,
		NodeInfo:                        nodeInfo,
		NodeUptime:                      nodeUptime,
		NodeResourceCapacities:          nodeResourceCapacities,
		NodeResourcesAllocatable:        nodeResourcesAllocatable,

		PodInfo:                              podInfo,
		PodUptime:                            podUptime,
		PodOwners:                            podOwners,
		PodPVCVolumes:                        podPVCVolumes,
		PodNetworkEgressBytes:                podNetworkEgressBytes,
		PodNetworkIngressBytes:               podNetworkIngressBytes,
		ContainerUptime:                      containerUptime,
		ContainerResourceRequests:            containerResourceRequests,
		ContainerResourceLimits:              containerResourceLimits,
		DCGMDeviceInfo:                       dcgmDeviceInfo,
		DCGMDeviceUptime:                     dcgmDeviceUptime,
		DCGMContainerUsageAvg:                dcgmContainerUsageAvg,
		DCGMContainerUsageMax:                dcgmContainerUsageMax,
		PVCUptime:                            pvcUptime,
		PVUptime:                             pvUptime,
		DeploymentInfo:                       deploymentInfo,
		DeploymentUptime:                     deploymentUptime,
		DeploymentAnnotations:                deploymentAnnotations,
		DeploymentMatchLabels:                deploymentMatchLabels,
		StatefulSetInfo:                      statefulSetInfo,
		StatefulSetUptime:                    statefulSetUptime,
		StatefulSetAnnotations:               statefulSetAnnotations,
		StatefulSetMatchLabels:               statefulSetMatchLabels,
		DaemonSetInfo:                        daemonSetInfo,
		DaemonSetUptime:                      daemonSetUptime,
		DaemonSetAnnotations:                 daemonSetAnnotations,
		DaemonSetArguments:                   daemonSetArguments,
		JobInfo:                              jobInfo,
		JobUptime:                            jobUptime,
		JobAnnotations:                       jobAnnotations,
		CronJobInfo:                          cronJobInfo,
		CronJobUptime:                        cronJobUptime,
		CronJobLabels:                        cronJobLabels,
		CronJobAnnotations:                   cronJobAnnotations,
		ReplicaSetInfo:                       replicaSetInfo,
		ReplicaSetUptime:                     replicaSetUptime,
		ReplicaSetLabels:                     replicaSetLabels,
		ReplicaSetAnnotations:                replicaSetAnnotations,
		ReplicaSetOwners:                     replicaSetOwners,
		NamespaceInfo:                        namespaceInfo,
		ServiceInfo:                          serviceInfo,
		ServiceUptime:                        serviceUptime,
		ServiceSelectorLabels:                serviceSelectorLabels,
		PodsWithDaemonSetOwner:               podsWithDaemonSetOwner,
		PodsWithJobOwner:                     podsWithJobOwner,
		ResourceQuotaInfo:                    resourceQuotaInfo,
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
