package adapters

import (
	"fmt"
	"sync"
	"time"

	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
)

// MaxBackfillSnapshots is the total number of snapshots to retain historically for the metrics querier
const MaxBackfillSnapshots = 2

// MetricsResolution is a type that holds the current time window's metric snapshot
// and the last time window's metric snapshot.
//
// This type is not thread-safe, so ensure it is accessed under the parent type's lock.
type MetricsResolution struct {
	resolution time.Duration
	snapshots  map[int64]*emitter.MetricsSnapshot
}

// NewMetricsResolution creates a new MetricsResolution instance used to track the resolutions for
// a specific time window
func NewMetricsResolution(resolution time.Duration, initSnapshots []*emitter.MetricsSnapshot) *MetricsResolution {
	snapshots := make(map[int64]*emitter.MetricsSnapshot)

	for _, snapshot := range initSnapshots {
		if snapshot == nil {
			continue
		}

		snapshots[snapshot.Window.Start().Unix()] = snapshot
	}

	return &MetricsResolution{
		resolution: resolution,
		snapshots:  snapshots,
	}
}

// Update will update the current snapshots
func (mr *MetricsResolution) Update(snapshots []*emitter.MetricsSnapshot) {
	// This is a valid state for a disabled resolution
	if len(snapshots) == 0 {
		return
	}

	for _, snapshot := range snapshots {
		mr.updateSnapshot(snapshot)
	}

	// drop oldest after reaching MaxBackfillSnapshots
	for len(mr.snapshots) > MaxBackfillSnapshots {
		oldest := time.Now().Unix()
		for t := range mr.snapshots {
			if t < oldest {
				oldest = t
			}
		}
		delete(mr.snapshots, oldest)
	}
}

func (mr *MetricsResolution) updateSnapshot(snapshot *emitter.MetricsSnapshot) {
	if snapshot == nil {
		return
	}

	snapshotWindow := snapshot.Window
	mr.snapshots[snapshotWindow.Start().Unix()] = snapshot
}

// SnapshotFor returns the snapshot that matches the provided start and end time
func (mr *MetricsResolution) SnapshotFor(start, end time.Time) *emitter.MetricsSnapshot {
	// ensure bounds are valid
	s := start.Truncate(mr.resolution)
	e := s.Add(mr.resolution)
	w := opencost.NewClosedWindow(s, e)

	snapshot, ok := mr.snapshots[s.Unix()]
	if !ok {
		return nil
	}

	// this is mostly an assertion -- we still want to return the snapshot, but definitely issue a
	// warning, as this shouldn't ever happen
	if !snapshot.Window.Equal(w) {
		log.Warnf("Snapshot Window not equal to Query Window: %s != %s", snapshot.Window, w)
	}

	return snapshot
}

// MetricsQuerierAdapter is an adapter for the OpenCost metrics querier interface. It allows
// the OpenCost emitter to work with the snapshotted metrics data provided by the exporter.
type MetricsQuerierAdapter struct {
	lock sync.RWMutex

	tenMinuteResolution *MetricsResolution
	hourlyResolution    *MetricsResolution
	dailyResolution     *MetricsResolution
}

// NewMetricsQuerierAdapter creates a new
func NewMetricsQuerierAdapter(summary *emitter.MetricsSummary) *MetricsQuerierAdapter {
	return &MetricsQuerierAdapter{
		tenMinuteResolution: NewMetricsResolution(10*time.Minute, summary.Minutely),
		hourlyResolution:    NewMetricsResolution(time.Hour, summary.Hourly),
		dailyResolution:     NewMetricsResolution(24*time.Hour, summary.Daily),
	}
}

// Update refreshes the `MetricsSummary` data driving the adapter.
func (mqa *MetricsQuerierAdapter) Update(summary *emitter.MetricsSummary) {
	mqa.lock.Lock()
	defer mqa.lock.Unlock()

	mqa.tenMinuteResolution.Update(summary.Minutely)
	mqa.hourlyResolution.Update(summary.Hourly)
	mqa.dailyResolution.Update(summary.Daily)
}

// must hold the read lock when calling this function
func (mqa *MetricsQuerierAdapter) metricsSnapshotFor(start, end time.Time) *emitter.MetricsSnapshot {
	t := end.Sub(start)

	if t == (10 * time.Minute) {
		snapshot := mqa.tenMinuteResolution.SnapshotFor(start, end)
		if snapshot != nil {
			return snapshot
		}

		log.Warnf("No metrics snapshot available for 10m duration. Must enable minutely metrics snapshotting!")
		return nil
	}

	if t == time.Hour {
		snapshot := mqa.hourlyResolution.SnapshotFor(start, end)
		if snapshot != nil {
			return snapshot
		}

		log.Warnf("No metrics snapshot available for the start and end times: %s, %s", start.Format(time.RFC3339), end.Format(time.RFC3339))
		return nil
	}

	if t == (24 * time.Hour) {
		snapshot := mqa.dailyResolution.SnapshotFor(start, end)
		if snapshot != nil {
			return snapshot
		}

		log.Warnf("No metrics snapshot available for the start and end times: %s, %s", start.Format(time.RFC3339), end.Format(time.RFC3339))
		return nil
	}

	return nil
}

func (mqa *MetricsQuerierAdapter) QueryPVActiveMinutes(start, end time.Time) *source.Future[source.PVActiveMinutesResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodePVActiveMinutesResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.PVActiveMinutes)
}

func (mqa *MetricsQuerierAdapter) QueryPVUsedAverage(start, end time.Time) *source.Future[source.PVUsedAvgResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodePVUsedAvgResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.PVUsedAverage)
}

func (mqa *MetricsQuerierAdapter) QueryPVUsedMax(start, end time.Time) *source.Future[source.PVUsedMaxResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodePVUsedMaxResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.PVUsedMax)
}

func (mqa *MetricsQuerierAdapter) QueryLocalStorageActiveMinutes(start, end time.Time) *source.Future[source.LocalStorageActiveMinutesResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeLocalStorageActiveMinutesResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.LocalStorageActiveMinutes)
}

func (mqa *MetricsQuerierAdapter) QueryLocalStorageCost(start, end time.Time) *source.Future[source.LocalStorageCostResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeLocalStorageCostResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.LocalStorageCost)
}

func (mqa *MetricsQuerierAdapter) QueryLocalStorageUsedCost(start, end time.Time) *source.Future[source.LocalStorageUsedCostResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeLocalStorageUsedCostResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.LocalStorageUsedCost)
}

func (mqa *MetricsQuerierAdapter) QueryLocalStorageUsedAvg(start, end time.Time) *source.Future[source.LocalStorageUsedAvgResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeLocalStorageUsedAvgResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.LocalStorageUsedAvg)
}

func (mqa *MetricsQuerierAdapter) QueryLocalStorageUsedMax(start, end time.Time) *source.Future[source.LocalStorageUsedMaxResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeLocalStorageUsedMaxResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.LocalStorageUsedMax)
}

func (mqa *MetricsQuerierAdapter) QueryLocalStorageBytes(start, end time.Time) *source.Future[source.LocalStorageBytesResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeLocalStorageBytesResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.LocalStorageBytes)
}

func (mqa *MetricsQuerierAdapter) QueryNodeActiveMinutes(start, end time.Time) *source.Future[source.NodeActiveMinutesResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNodeActiveMinutesResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NodeActiveMinutes)
}

func (mqa *MetricsQuerierAdapter) QueryNodeCPUCoresCapacity(start, end time.Time) *source.Future[source.NodeCPUCoresCapacityResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNodeCPUCoresCapacityResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NodeCPUCoresCapacity)
}

func (mqa *MetricsQuerierAdapter) QueryNodeCPUCoresAllocatable(start, end time.Time) *source.Future[source.NodeCPUCoresAllocatableResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNodeCPUCoresAllocatableResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NodeCPUCoresAllocatable)
}

func (mqa *MetricsQuerierAdapter) QueryNodeRAMBytesCapacity(start, end time.Time) *source.Future[source.NodeRAMBytesCapacityResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNodeRAMBytesCapacityResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NodeRAMBytesCapacity)
}

func (mqa *MetricsQuerierAdapter) QueryNodeRAMBytesAllocatable(start, end time.Time) *source.Future[source.NodeRAMBytesAllocatableResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNodeRAMBytesAllocatableResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NodeRAMBytesAllocatable)
}

func (mqa *MetricsQuerierAdapter) QueryNodeGPUCount(start, end time.Time) *source.Future[source.NodeGPUCountResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNodeGPUCountResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NodeGPUCount)
}

func (mqa *MetricsQuerierAdapter) QueryNodeCPUModeTotal(start, end time.Time) *source.Future[source.NodeCPUModeTotalResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNodeCPUModeTotalResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NodeCPUModeTotal)
}

func (mqa *MetricsQuerierAdapter) QueryNodeIsSpot(start, end time.Time) *source.Future[source.NodeIsSpotResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNodeIsSpotResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NodeIsSpot)
}

func (mqa *MetricsQuerierAdapter) QueryNodeRAMSystemPercent(start, end time.Time) *source.Future[source.NodeRAMSystemPercentResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNodeRAMSystemPercentResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NodeRAMSystemPercent)
}

func (mqa *MetricsQuerierAdapter) QueryNodeRAMUserPercent(start, end time.Time) *source.Future[source.NodeRAMUserPercentResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNodeRAMUserPercentResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NodeRAMUserPercent)
}

func (mqa *MetricsQuerierAdapter) QueryLBActiveMinutes(start, end time.Time) *source.Future[source.LBActiveMinutesResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeLBActiveMinutesResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.LBActiveMinutes)
}

func (mqa *MetricsQuerierAdapter) QueryLBPricePerHr(start, end time.Time) *source.Future[source.LBPricePerHrResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeLBPricePerHrResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.LBPricePerHr)
}
func (mqa *MetricsQuerierAdapter) QueryClusterInfo(start, end time.Time) *source.Future[source.ClusterInfoResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeClusterInfoResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ClusterInfo)
}

func (mqa *MetricsQuerierAdapter) QueryClusterUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeUptimeResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ClusterUptime)
}

func (mqa *MetricsQuerierAdapter) QueryClusterManagementDuration(start, end time.Time) *source.Future[source.ClusterManagementDurationResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeClusterManagementDurationResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ClusterManagementDuration)
}

func (mqa *MetricsQuerierAdapter) QueryClusterManagementPricePerHr(start, end time.Time) *source.Future[source.ClusterManagementPricePerHrResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeClusterManagementPricePerHrResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ClusterManagementPricePerHr)
}

func (mqa *MetricsQuerierAdapter) QueryPods(start, end time.Time) *source.Future[source.PodsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodePodsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.Pods)
}

func (mqa *MetricsQuerierAdapter) QueryPodsUID(start, end time.Time) *source.Future[source.PodsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodePodsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.PodsUID)
}

func (mqa *MetricsQuerierAdapter) QueryPodInfo(start, end time.Time) *source.Future[source.PodInfoResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodePodInfoResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.PodInfo)
}

func (mqa *MetricsQuerierAdapter) QueryPodUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeUptimeResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.PodUptime)
}

func (mqa *MetricsQuerierAdapter) QueryPodOwners(start, end time.Time) *source.Future[source.OwnerResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeOwnerResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.PodOwners)
}

func (mqa *MetricsQuerierAdapter) QueryPodPVCVolumes(start, end time.Time) *source.Future[source.PodPVCVolumeResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodePodPVCVolumeResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.PodPVCVolumes)
}

func (mqa *MetricsQuerierAdapter) QueryPodNetworkEgressBytes(start, end time.Time) *source.Future[source.PodNetworkBytesResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodePodNetworkBytesResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.PodNetworkEgressBytes)
}

func (mqa *MetricsQuerierAdapter) QueryPodNetworkIngressBytes(start, end time.Time) *source.Future[source.PodNetworkBytesResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodePodNetworkBytesResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.PodNetworkIngressBytes)
}

func (mqa *MetricsQuerierAdapter) QueryContainerUptime(start, end time.Time) *source.Future[source.ContainerUptimeResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeContainerUptimeResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ContainerUptime)
}

func (mqa *MetricsQuerierAdapter) QueryContainerResourceRequests(start, end time.Time) *source.Future[source.ContainerResourceResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeContainerResourceResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ContainerResourceRequests)
}

func (mqa *MetricsQuerierAdapter) QueryContainerResourceLimits(start, end time.Time) *source.Future[source.ContainerResourceResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeContainerResourceResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ContainerResourceLimits)
}

func (mqa *MetricsQuerierAdapter) QueryRAMBytesAllocated(start, end time.Time) *source.Future[source.RAMBytesAllocatedResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeRAMBytesAllocatedResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.RAMBytesAllocated)
}

func (mqa *MetricsQuerierAdapter) QueryRAMRequests(start, end time.Time) *source.Future[source.RAMRequestsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeRAMRequestsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.RAMRequests)
}

func (mqa *MetricsQuerierAdapter) QueryRAMLimits(start, end time.Time) *source.Future[source.RAMLimitsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeRAMLimitsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.RAMLimits)
}

func (mqa *MetricsQuerierAdapter) QueryRAMUsageAvg(start, end time.Time) *source.Future[source.RAMUsageAvgResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeRAMUsageAvgResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.RAMUsageAvg)
}

func (mqa *MetricsQuerierAdapter) QueryRAMUsageMax(start, end time.Time) *source.Future[source.RAMUsageMaxResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeRAMUsageMaxResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.RAMUsageMax)
}

func (mqa *MetricsQuerierAdapter) QueryNodeRAMPricePerGiBHr(start, end time.Time) *source.Future[source.NodeRAMPricePerGiBHrResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNodeRAMPricePerGiBHrResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NodeRAMPricePerGiBHr)
}

func (mqa *MetricsQuerierAdapter) QueryCPUCoresAllocated(start, end time.Time) *source.Future[source.CPUCoresAllocatedResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeCPUCoresAllocatedResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.CPUCoresAllocated)
}

func (mqa *MetricsQuerierAdapter) QueryCPURequests(start, end time.Time) *source.Future[source.CPURequestsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeCPURequestsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.CPURequests)
}

func (mqa *MetricsQuerierAdapter) QueryCPULimits(start, end time.Time) *source.Future[source.CPULimitsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeCPULimitsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.CPULimits)
}

func (mqa *MetricsQuerierAdapter) QueryCPUUsageAvg(start, end time.Time) *source.Future[source.CPUUsageAvgResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeCPUUsageAvgResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.CPUUsageAvg)
}

func (mqa *MetricsQuerierAdapter) QueryCPUUsageMax(start, end time.Time) *source.Future[source.CPUUsageMaxResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeCPUUsageMaxResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.CPUUsageMax)
}

func (mqa *MetricsQuerierAdapter) QueryNodeCPUPricePerHr(start, end time.Time) *source.Future[source.NodeCPUPricePerHrResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNodeCPUPricePerHrResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NodeCPUPricePerHr)
}

func (mqa *MetricsQuerierAdapter) QueryGPUsAllocated(start, end time.Time) *source.Future[source.GPUsAllocatedResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeGPUsAllocatedResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.GPUsAllocated)
}

func (mqa *MetricsQuerierAdapter) QueryGPUsRequested(start, end time.Time) *source.Future[source.GPUsRequestedResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeGPUsRequestedResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.GPUsRequested)
}

func (mqa *MetricsQuerierAdapter) QueryGPUsUsageAvg(start, end time.Time) *source.Future[source.GPUsUsageAvgResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeGPUsUsageAvgResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.GPUsUsageAvg)
}

func (mqa *MetricsQuerierAdapter) QueryGPUsUsageMax(start, end time.Time) *source.Future[source.GPUsUsageMaxResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeGPUsUsageMaxResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.GPUsUsageMax)
}

func (mqa *MetricsQuerierAdapter) QueryNodeGPUPricePerHr(start, end time.Time) *source.Future[source.NodeGPUPricePerHrResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNodeGPUPricePerHrResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NodeGPUPricePerHr)
}

func (mqa *MetricsQuerierAdapter) QueryGPUInfo(start, end time.Time) *source.Future[source.GPUInfoResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeGPUInfoResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.GPUInfo)
}

func (mqa *MetricsQuerierAdapter) QueryIsGPUShared(start, end time.Time) *source.Future[source.IsGPUSharedResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeIsGPUSharedResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.IsGPUShared)
}

func (mqa *MetricsQuerierAdapter) QueryPodPVCAllocation(start, end time.Time) *source.Future[source.PodPVCAllocationResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodePodPVCAllocationResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.PodPVCAllocation)
}

func (mqa *MetricsQuerierAdapter) QueryPVCBytesRequested(start, end time.Time) *source.Future[source.PVCBytesRequestedResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodePVCBytesRequestedResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.PVCBytesRequested)
}

func (mqa *MetricsQuerierAdapter) QueryPVCInfo(start, end time.Time) *source.Future[source.PVCInfoResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodePVCInfoResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.PVCInfo)
}

func (mqa *MetricsQuerierAdapter) QueryPVBytes(start, end time.Time) *source.Future[source.PVBytesResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodePVBytesResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.PVBytes)
}

func (mqa *MetricsQuerierAdapter) QueryPVPricePerGiBHour(start, end time.Time) *source.Future[source.PVPricePerGiBHourResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodePVPricePerGiBHourResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.PVPricePerGiBHour)
}

func (mqa *MetricsQuerierAdapter) QueryPVInfo(start, end time.Time) *source.Future[source.PVInfoResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodePVInfoResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.PVInfo)
}

func (mqa *MetricsQuerierAdapter) QueryNetZoneGiB(start, end time.Time) *source.Future[source.NetZoneGiBResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNetZoneGiBResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NetZoneGiB)
}

func (mqa *MetricsQuerierAdapter) QueryNetZonePricePerGiB(start, end time.Time) *source.Future[source.NetZonePricePerGiBResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNetZonePricePerGiBResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NetZonePricePerGiB)
}

func (mqa *MetricsQuerierAdapter) QueryNetRegionGiB(start, end time.Time) *source.Future[source.NetRegionGiBResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNetRegionGiBResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NetRegionGiB)
}

func (mqa *MetricsQuerierAdapter) QueryNetRegionPricePerGiB(start, end time.Time) *source.Future[source.NetRegionPricePerGiBResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNetRegionPricePerGiBResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NetRegionPricePerGiB)
}

func (mqa *MetricsQuerierAdapter) QueryNetInternetGiB(start, end time.Time) *source.Future[source.NetInternetGiBResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNetInternetGiBResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NetInternetGiB)
}

func (mqa *MetricsQuerierAdapter) QueryNetInternetPricePerGiB(start, end time.Time) *source.Future[source.NetInternetPricePerGiBResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNetInternetPricePerGiBResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NetInternetPricePerGiB)
}

func (mqa *MetricsQuerierAdapter) QueryNetInternetServiceGiB(start, end time.Time) *source.Future[source.NetInternetServiceGiBResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNetInternetServiceGiBResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NetInternetServiceGiB)
}

func (mqa *MetricsQuerierAdapter) QueryNetNatGatewayGiB(start, end time.Time) *source.Future[source.NetNatGatewayGiBResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNetNatGatewayGiBResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NetNatGatewayGiB)
}

func (mqa *MetricsQuerierAdapter) QueryNetNatGatewayPricePerGiB(start, end time.Time) *source.Future[source.NetNatGatewayPricePerGiBResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNetNatGatewayPricePerGiBResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NetNatGatewayPricePerGiB)
}

func (mqa *MetricsQuerierAdapter) QueryNetTransferBytes(start, end time.Time) *source.Future[source.NetTransferBytesResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNetTransferBytesResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NetTransferBytes)
}

func (mqa *MetricsQuerierAdapter) QueryNetZoneIngressGiB(start, end time.Time) *source.Future[source.NetZoneIngressGiBResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNetZoneIngressGiBResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NetZoneIngressGiB)
}

func (mqa *MetricsQuerierAdapter) QueryNetRegionIngressGiB(start, end time.Time) *source.Future[source.NetRegionIngressGiBResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNetRegionIngressGiBResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NetRegionIngressGiB)
}

func (mqa *MetricsQuerierAdapter) QueryNetInternetIngressGiB(start, end time.Time) *source.Future[source.NetInternetIngressGiBResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNetInternetIngressGiBResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NetInternetIngressGiB)
}

func (mqa *MetricsQuerierAdapter) QueryNetInternetServiceIngressGiB(start, end time.Time) *source.Future[source.NetInternetServiceIngressGiBResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNetInternetServiceIngressGiBResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NetInternetServiceIngressGiB)
}

func (mqa *MetricsQuerierAdapter) QueryNetNatGatewayIngressGiB(start, end time.Time) *source.Future[source.NetNatGatewayIngressGiBResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNetNatGatewayIngressGiBResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NetNatGatewayIngressGiB)
}

func (mqa *MetricsQuerierAdapter) QueryNetNatGatewayIngressPricePerGiB(start, end time.Time) *source.Future[source.NetNatGatewayPricePerGiBResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNetNatGatewayPricePerGiBResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NetNatGatewayIngressPricePerGiB)
}

func (mqa *MetricsQuerierAdapter) QueryNetReceiveBytes(start, end time.Time) *source.Future[source.NetReceiveBytesResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNetReceiveBytesResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NetReceiveBytes)
}

func (mqa *MetricsQuerierAdapter) QueryNamespaceUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeUptimeResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NamespaceUptime)
}

func (mqa *MetricsQuerierAdapter) QueryNamespaceAnnotations(start, end time.Time) *source.Future[source.NamespaceAnnotationsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNamespaceAnnotationsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NamespaceAnnotations)
}

func (mqa *MetricsQuerierAdapter) QueryPodAnnotations(start, end time.Time) *source.Future[source.PodAnnotationsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodePodAnnotationsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.PodAnnotations)
}

func (mqa *MetricsQuerierAdapter) QueryNodeLabels(start, end time.Time) *source.Future[source.NodeLabelsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNodeLabelsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NodeLabels)
}

func (mqa *MetricsQuerierAdapter) QueryNamespaceLabels(start, end time.Time) *source.Future[source.NamespaceLabelsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNamespaceLabelsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NamespaceLabels)
}

func (mqa *MetricsQuerierAdapter) QueryPodLabels(start, end time.Time) *source.Future[source.PodLabelsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodePodLabelsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.PodLabels)
}

func (mqa *MetricsQuerierAdapter) QueryServiceLabels(start, end time.Time) *source.Future[source.ServiceLabelsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeServiceLabelsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ServiceLabels)
}

func (mqa *MetricsQuerierAdapter) QueryDeploymentLabels(start, end time.Time) *source.Future[source.LabelsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeLabelsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.DeploymentLabels)
}

func (mqa *MetricsQuerierAdapter) QueryStatefulSetLabels(start, end time.Time) *source.Future[source.LabelsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeLabelsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.StatefulSetLabels)
}

func (mqa *MetricsQuerierAdapter) QueryDaemonSetLabels(start, end time.Time) *source.Future[source.LabelsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeLabelsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.DaemonSetLabels)
}

func (mqa *MetricsQuerierAdapter) QueryJobLabels(start, end time.Time) *source.Future[source.LabelsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeLabelsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.JobLabels)
}

func (mqa *MetricsQuerierAdapter) QueryPodsWithReplicaSetOwner(start, end time.Time) *source.Future[source.PodsWithReplicaSetOwnerResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodePodsWithReplicaSetOwnerResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.PodsWithReplicaSetOwner)
}

func (mqa *MetricsQuerierAdapter) QueryReplicaSetsWithoutOwners(start, end time.Time) *source.Future[source.ReplicaSetsWithoutOwnersResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeReplicaSetsWithoutOwnersResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ReplicaSetsWithoutOwners)
}

func (mqa *MetricsQuerierAdapter) QueryReplicaSetsWithRollout(start, end time.Time) *source.Future[source.ReplicaSetsWithRolloutResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeReplicaSetsWithRolloutResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ReplicaSetsWithRollout)
}

func (mqa *MetricsQuerierAdapter) QueryResourceQuotaUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeUptimeResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ResourceQuotaUptime)
}

func (mqa *MetricsQuerierAdapter) QueryResourceQuotaSpecCPURequestAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeResourceResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ResourceQuotaSpecCPURequestAvg)
}

func (mqa *MetricsQuerierAdapter) QueryResourceQuotaSpecCPURequestMax(start, end time.Time) *source.Future[source.ResourceResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeResourceResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ResourceQuotaSpecCPURequestMax)
}

func (mqa *MetricsQuerierAdapter) QueryResourceQuotaSpecRAMRequestAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeResourceResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ResourceQuotaSpecRAMRequestAvg)
}

func (mqa *MetricsQuerierAdapter) QueryResourceQuotaSpecRAMRequestMax(start, end time.Time) *source.Future[source.ResourceResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeResourceResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ResourceQuotaSpecRAMRequestMax)
}

func (mqa *MetricsQuerierAdapter) QueryResourceQuotaSpecCPULimitAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeResourceResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ResourceQuotaSpecCPULimitAvg)
}

func (mqa *MetricsQuerierAdapter) QueryResourceQuotaSpecCPULimitMax(start, end time.Time) *source.Future[source.ResourceResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeResourceResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ResourceQuotaSpecCPULimitMax)
}

func (mqa *MetricsQuerierAdapter) QueryResourceQuotaSpecRAMLimitAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeResourceResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ResourceQuotaSpecRAMLimitAvg)
}

func (mqa *MetricsQuerierAdapter) QueryResourceQuotaSpecRAMLimitMax(start, end time.Time) *source.Future[source.ResourceResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeResourceResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ResourceQuotaSpecRAMLimitMax)
}

func (mqa *MetricsQuerierAdapter) QueryResourceQuotaStatusUsedCPURequestAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeResourceResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ResourceQuotaStatusUsedCPURequestAvg)
}

func (mqa *MetricsQuerierAdapter) QueryResourceQuotaStatusUsedCPURequestMax(start, end time.Time) *source.Future[source.ResourceResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeResourceResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ResourceQuotaStatusUsedCPURequestMax)
}

func (mqa *MetricsQuerierAdapter) QueryResourceQuotaStatusUsedRAMRequestAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeResourceResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ResourceQuotaStatusUsedRAMRequestAvg)
}

func (mqa *MetricsQuerierAdapter) QueryResourceQuotaStatusUsedRAMRequestMax(start, end time.Time) *source.Future[source.ResourceResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeResourceResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ResourceQuotaStatusUsedRAMRequestMax)
}

func (mqa *MetricsQuerierAdapter) QueryResourceQuotaStatusUsedCPULimitAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeResourceResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ResourceQuotaStatusUsedCPULimitAvg)
}

func (mqa *MetricsQuerierAdapter) QueryResourceQuotaStatusUsedCPULimitMax(start, end time.Time) *source.Future[source.ResourceResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeResourceResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ResourceQuotaStatusUsedCPULimitMax)
}

func (mqa *MetricsQuerierAdapter) QueryResourceQuotaStatusUsedRAMLimitAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeResourceResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ResourceQuotaStatusUsedRAMLimitAvg)
}

func (mqa *MetricsQuerierAdapter) QueryResourceQuotaStatusUsedRAMLimitMax(start, end time.Time) *source.Future[source.ResourceResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeResourceResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ResourceQuotaStatusUsedRAMLimitMax)
}

func (mqa *MetricsQuerierAdapter) QueryDeploymentInfo(start, end time.Time) *source.Future[source.DeploymentInfoResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeDeploymentInfoResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.DeploymentInfo)
}

func (mqa *MetricsQuerierAdapter) QueryDeploymentUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeUptimeResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.DeploymentUptime)
}

func (mqa *MetricsQuerierAdapter) QueryDeploymentAnnotations(start, end time.Time) *source.Future[source.AnnotationsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeAnnotationsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.DeploymentAnnotations)
}

func (mqa *MetricsQuerierAdapter) QueryDeploymentMatchLabels(start, end time.Time) *source.Future[source.DeploymentLabelsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeDeploymentLabelsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.DeploymentMatchLabels)
}

func (mqa *MetricsQuerierAdapter) QueryStatefulSetInfo(start, end time.Time) *source.Future[source.StatefulSetInfoResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeStatefulSetInfoResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.StatefulSetInfo)
}

func (mqa *MetricsQuerierAdapter) QueryStatefulSetUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeUptimeResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.StatefulSetUptime)
}

func (mqa *MetricsQuerierAdapter) QueryStatefulSetAnnotations(start, end time.Time) *source.Future[source.AnnotationsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeAnnotationsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.StatefulSetAnnotations)
}

func (mqa *MetricsQuerierAdapter) QueryStatefulSetMatchLabels(start, end time.Time) *source.Future[source.StatefulSetLabelsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeStatefulSetLabelsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.StatefulSetMatchLabels)
}

func (mqa *MetricsQuerierAdapter) QueryDaemonSetInfo(start, end time.Time) *source.Future[source.DaemonSetInfoResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeDaemonSetInfoResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.DaemonSetInfo)
}

func (mqa *MetricsQuerierAdapter) QueryDaemonSetUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeUptimeResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.DaemonSetUptime)
}

func (mqa *MetricsQuerierAdapter) QueryDaemonSetAnnotations(start, end time.Time) *source.Future[source.AnnotationsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeAnnotationsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.DaemonSetAnnotations)
}

func (mqa *MetricsQuerierAdapter) QueryJobInfo(start, end time.Time) *source.Future[source.JobInfoResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeJobInfoResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.JobInfo)
}

func (mqa *MetricsQuerierAdapter) QueryJobUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeUptimeResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.JobUptime)
}

func (mqa *MetricsQuerierAdapter) QueryJobAnnotations(start, end time.Time) *source.Future[source.AnnotationsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeAnnotationsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.JobAnnotations)
}

func (mqa *MetricsQuerierAdapter) QueryCronJobInfo(start, end time.Time) *source.Future[source.CronJobInfoResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeCronJobInfoResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.CronJobInfo)
}

func (mqa *MetricsQuerierAdapter) QueryCronJobUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeUptimeResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.CronJobUptime)
}

func (mqa *MetricsQuerierAdapter) QueryCronJobLabels(start, end time.Time) *source.Future[source.LabelsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeLabelsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.CronJobLabels)
}

func (mqa *MetricsQuerierAdapter) QueryCronJobAnnotations(start, end time.Time) *source.Future[source.AnnotationsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeAnnotationsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.CronJobAnnotations)
}

func (mqa *MetricsQuerierAdapter) QueryReplicaSetInfo(start, end time.Time) *source.Future[source.ReplicaSetInfoResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeReplicaSetInfoResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ReplicaSetInfo)
}

func (mqa *MetricsQuerierAdapter) QueryReplicaSetUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeUptimeResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ReplicaSetUptime)
}

func (mqa *MetricsQuerierAdapter) QueryReplicaSetLabels(start, end time.Time) *source.Future[source.LabelsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeLabelsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ReplicaSetLabels)
}

func (mqa *MetricsQuerierAdapter) QueryReplicaSetAnnotations(start, end time.Time) *source.Future[source.AnnotationsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeAnnotationsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ReplicaSetAnnotations)
}

func (mqa *MetricsQuerierAdapter) QueryReplicaSetOwners(start, end time.Time) *source.Future[source.OwnerResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeOwnerResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ReplicaSetOwners)
}

func (mqa *MetricsQuerierAdapter) QueryNamespaceInfo(start, end time.Time) *source.Future[source.NamespaceInfoResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNamespaceInfoResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NamespaceInfo)
}

func (mqa *MetricsQuerierAdapter) QueryServiceInfo(start, end time.Time) *source.Future[source.ServiceInfoResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeServiceInfoResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ServiceInfo)
}

func (mqa *MetricsQuerierAdapter) QueryServiceUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeUptimeResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ServiceUptime)
}

func (mqa *MetricsQuerierAdapter) QueryNodeInfo(start, end time.Time) *source.Future[source.NodeInfoResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNodeInfoResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NodeInfo)
}

func (mqa *MetricsQuerierAdapter) QueryNodeUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeUptimeResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NodeUptime)
}

func (mqa *MetricsQuerierAdapter) QueryNodeResourceCapacities(start, end time.Time) *source.Future[source.ResourceResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeResourceResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NodeResourceCapacities)
}

func (mqa *MetricsQuerierAdapter) QueryNodeResourcesAllocatable(start, end time.Time) *source.Future[source.ResourceResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeResourceResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NodeResourcesAllocatable)
}

func (mqa *MetricsQuerierAdapter) QueryPVCUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeUptimeResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.PVCUptime)
}

func (mqa *MetricsQuerierAdapter) QueryPVUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeUptimeResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.PVUptime)
}

func (mqa *MetricsQuerierAdapter) QueryPodsWithDaemonSetOwner(start, end time.Time) *source.Future[source.PodsWithDaemonSetOwnerResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodePodsWithDaemonSetOwnerResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.PodsWithDaemonSetOwner)
}

func (mqa *MetricsQuerierAdapter) QueryPodsWithJobOwner(start, end time.Time) *source.Future[source.PodsWithJobOwnerResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodePodsWithJobOwnerResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.PodsWithJobOwner)
}

func (mqa *MetricsQuerierAdapter) QueryResourceQuotaInfo(start, end time.Time) *source.Future[source.ResourceQuotaInfoResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeResourceQuotaInfoResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ResourceQuotaInfo)
}

func (mqa *MetricsQuerierAdapter) QueryServiceSelectorLabels(start, end time.Time) *source.Future[source.ServiceLabelsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeServiceLabelsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.ServiceSelectorLabels)
}

func (mqa *MetricsQuerierAdapter) QueryDCGMDeviceInfo(start, end time.Time) *source.Future[source.DCGMDeviceInfoResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeDCGMDeviceInfoResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.DCGMDeviceInfo)
}

func (mqa *MetricsQuerierAdapter) QueryDCGMDeviceUptime(start, end time.Time) *source.Future[source.DCGMDeviceUptimeResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeDCGMDeviceUptimeResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.DCGMDeviceUptime)
}

func (mqa *MetricsQuerierAdapter) QueryDCGMContainerUsageAvg(start, end time.Time) *source.Future[source.DCGMDeviceContainerUsageResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeDCGMDeviceContainerUsageResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.DCGMContainerUsageAvg)
}

func (mqa *MetricsQuerierAdapter) QueryDCGMContainerUsageMax(start, end time.Time) *source.Future[source.DCGMDeviceContainerUsageResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeDCGMDeviceContainerUsageResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.DCGMContainerUsageMax)
}

// QueryDataCoverage is used for a non-compliant exporter -- we don't need to worry about implementation for now.
func (mqa *MetricsQuerierAdapter) QueryDataCoverage(limitDays int) (time.Time, time.Time, error) {
	return time.Time{}, time.Time{}, fmt.Errorf("not implemented")
}

func newErrorResult[T any](decoder source.ResultDecoder[T], err error) *source.Future[T] {
	ch := make(source.QueryResultsChan)
	go func() {
		results := source.NewQueryResults("")
		results.Error = err
		ch <- results
	}()

	return source.NewFuture(decoder, ch)
}
