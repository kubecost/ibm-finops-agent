package adapters

import (
	"fmt"
	"sync"
	"time"

	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
)

// MetricsQuerierAdapter is an adapter for the OpenCost metrics querier interface. It allows
// the OpenCost emitter to work with the snapshotted metrics data provided by the exporter.
type MetricsQuerierAdapter struct {
	lock    sync.RWMutex
	summary *emitter.MetricsSummary
}

// NewMetricsQuerierAdapter creates a new
func NewMetricsQuerierAdapter(summary *emitter.MetricsSummary) *MetricsQuerierAdapter {
	return &MetricsQuerierAdapter{
		summary: summary,
	}
}

// Update refreshes the `MetricsSummary` data driving the adapter.
func (mqa *MetricsQuerierAdapter) Update(summary *emitter.MetricsSummary) {
	mqa.lock.Lock()
	defer mqa.lock.Unlock()

	mqa.summary = summary
}

// must hold the read lock when calling this function
func (mqa *MetricsQuerierAdapter) metricsSnapshotFor(start, end time.Time) *emitter.MetricsSnapshot {
	t := end.Sub(start)

	if t == (10 * time.Minute) {
		if mqa.summary.Minutely != nil {
			return mqa.summary.Minutely
		}

		log.Warnf("No metrics snapshot available for 10m duration. Must enable minutely metrics snapshotting!")
		return nil
	}

	if t == time.Hour {
		return mqa.summary.Hourly
	}

	if t == (24 * time.Hour) {
		return mqa.summary.Daily
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

func (mqa *MetricsQuerierAdapter) QueryNetReceiveBytes(start, end time.Time) *source.Future[source.NetReceiveBytesResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeNetReceiveBytesResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.NetReceiveBytes)
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

func (mqa *MetricsQuerierAdapter) QueryDeploymentLabels(start, end time.Time) *source.Future[source.DeploymentLabelsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeDeploymentLabelsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.DeploymentLabels)
}

func (mqa *MetricsQuerierAdapter) QueryStatefulSetLabels(start, end time.Time) *source.Future[source.StatefulSetLabelsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeStatefulSetLabelsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.StatefulSetLabels)
}

func (mqa *MetricsQuerierAdapter) QueryDaemonSetLabels(start, end time.Time) *source.Future[source.DaemonSetLabelsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeDaemonSetLabelsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
	}

	return source.NewFutureFrom(snapshot.DaemonSetLabels)
}

func (mqa *MetricsQuerierAdapter) QueryJobLabels(start, end time.Time) *source.Future[source.JobLabelsResult] {
	mqa.lock.RLock()
	defer mqa.lock.RUnlock()

	snapshot := mqa.metricsSnapshotFor(start, end)
	if snapshot == nil {
		return newErrorResult(source.DecodeJobLabelsResult, fmt.Errorf("invalid start/end duration: %dh", int(end.Sub(start).Hours())))
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
