package telemetry

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/ibm/finops-agent/pkg/health"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	descCycleLastEnd = prometheus.NewDesc(prefix+"exporter_cycle_last_end_timestamp_seconds",
		"When the exporter's last snapshot-and-emit cycle ended, or when the agent started if none has.", nil, nil)
	descCycleOverruns = prometheus.NewDesc(prefix+"exporter_cycle_overruns_total",
		"Exporter ticks skipped because a cycle took longer than the interval.", nil, nil)
	descSnapshotTimeouts = prometheus.NewDesc(prefix+"exporter_snapshot_timeouts_total",
		"Exporter cycles in which no snapshot completed within the snapshot deadline.", nil, nil)
	descEmitTimeouts = prometheus.NewDesc(prefix+"exporter_emit_timeouts_total",
		"Init or Emit calls that outlived the emit deadline.", nil, nil)
	descEmitterReady = prometheus.NewDesc(prefix+"emitter_ready",
		"1 when the emitter has initialised and its recent cycles haven't all failed, else 0.", []string{"emitter"}, nil)

	descUploadLastSuccess = prometheus.NewDesc(prefix+"cldy_upload_last_success_timestamp_seconds",
		"When a Cloudability payload was last delivered, or when the agent started if none has been.", nil, nil)
	descBacklogFiles = prometheus.NewDesc(prefix+"cldy_upload_backlog_files",
		"Cloudability payloads and finalised samples queued for upload at the end of the last upload cycle.", nil, nil)
	descBacklogBytes = prometheus.NewDesc(prefix+"cldy_upload_backlog_bytes",
		"Bytes of Cloudability payloads and finalised samples queued for upload at the end of the last upload cycle.", nil, nil)

	descHealthCondition = prometheus.NewDesc(prefix+"health_condition",
		"Active degraded conditions of each health component, by type: the number active, 0 once cleared.", []string{"component", "type"}, nil)

	descKubecostWrites = prometheus.NewDesc(prefix+"kubecost_export_writes_total",
		"Kubecost export writes to the bucket, failed or not.", nil, nil)
	descKubecostWriteFailures = prometheus.NewDesc(prefix+"kubecost_export_write_failures_total",
		"Kubecost export writes to the bucket that failed.", nil, nil)
	descKubecostRejectedAfterStop = prometheus.NewDesc(prefix+"kubecost_export_rejected_after_stop_total",
		"Kubecost export writes refused because the emitter had stopped.", nil, nil)
	descKubecostCanary = prometheus.NewDesc(prefix+"kubecost_bucket_canary_runs_total",
		"Kubecost bucket canary probes, failed or not.", nil, nil)
	descKubecostCanaryFailures = prometheus.NewDesc(prefix+"kubecost_bucket_canary_failures_total",
		"Kubecost bucket canary probes that failed.", nil, nil)
	descKubecostCanaryLastSuccess = prometheus.NewDesc(prefix+"kubecost_bucket_canary_last_success_timestamp_seconds",
		"When the Kubecost bucket canary last succeeded, or when the agent started if it hasn't.", nil, nil)
	descKubecostForcedSwaps = prometheus.NewDesc(prefix+"kubecost_forced_snapshot_swaps_total",
		"Snapshots published to the Kubecost adapters under a computation pinned for too long.", nil, nil)
	descWALWrites = prometheus.NewDesc(prefix+"collector_wal_writes_total",
		"Collector write-ahead log writes, failed or not.", nil, nil)
	descWALWriteFailures = prometheus.NewDesc(prefix+"collector_wal_write_failures_total",
		"Collector write-ahead log writes that failed.", nil, nil)
	descWALStoreAttempts = prometheus.NewDesc(prefix+"collector_wal_store_attempts_total",
		"Attempts to build the collector write-ahead log's bucket store, failed or not.", nil, nil)
	descWALStoreFailures = prometheus.NewDesc(prefix+"collector_wal_store_failures_total",
		"Attempts to build the collector write-ahead log's bucket store that failed.", nil, nil)
	descWALRestoreErrors = prometheus.NewDesc(prefix+"collector_wal_restore_errors_total",
		"Bucket List and Read failures while the collector write-ahead log was replayed.", nil, nil)
)

// maxConditionSeries bounds the health_condition series remembered to export at 0 once cleared
// (I7). Condition types are a closed set per component, so it is never reached in practice.
const maxConditionSeries = 256

// statusCollector exports the gauges and counters read at scrape time.
type statusCollector struct {
	m *Metrics

	mu   sync.Mutex
	seen map[[2]string]struct{} // health_condition series ever exported
}

func (c *statusCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		descCycleLastEnd, descCycleOverruns, descSnapshotTimeouts, descEmitTimeouts, descEmitterReady,
		descUploadLastSuccess, descBacklogFiles, descBacklogBytes, descHealthCondition,
		descKubecostWrites, descKubecostWriteFailures, descKubecostRejectedAfterStop, descKubecostCanary,
		descKubecostCanaryFailures, descKubecostCanaryLastSuccess, descKubecostForcedSwaps,
		descWALWrites, descWALWriteFailures, descWALStoreAttempts, descWALStoreFailures, descWALRestoreErrors,
	} {
		ch <- d
	}
}

func (c *statusCollector) Collect(ch chan<- prometheus.Metric) {
	m := c.m
	m.mu.Lock()
	exporterStatus, uploadStatus, registry := m.exporterStatus, m.uploadStatus, m.registry
	kubecostExport, walCounters := m.kubecostExport, m.walCounters
	m.mu.Unlock()

	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}
	counter := func(d *prometheus.Desc, v uint64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, float64(v), labels...)
	}

	if exporterStatus != nil {
		s := exporterStatus()
		gauge(descCycleLastEnd, m.timestamp(s.LastCycleEnd))
		counter(descCycleOverruns, s.CycleOverrunsTotal)
		counter(descSnapshotTimeouts, s.SnapshotTimeoutsTotal)
		counter(descEmitTimeouts, s.EmitTimeoutsTotal)
		for _, es := range s.Emitters {
			ready := 0.0
			if EmitterReady(es) {
				ready = 1
			}
			gauge(descEmitterReady, ready, EmitterName(es.ID))
		}
	}
	if uploadStatus != nil {
		s := uploadStatus()
		gauge(descUploadLastSuccess, m.timestamp(s.LastSuccess))
		gauge(descBacklogFiles, float64(s.BacklogFiles))
		gauge(descBacklogBytes, float64(s.BacklogBytes))
	}
	if registry != nil {
		c.collectConditions(registry.Check(context.Background()), gauge)
	}
	if kubecostExport != nil {
		s := kubecostExport()
		// Totals and failures are separate counters: a difference of two counters read one after
		// the other can go backwards.
		counter(descKubecostWrites, s.WritesTotal)
		counter(descKubecostWriteFailures, s.WriteFailuresTotal)
		counter(descKubecostRejectedAfterStop, s.WritesRejectedAfterStopTotal)
		counter(descKubecostCanary, s.CanaryRunsTotal)
		counter(descKubecostCanaryFailures, s.CanaryFailuresTotal)
		gauge(descKubecostCanaryLastSuccess, m.timestamp(s.LastCanarySuccess))
		counter(descKubecostForcedSwaps, s.ForcedSnapshotSwapsTotal)
	}
	if walCounters != nil {
		s := walCounters()
		if s.Configured {
			counter(descWALWrites, s.WritesTotal)
			counter(descWALWriteFailures, s.WriteFailuresTotal)
			counter(descWALStoreAttempts, s.StoreAttemptsTotal)
			counter(descWALStoreFailures, s.StoreFailuresTotal)
			counter(descWALRestoreErrors, s.RestoreErrorsTotal)
		}
	}
}

// collectConditions exports one health_condition series per component and condition type: the
// number of active conditions of that type, and 0 for a type seen active earlier in the process.
func (c *statusCollector) collectConditions(result health.Result, gauge func(*prometheus.Desc, float64, ...string)) {
	active := map[[2]string]int{}
	for _, comp := range result.Components {
		for _, cond := range comp.Conditions {
			active[[2]string{comp.Name, cond.Type}]++
		}
	}
	c.mu.Lock()
	if c.seen == nil {
		c.seen = map[[2]string]struct{}{}
	}
	for k := range active {
		if len(c.seen) < maxConditionSeries {
			c.seen[k] = struct{}{}
		}
	}
	keys := make([][2]string, 0, len(c.seen))
	for k := range c.seen {
		keys = append(keys, k)
	}
	c.mu.Unlock()
	slices.SortFunc(keys, func(a, b [2]string) int {
		if a[0] != b[0] {
			return strings.Compare(a[0], b[0])
		}
		return strings.Compare(a[1], b[1])
	})
	for _, k := range keys {
		gauge(descHealthCondition, float64(active[k]), k[0], k[1])
	}
}

// timestamp is t in Unix seconds, or the agent's start if t is zero.
func (m *Metrics) timestamp(t time.Time) float64 {
	if t.IsZero() {
		t = m.start
	}
	return float64(t.UnixNano()) / 1e9
}

// EmitterReady reports whether an emitter has initialised and its recent cycles haven't all
// failed: it has neither of the exporter's emitter conditions.
func EmitterReady(es emitter.EmitterStatus) bool {
	return es.State == emitter.EmitterReady && es.ConsecutiveFailures < emitter.NotReadyAfterFailures
}
