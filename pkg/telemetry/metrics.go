package telemetry

import (
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/ibm/finops-agent/pkg/health"
	"github.com/prometheus/client_golang/prometheus"
)

const prefix = "finops_agent_"

// Metrics holds the agent's reliability metrics. Counters are added to by the components' event
// sinks (Cloudability, ExporterObserver) or read from their own counters at scrape time (the
// Set*Source and Add*Source hooks). Gauges are read at scrape time. It is safe for concurrent
// use; the hooks are meant to be set once, at startup.
type Metrics struct {
	start time.Time

	dataDropped              *counterFamily // emitter, reason
	snapshotComponentFails   *counterFamily // component
	objectConversionFailures *counterFamily // resource
	windowGaps               *counterFamily // resolution, reason

	quarantineEvicted    prometheus.Counter
	unfinalizedDiscarded prometheus.Counter
	emissionSlotsSkipped prometheus.Counter
	headDeferred         prometheus.Counter
	uploadAttempts       *prometheus.CounterVec   // service, result
	uploadDuration       *prometheus.HistogramVec // service
	recoveryItems        *prometheus.CounterVec   // kind, outcome
	emitTotal            *prometheus.CounterVec   // emitter, result
	snapshotDuration     prometheus.Histogram
	nodeStatsDuration    prometheus.Histogram

	// scrape-time gauges and counters
	status *statusCollector

	mu             sync.Mutex
	exporterStatus func() emitter.ExporterStatus
	uploadStatus   func() UploadStatus
	registry       *health.Registry
	kubecostExport func() KubecostExportCounters
	walCounters    func() WALCounters
}

// UploadStatus is the Cloudability upload loop's progress, for the upload gauges and the status
// summary.
type UploadStatus struct {
	// LastSuccess is when a payload was last delivered; zero if none has been since the start.
	LastSuccess time.Time
	// BacklogFiles and BacklogBytes are the payloads and samples queued at the end of the last
	// upload cycle.
	BacklogFiles int
	BacklogBytes int64
}

// KubecostExportCounters are the Kubecost emitter's export counters (chunk 07). They only ever
// increase.
type KubecostExportCounters struct {
	WritesTotal, WriteFailuresTotal      uint64
	WritesRejectedAfterStopTotal         uint64
	CanaryRunsTotal, CanaryFailuresTotal uint64
	LastCanarySuccess                    time.Time
	ForcedSnapshotSwapsTotal             uint64
}

// WALCounters are the collector write-ahead log's counters (chunk 07). They only ever increase.
type WALCounters struct {
	Configured                             bool
	StoreAttemptsTotal, StoreFailuresTotal uint64
	RestoreErrorsTotal                     uint64
	WritesTotal, WriteFailuresTotal        uint64
}

// WindowGaps is the number of metrics windows at one resolution never queried, for one reason
// (chunk 06). Total only ever increases.
type WindowGaps struct {
	Resolution time.Duration
	Reason     string
	Total      uint64
}

// DurationBuckets are the buckets of the duration histograms, in seconds: 100 ms to 30 min.
var DurationBuckets = []float64{0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800}

// New returns the agent's metrics, not yet registered.
func New() *Metrics {
	m := &Metrics{
		start: time.Now(),
		dataDropped: newCounterFamily(prefix+"data_dropped_total",
			"Collected data lost, by emitter and reason. Each lost item is counted once: summed over reasons it is the total loss. "+
				"Every drop is also logged at Error with event=data_dropped.",
			"emitter", "reason"),
		snapshotComponentFails: newCounterFamily(prefix+"snapshot_component_failures_total",
			"Snapshot components (cluster_info, kubernetes, node_stats, metrics) that failed.", "component"),
		objectConversionFailures: newCounterFamily(prefix+"snapshot_object_conversion_failures_total",
			"Kubernetes objects left out of snapshots because they failed typed conversion, by resource.", "resource"),
		windowGaps: newCounterFamily(prefix+"window_gaps_total",
			"Closed metrics windows never queried, by resolution and reason.", "resolution", "reason"),
		quarantineEvicted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: prefix + "cldy_quarantine_evicted_total",
			Help: "Quarantined Cloudability items removed to keep the quarantine within its bounds. Each was already counted in data_dropped_total when quarantined.",
		}),
		unfinalizedDiscarded: prometheus.NewCounter(prometheus.CounterOpts{
			Name: prefix + "cldy_unfinalized_discarded_total",
			Help: "Unfinished Cloudability samples or payloads removed. Not a drop: their data is in a finalised sample or still in scratch.",
		}),
		emissionSlotsSkipped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: prefix + "emission_slots_skipped_total",
			Help: "Cloudability emission slots skipped after a stall; the next sample covers their usage.",
		}),
		headDeferred: prometheus.NewCounter(prometheus.CounterOpts{
			Name: prefix + "cldy_upload_head_deferred_total",
			Help: "Cloudability payloads moved behind the rest of the upload queue after failing at its head.",
		}),
		uploadAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: prefix + "cldy_upload_attempts_total",
			Help: "Cloudability payload upload attempts, by storage service and result.",
		}, []string{"service", "result"}),
		uploadDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    prefix + "cldy_upload_duration_seconds",
			Help:    "Duration of Cloudability payload upload attempts, by storage service.",
			Buckets: DurationBuckets,
		}, []string{"service"}),
		recoveryItems: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: prefix + "cldy_recovery_items_total",
			Help: "Cloudability samples and payloads found by startup recovery, by kind and outcome.",
		}, []string{"kind", "outcome"}),
		emitTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: prefix + "emit_total",
			Help: "Init and Emit calls the exporter made on each emitter, by result; skipped when the previous call was still running.",
		}, []string{"emitter", "result"}),
		snapshotDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    prefix + "snapshot_duration_seconds",
			Help:    "Duration of cluster snapshots, successful or not.",
			Buckets: DurationBuckets,
		}),
		nodeStatsDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    prefix + "node_stats_duration_seconds",
			Help:    "Duration of node-stats collections across every node.",
			Buckets: DurationBuckets,
		}),
	}
	m.status = &statusCollector{m: m}

	// Drops the exporter and the Kubecost emitter count themselves, read once their hooks are set.
	m.dataDropped.addSource(func(add func(uint64, ...string)) {
		m.mu.Lock()
		exporterStatus, kubecostExport := m.exporterStatus, m.kubecostExport
		m.mu.Unlock()
		if exporterStatus != nil {
			add(exporterStatus().AbandonedShortLivedPodsTotal, EmitterExporter, ReasonSnapshotAbandoned)
		}
		if kubecostExport != nil {
			add(kubecostExport().WritesRejectedAfterStopTotal, EmitterKubecost, ReasonExportRejectedAfterStop)
		}
	})

	for reason, emitters := range dropReasons {
		for _, e := range emitters {
			m.dataDropped.preset(e, reason)
		}
	}
	for _, c := range snapshotComponents {
		m.snapshotComponentFails.preset(c)
	}
	for _, s := range uploadServices {
		for _, r := range uploadResults {
			m.uploadAttempts.WithLabelValues(s, r)
		}
	}
	for _, k := range recoveryKinds {
		for _, o := range recoveryOutcomes {
			m.recoveryItems.WithLabelValues(k, o)
		}
	}
	return m
}

// Register registers every metric with r: prometheus.DefaultRegisterer in production, whose
// /metrics OpenCost's metrics share. It returns every error, such as a name collision.
func (m *Metrics) Register(r prometheus.Registerer) error {
	var errs error
	for _, c := range m.collectors() {
		errs = errors.Join(errs, r.Register(c))
	}
	return errs
}

func (m *Metrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.dataDropped, m.snapshotComponentFails, m.objectConversionFailures, m.windowGaps,
		m.quarantineEvicted, m.unfinalizedDiscarded, m.emissionSlotsSkipped, m.headDeferred,
		m.uploadAttempts, m.uploadDuration, m.recoveryItems, m.emitTotal,
		m.snapshotDuration, m.nodeStatsDuration, m.status,
	}
}

// NodeStatsDuration is the observer for nodes.NodeStatsSummaryClient.SetDurationObserver.
func (m *Metrics) NodeStatsDuration() prometheus.Observer {
	return m.nodeStatsDuration
}

// SetExporter reads the exporter's heartbeat, per-emitter state and dropped short-lived pods at
// scrape time.
func (m *Metrics) SetExporter(status func() emitter.ExporterStatus) {
	m.mu.Lock()
	m.exporterStatus = status
	m.mu.Unlock()
	m.emissionEmitters(status())
}

// emissionEmitters presets emit_total for every emitter the exporter runs.
func (m *Metrics) emissionEmitters(status emitter.ExporterStatus) {
	for _, es := range status.Emitters {
		for _, r := range emitResults {
			m.emitTotal.WithLabelValues(EmitterName(es.ID), r)
		}
	}
}

// AddDropSource counts total, a counter kept elsewhere, as emitter's drops for reason. Use it for
// components that count their own drops: the snapshot provider's (snapshot_failed) and the
// cluster cache's (short_lived_pod_overflow).
func (m *Metrics) AddDropSource(emitterName, reason string, total func() uint64) {
	if !validDrop(emitterName, reason) {
		warnUnknown("emitter/reason", emitterName+"/"+reason)
		emitterName, reason = unknown, unknown
	}
	m.dataDropped.preset(emitterName, reason)
	m.dataDropped.addSource(func(add func(uint64, ...string)) {
		add(total(), emitterName, reason)
	})
}

// SetUploadStatus reads the Cloudability upload loop's progress at scrape time.
func (m *Metrics) SetUploadStatus(status func() UploadStatus) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.uploadStatus = status
}

// SetHealthRegistry reports the registry's conditions as finops_agent_health_condition and in
// the status summary.
func (m *Metrics) SetHealthRegistry(r *health.Registry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.registry = r
}

// SetKubecostExport reads the Kubecost emitter's export counters at scrape time (chunk 07:
// KubecostEmitter.Status).
func (m *Metrics) SetKubecostExport(counters func() KubecostExportCounters) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kubecostExport = counters
}

// SetWAL reads the collector write-ahead log's counters at scrape time (chunk 07: WAL.Status).
func (m *Metrics) SetWAL(counters func() WALCounters) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.walCounters = counters
}

// SetWindowGaps reads the snapshot provider's window gaps at scrape time (chunk 06:
// ConcurrentSnapshotProvider.WindowGapsTotal).
func (m *Metrics) SetWindowGaps(gaps func() []WindowGaps) {
	for _, res := range windowResolutions {
		for _, reason := range windowGapReasons {
			m.windowGaps.preset(res, reason)
		}
	}
	m.windowGaps.addSource(func(add func(uint64, ...string)) {
		for _, g := range gaps() {
			add(g.Total, closed("resolution", resolutionLabel(g.Resolution), windowResolutions), closed("reason", g.Reason, windowGapReasons))
		}
	})
}

// SetConversionFailures reads the objects skipped for failing typed conversion, per resource, at
// scrape time (chunk 06: cluster.ConversionFailures).
func (m *Metrics) SetConversionFailures(failures func() map[string]uint64) {
	m.objectConversionFailures.addSource(func(add func(uint64, ...string)) {
		for resource, n := range failures() {
			add(n, closed("resource", resource, conversionResources))
		}
	})
}

// SnapshotComponentFailed counts a failed snapshot component (chunk 06: ClusterSnapshot.
// ComponentErrors).
func (m *Metrics) SnapshotComponentFailed(component string) {
	m.snapshotComponentFails.add(1, closed("component", component, snapshotComponents))
}

// resolutionLabel formats a metrics resolution as 10m, 1h or 1d.
func resolutionLabel(d time.Duration) string {
	switch {
	case d <= 0:
		return d.String()
	case d%(24*time.Hour) == 0:
		return strconv.Itoa(int(d/(24*time.Hour))) + "d"
	case d%time.Hour == 0:
		return strconv.Itoa(int(d/time.Hour)) + "h"
	case d%time.Minute == 0:
		return strconv.Itoa(int(d/time.Minute)) + "m"
	}
	return d.String()
}

// EmitterName is the emitter label value for an emitter ID.
func EmitterName(id emitter.EmitterID) string {
	switch id {
	case emitter.CldyEmitterID:
		return EmitterCloudability
	case emitter.KubecostEmitterID:
		return EmitterKubecost
	case emitter.TurboEmitterID:
		return EmitterTurbonomic
	}
	warnUnknown("emitter", string(id))
	return unknown
}
