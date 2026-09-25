package telemetry_test

// The agent's reliability metrics (docs/reliability/FINDINGS.md chunk 09).

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/ibm/finops-agent/pkg/condition"
	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/ibm/finops-agent/pkg/health"
	"github.com/ibm/finops-agent/pkg/telemetry"
	heartbeatexporter "github.com/opencost/opencost/core/pkg/heartbeat/exporter"
	"github.com/opencost/opencost/pkg/costmodel"
	ocmetrics "github.com/opencost/opencost/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func newRegistered(t *testing.T) (*telemetry.Metrics, *prometheus.Registry) {
	t.Helper()
	m := telemetry.New()
	reg := prometheus.NewPedanticRegistry()
	if err := m.Register(reg); err != nil {
		t.Fatalf("Register: %v", err)
	}
	return m, reg
}

// series returns every sample of the finops_agent_ metrics in reg, as name{labels} -> value.
func series(t *testing.T, reg prometheus.Gatherer) map[string]float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	out := map[string]float64{}
	for _, f := range families {
		if !strings.HasPrefix(f.GetName(), "finops_agent_") {
			continue
		}
		for _, m := range f.GetMetric() {
			out[f.GetName()+labelString(m)] = value(m)
		}
	}
	return out
}

func labelString(m *dto.Metric) string {
	var parts []string
	for _, l := range m.GetLabel() {
		parts = append(parts, fmt.Sprintf("%s=%q", l.GetName(), l.GetValue()))
	}
	if len(parts) == 0 {
		return ""
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func value(m *dto.Metric) float64 {
	switch {
	case m.GetCounter() != nil:
		return m.GetCounter().GetValue()
	case m.GetGauge() != nil:
		return m.GetGauge().GetValue()
	case m.GetHistogram() != nil:
		return float64(m.GetHistogram().GetSampleCount())
	}
	return 0
}

func dropped(t *testing.T, reg prometheus.Gatherer, emitterName, reason string) float64 {
	t.Helper()
	return series(t, reg)[fmt.Sprintf("finops_agent_data_dropped_total{emitter=%q,reason=%q}", emitterName, reason)]
}

func droppedSum(t *testing.T, reg prometheus.Gatherer) float64 {
	t.Helper()
	var sum float64
	for k, v := range series(t, reg) {
		if strings.HasPrefix(k, "finops_agent_data_dropped_total{") {
			sum += v
		}
	}
	return sum
}

// I1: every drop reason, from every emitter that raises it, increments
// finops_agent_data_dropped_total{emitter,reason} by exactly the number of items lost, and
// nothing else: summed over every series, it is the total loss.
func TestEveryDropReasonCountsExactly(t *testing.T) {
	reasons := telemetry.DropReasons()
	if len(reasons) < 16 {
		t.Fatalf("only %d drop reasons: %v", len(reasons), reasons)
	}
	for _, reason := range reasons {
		for _, emitterName := range telemetry.DropReasonEmitters(reason) {
			t.Run(emitterName+"/"+reason, func(t *testing.T) {
				m, reg := newRegistered(t)
				if got := dropped(t, reg, emitterName, reason); got != 0 {
					t.Fatalf("before any drop: %v", got)
				}
				var total uint64
				record := func(n uint64) {
					total += n
					switch emitterName {
					case telemetry.EmitterCloudability:
						m.Cloudability().DataDropped(reason, int(n))
					case telemetry.EmitterKubecost:
						m.SetKubecostExport(func() telemetry.KubecostExportCounters {
							return telemetry.KubecostExportCounters{WritesRejectedAfterStopTotal: total}
						})
					case telemetry.EmitterExporter:
						if reason == telemetry.ReasonSnapshotAbandoned {
							m.SetExporter(func() emitter.ExporterStatus {
								return emitter.ExporterStatus{AbandonedShortLivedPodsTotal: total}
							})
						} else if n == 3 {
							// a source is added once and read at every scrape
							m.AddDropSource(emitterName, reason, func() uint64 { return total })
						}
					default:
						t.Fatalf("no way to raise %s from %s", reason, emitterName)
					}
				}
				record(3)
				if got := dropped(t, reg, emitterName, reason); got != 3 {
					t.Fatalf("after 3 dropped: %v", got)
				}
				record(4)
				if got := dropped(t, reg, emitterName, reason); got != 7 {
					t.Fatalf("after 3+4 dropped: %v", got)
				}
				if got := droppedSum(t, reg); got != 7 {
					t.Fatalf("data_dropped_total summed over every series is %v; want 7", got)
				}
				if s := m.Summary(); s.DataDroppedTotal != 7 || s.DataDropped[emitterName][reason] != 7 {
					t.Fatalf("summary: total %d, by reason %v", s.DataDroppedTotal, s.DataDropped)
				}
			})
		}
	}
}

// quarantine_evicted re-counts items already counted as dropped when they were quarantined, so it
// has its own counter and summing data_dropped_total counts each lost item once.
func TestQuarantineEvictionIsNotCountedTwice(t *testing.T) {
	m, reg := newRegistered(t)
	sink := m.Cloudability()
	sink.DataDropped(telemetry.ReasonInvalidSample, 2) // quarantined: lost
	sink.DataDropped(telemetry.ReasonQuarantineEvicted, 2)
	if got := droppedSum(t, reg); got != 2 {
		t.Errorf("data_dropped_total sums to %v; want 2, the items quarantined", got)
	}
	if got := series(t, reg)["finops_agent_cldy_quarantine_evicted_total"]; got != 2 {
		t.Errorf("cldy_quarantine_evicted_total = %v; want 2", got)
	}
	if s := m.Summary(); s.DataDroppedTotal != 2 || s.QuarantineEvictedTotal != 2 {
		t.Errorf("summary: dropped %d, quarantine evicted %d", s.DataDroppedTotal, s.QuarantineEvictedTotal)
	}
}

// A reason, service or result outside its closed set is recorded as unknown, never as a label
// value of its own.
func TestLabelsStayInTheirClosedSets(t *testing.T) {
	m, reg := newRegistered(t)
	sink := m.Cloudability()
	sink.DataDropped("/opt/finops-agent/upload/abc_2026.tgz", 1)
	sink.DataDropped(telemetry.ReasonSnapshotFailed, 1) // an exporter reason, not Cloudability's
	sink.UploadAttempt("https://example.com/bucket?sig=secret", telemetry.UploadResultOK)
	sink.RecoveryItem("node-1", "exploded", 1)
	m.Exporter().EmitterCall("some-new-emitter", "weird")
	m.SnapshotComponentFailed("network")

	s := series(t, reg)
	if got := s[`finops_agent_data_dropped_total{emitter="cloudability",reason="unknown"}`]; got != 2 {
		t.Errorf("unknown reasons counted %v; want 2", got)
	}
	for k := range s {
		for _, bad := range []string{"/opt", "https", "secret", "node-1", "exploded", "some-new", "weird", "network", "snapshot_failed\"}"} {
			if strings.Contains(k, bad) && !strings.Contains(k, `emitter="exporter"`) {
				t.Errorf("series %s carries an unbounded label value (%s)", k, bad)
			}
		}
	}
}

// Registration succeeds next to OpenCost's metrics on the default registry, which /metrics
// serves, and every agent metric is named finops_agent_*. A second registration is an error,
// not a panic.
func TestRegistersAlongsideOpenCostMetrics(t *testing.T) {
	ocmetrics.InitOpencostTelemetry(&ocmetrics.MetricsConfig{})
	costmodel.NewCostModelMetricsEmitter(nil, nil, clusterInfo{}, nil) // registers the cost-model metrics

	m := telemetry.New()
	if err := m.Register(prometheus.DefaultRegisterer); err != nil {
		t.Fatalf("registering alongside OpenCost's metrics: %v", err)
	}
	t.Cleanup(func() {
		for _, c := range telemetry.Collectors(m) {
			prometheus.DefaultRegisterer.Unregister(c)
		}
	})
	if err := telemetry.New().Register(prometheus.DefaultRegisterer); err == nil {
		t.Errorf("registering twice succeeded; want an AlreadyRegistered error")
	}

	descs := make(chan *prometheus.Desc, 256)
	var names []string
	for _, c := range telemetry.Collectors(m) {
		c.Describe(descs)
	}
	close(descs)
	for d := range descs {
		names = append(names, d.String())
	}
	for _, n := range names {
		if !strings.Contains(n, `fqName: "finops_agent_`) {
			t.Errorf("metric not named finops_agent_*: %s", n)
		}
	}
}

type clusterInfo struct{}

func (clusterInfo) GetClusterInfo() map[string]string { return map[string]string{"id": "cluster"} }

// seriesBudget bounds the agent's own series, whatever happens (I7). With every hook set, the
// label sets are closed: drops (17 emitter/reason pairs plus unknowns), upload attempts (4
// services × 5 results, plus unknowns), recovery (2 × 4, plus unknowns), emit_total (per emitter
// × 3), conditions (per component and type), and the histograms: upload duration per service,
// snapshot and node-stats duration, 15 series each.
const seriesBudget = 300

// A chaos-style run, where every event and source fires with random and hostile label values,
// keeps the series under a fixed budget.
func TestSeriesCardinalityStaysWithinBudget(t *testing.T) {
	m, reg := newRegistered(t)
	registry := health.NewRegistry()
	registry.SetPhase(health.PhaseRunning)
	m.SetHealthRegistry(registry)
	var condMu = make(chan []condition.Condition, 1)
	condMu <- nil
	registry.Register("chaos", health.ComponentFunc(func(context.Context, time.Time) health.Report {
		c := <-condMu
		condMu <- c
		return health.Report{Live: true, Conditions: c}
	}))
	m.SetExporter(func() emitter.ExporterStatus {
		return emitter.ExporterStatus{Emitters: []emitter.EmitterStatus{{ID: emitter.CldyEmitterID}, {ID: emitter.KubecostEmitterID}}}
	})
	m.SetUploadStatus(func() telemetry.UploadStatus { return telemetry.UploadStatus{BacklogFiles: 3} })
	m.SetKubecostExport(func() telemetry.KubecostExportCounters { return telemetry.KubecostExportCounters{WritesTotal: 5} })
	m.SetWAL(func() telemetry.WALCounters { return telemetry.WALCounters{Configured: true, WritesTotal: 5} })
	m.SetWindowGaps(func() []telemetry.WindowGaps {
		return []telemetry.WindowGaps{{Resolution: time.Hour, Reason: telemetry.WindowGapBackfillLimit, Total: 2}, {Resolution: 7 * time.Minute, Reason: "x", Total: 1}}
	})
	m.SetConversionFailures(func() map[string]uint64 { return map[string]uint64{"pods": 1, "widgets.example.com": 2} })

	rng := rand.New(rand.NewPCG(1, 2))
	word := func() string {
		pool := append(telemetry.DropReasons(), "disk_pressure", fmt.Sprintf("file-%d.tgz", rng.IntN(1e6)), fmt.Sprintf("node-%d", rng.IntN(1e6)))
		return pool[rng.IntN(len(pool))]
	}
	sink, obs := m.Cloudability(), m.Exporter()
	for i := range 20000 {
		switch rng.IntN(9) {
		case 0:
			sink.DataDropped(word(), 1+rng.IntN(5))
		case 1:
			sink.UploadAttempt(word(), word())
		case 2:
			sink.UploadDuration(word(), time.Duration(rng.IntN(1000))*time.Millisecond)
		case 3:
			sink.RecoveryItem(word(), word(), 1)
		case 4:
			obs.EmitterCall(emitter.EmitterID(word()), word())
		case 5:
			obs.SnapshotDuration(time.Second)
			obs.SnapshotComponentFailed(word())
		case 6:
			sink.EmissionSlotsSkipped(1)
			sink.UnfinalizedDiscarded(1)
			sink.HeadDeferred(1)
		case 7:
			m.NodeStatsDuration().Observe(1)
		case 8:
			var cs []condition.Condition
			for range rng.IntN(4) {
				cs = append(cs, condition.Condition{Type: []string{"disk_pressure", "upload_auth_failed", "snapshot_failing"}[rng.IntN(3)], Reason: word()})
			}
			<-condMu
			condMu <- cs
		}
		if i%1000 == 0 {
			series(t, reg) // scrape mid-run, as Prometheus would
		}
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	n := 0 // Prometheus series: a histogram is one per bucket, plus +Inf, _sum and _count
	for _, f := range families {
		for _, m := range f.GetMetric() {
			if h := m.GetHistogram(); h != nil {
				n += len(h.GetBucket()) + 3
			} else {
				n++
			}
		}
	}
	if n > seriesBudget {
		t.Fatalf("%d series after the chaos run; the budget is %d", n, seriesBudget)
	}
	t.Logf("%d series after the chaos run (budget %d)", n, seriesBudget)
}

// finops_agent_health_condition counts each component's active conditions by type, and stays at 0
// once a condition clears rather than disappearing.
func TestHealthConditionGauge(t *testing.T) {
	m, reg := newRegistered(t)
	registry := health.NewRegistry()
	m.SetHealthRegistry(registry)
	active := make(chan []condition.Condition, 1)
	active <- []condition.Condition{{Type: "emitter_failing", Reason: "a"}, {Type: "emitter_failing", Reason: "b"}}
	registry.Register("exporter", health.ComponentFunc(func(context.Context, time.Time) health.Report {
		c := <-active
		active <- c
		return health.Report{Live: true, Conditions: c}
	}))
	key := `finops_agent_health_condition{component="exporter",type="emitter_failing"}`
	if got := series(t, reg)[key]; got != 2 {
		t.Fatalf("%s = %v; want 2", key, got)
	}
	<-active
	active <- nil
	s := series(t, reg)
	if got, ok := s[key]; !ok || got != 0 {
		t.Fatalf("%s = %v (present %v) after clearing; want 0", key, got, ok)
	}
}

// The scrape-time gauges read the exporter's and uploader's heartbeats. Before anything has
// happened the timestamps are the agent's start, so "time since" alerts don't fire at startup.
func TestHeartbeatGauges(t *testing.T) {
	before := time.Now()
	m, reg := newRegistered(t)
	end := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	status := emitter.ExporterStatus{
		LastCycleEnd: end, CycleOverrunsTotal: 4,
		Emitters: []emitter.EmitterStatus{
			{ID: emitter.CldyEmitterID, State: emitter.EmitterReady},
			{ID: emitter.KubecostEmitterID, State: emitter.EmitterReady, ConsecutiveFailures: emitter.NotReadyAfterFailures},
		},
	}
	m.SetExporter(func() emitter.ExporterStatus { return status })
	m.SetUploadStatus(func() telemetry.UploadStatus { return telemetry.UploadStatus{BacklogFiles: 2, BacklogBytes: 2048} })

	s := series(t, reg)
	for k, want := range map[string]float64{
		"finops_agent_exporter_cycle_last_end_timestamp_seconds":       float64(end.Unix()),
		"finops_agent_exporter_cycle_overruns_total":                   4,
		`finops_agent_emitter_ready{emitter="cloudability"}`:           1,
		`finops_agent_emitter_ready{emitter="kubecost"}`:               0,
		"finops_agent_cldy_upload_backlog_files":                       2,
		"finops_agent_cldy_upload_backlog_bytes":                       2048,
		`finops_agent_emit_total{emitter="kubecost",result="skipped"}`: 0,
	} {
		if got, ok := s[k]; !ok || got != want {
			t.Errorf("%s = %v (present %v); want %v", k, got, ok, want)
		}
	}
	if got := s["finops_agent_cldy_upload_last_success_timestamp_seconds"]; got < float64(before.Unix()) || got > float64(time.Now().Unix()+1) {
		t.Errorf("last upload success with none yet = %v; want the agent's start", got)
	}
}

// I6: the status summary goes out in the Kubecost heartbeat's metadata, alongside OpenCost's own.
func TestHeartbeatMetadataCarriesTheSummary(t *testing.T) {
	m, _ := newRegistered(t)
	m.Cloudability().DataDropped(telemetry.ReasonBacklogAge, 2)
	m.SetUploadStatus(func() telemetry.UploadStatus { return telemetry.UploadStatus{BacklogFiles: 9} })
	registry := health.NewRegistry()
	registry.SetPhase(health.PhaseRunning)
	registry.Register("cldy-emitter", health.ComponentFunc(func(context.Context, time.Time) health.Report {
		return health.Report{Live: true, Conditions: []condition.Condition{{Type: "upload_auth_failed", Message: "token=secret"}}}
	}))
	m.SetHealthRegistry(registry)

	src := heartbeatexporter.NewHeartbeatSource("finops-agent", "v1", heartbeatexporter.NewMultiMetadataProvider(
		heartbeatexporter.NewLogLevelMetadataProvider(), m.HeartbeatMetadata()))
	hb := src.Make(time.Now())
	body, err := json.Marshal(hb)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Metadata map[string]json.RawMessage `json:"metadata"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatal(err)
	}
	if _, ok := doc.Metadata["logLevel"]; !ok {
		t.Errorf("OpenCost's own metadata is gone: %s", body)
	}
	var s telemetry.Summary
	if err := json.Unmarshal(doc.Metadata[telemetry.SummaryKey], &s); err != nil {
		t.Fatalf("metadata.%s: %v in %s", telemetry.SummaryKey, err, body)
	}
	if s.Ready || s.DataDroppedTotal != 2 || s.BacklogFiles != 9 || len(s.ActiveConditions) != 1 ||
		s.ActiveConditions[0].Component != "cldy-emitter" || s.ActiveConditions[0].Type != "upload_auth_failed" {
		t.Errorf("summary = %+v", s)
	}
	if strings.Contains(string(body), "secret") {
		t.Errorf("the heartbeat carries a condition message: %s", body)
	}
}

// Window gaps (chunk 06) are read at scrape time, per resolution, and summed in the summary.
func TestWindowGapsAndConversionFailures(t *testing.T) {
	m, reg := newRegistered(t)
	m.SetWindowGaps(func() []telemetry.WindowGaps {
		return []telemetry.WindowGaps{
			{Resolution: 10 * time.Minute, Reason: telemetry.WindowGapBackfillLimit, Total: 1},
			{Resolution: 24 * time.Hour, Reason: telemetry.WindowGapBackfillLimit, Total: 3},
		}
	})
	m.SetConversionFailures(func() map[string]uint64 { return map[string]uint64{"pods": 2} })
	s := series(t, reg)
	for k, want := range map[string]float64{
		`finops_agent_window_gaps_total{reason="backfill_limit",resolution="10m"}`: 1,
		`finops_agent_window_gaps_total{reason="backfill_limit",resolution="1d"}`:  3,
		`finops_agent_window_gaps_total{reason="backfill_limit",resolution="1h"}`:  0,
		`finops_agent_snapshot_object_conversion_failures_total{resource="pods"}`:  2,
		`finops_agent_snapshot_component_failures_total{component="node_stats"}`:   0,
	} {
		if got, ok := s[k]; !ok || got != want {
			t.Errorf("%s = %v (present %v); want %v", k, got, ok, want)
		}
	}
	if got := m.Summary().WindowGapsTotal; got != 4 {
		t.Errorf("summary window gaps = %d; want 4", got)
	}
}
