package cldy_test

// The Cloudability emitter's and uploader's events reach the agent's Prometheus metrics
// (docs/reliability/FINDINGS.md chunk 09) through UploaderConfig.Events, as main wires them.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ibm/finops-agent/cldy"
	"github.com/ibm/finops-agent/pkg/telemetry"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// gathered returns every finops_agent_ sample in reg as name{labels} -> value (a histogram's
// value is its sample count).
func gathered(t *testing.T, reg prometheus.Gatherer) map[string]float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]float64{}
	for _, f := range families {
		for _, m := range f.GetMetric() {
			var labels []string
			for _, l := range m.GetLabel() {
				labels = append(labels, fmt.Sprintf("%s=%q", l.GetName(), l.GetValue()))
			}
			key := f.GetName()
			if len(labels) > 0 {
				key += "{" + strings.Join(labels, ",") + "}"
			}
			out[key] = metricValue(m)
		}
	}
	return out
}

func metricValue(m *dto.Metric) float64 {
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

// Startup recovery, drops and uploads are counted in Prometheus exactly as in the uploader's own
// counts: each recovered, quarantined, expired and unfinished item once, each drop by reason, and
// each upload attempt by service and result.
func TestUploaderEventsReachPrometheus(t *testing.T) {
	m := telemetry.New()
	reg := prometheus.NewPedanticRegistry()
	if err := m.Register(reg); err != nil {
		t.Fatal(err)
	}

	clock := newFakeClock(time.Now().Truncate(time.Second))
	now := clock.Now()
	scratch := newProdScratch(t, t.TempDir(), "cid-telemetry")
	scratch.AddUpload(t, now.Add(-100*time.Hour))                                                            // expired: dropped
	scratch.AddUpload(t, now.Add(-time.Hour))                                                                // recovered
	if err := os.WriteFile(filepath.Join(scratch.UploadDir(), "junk.txt"), []byte("x"), 0o644); err != nil { // quarantined
		t.Fatal(err)
	}
	scratch.AddCompleteSample(t, now.Add(-30*time.Minute), 0)                                                              // recovered into a payload
	scratch.AddIncompleteSample(t, now.Add(-20*time.Minute), 1)                                                            // no manifest: quarantined
	if err := os.MkdirAll(filepath.Join(scratch.ClusterScratchDir(), cldy.StagingPrefix+"1_2"), os.ModePerm); err != nil { // unfinished
		t.Fatal(err)
	}

	config := scratch.UploaderConfig(t)
	config.RecoveryPeriod = queueRecoveryPeriod
	config.Events = m.Cloudability()
	client := &scriptedClient{status: always(200)}
	cu := cldy.NewUploaderForTest(config, []cldy.StorageService{apptioService(client)}, clock.Now)
	cu.SetClusterID(scratch.ClusterID)
	clock.Advance(10 * time.Minute)
	cu.UploadCycleForTest()

	if got := len(client.Delivered()); got != 2 {
		t.Fatalf("delivered %d payloads; want the recovered payload and the recovered sample's", got)
	}
	got := gathered(t, reg)
	for k, want := range map[string]float64{
		`finops_agent_data_dropped_total{emitter="cloudability",reason="recovery_expired"}`:     1,
		`finops_agent_data_dropped_total{emitter="cloudability",reason="invalid_payload"}`:      1,
		`finops_agent_data_dropped_total{emitter="cloudability",reason="invalid_sample"}`:       1,
		`finops_agent_cldy_recovery_items_total{kind="payload",outcome="dropped"}`:              1,
		`finops_agent_cldy_recovery_items_total{kind="payload",outcome="quarantined"}`:          1,
		`finops_agent_cldy_recovery_items_total{kind="payload",outcome="recovered"}`:            1,
		`finops_agent_cldy_recovery_items_total{kind="sample",outcome="recovered"}`:             1,
		`finops_agent_cldy_recovery_items_total{kind="sample",outcome="quarantined"}`:           1,
		`finops_agent_cldy_recovery_items_total{kind="sample",outcome="discarded_unfinalized"}`: 1,
		`finops_agent_cldy_unfinalized_discarded_total`:                                         1,
		`finops_agent_cldy_upload_attempts_total{result="ok",service="frontdoor"}`:              2,
		`finops_agent_cldy_upload_duration_seconds{service="frontdoor"}`:                        2,
	} {
		if got[k] != want {
			t.Errorf("%s = %v; want %v", k, got[k], want)
		}
	}

	var sum float64
	for k, v := range got {
		if strings.HasPrefix(k, "finops_agent_data_dropped_total{") {
			sum += v
		}
	}
	counts := cu.EventsForTest()
	var want int
	for _, n := range counts.Dropped {
		want += n
	}
	if sum != float64(want) || sum != 3 {
		t.Errorf("data_dropped_total sums to %v; the uploader counted %d drops (%v); want 3", sum, want, counts.Dropped)
	}
}

// The status summary and upload gauges read the uploader's heartbeat.
func TestEmitterUploadStatus(t *testing.T) {
	clock := newFakeClock(time.Now().Truncate(time.Second))
	scratch := newProdScratch(t, t.TempDir(), "cid-upload-status")
	config := scratch.UploaderConfig(t)
	config.RecoveryPeriod = queueRecoveryPeriod
	client := &scriptedClient{status: always(503)}
	cu := cldy.NewUploaderForTest(config, []cldy.StorageService{apptioService(client)}, clock.Now)
	cu.SetClusterID(scratch.ClusterID)
	scratch.AddCompleteSample(t, clock.Now(), 0)
	clock.Advance(10 * time.Minute)
	cu.UploadCycleForTest()

	ce := cldy.NewEmitterForTest(cldy.EmitterConfig{UploaderConfig: config}, cu, clock.Now)
	s := ce.UploadStatus()
	if s.BacklogFiles != 1 || s.BacklogBytes <= 0 || !s.LastSuccess.IsZero() {
		t.Fatalf("upload status with one undelivered payload = %+v", s)
	}
}

// promUploader builds an uploader on scratch whose events go to a fresh Prometheus registry, as in
// production.
func promUploader(t *testing.T, scratch *prodScratch, services []cldy.StorageService, clock *fakeClock) (*cldy.CldyUploader, *prometheus.Registry) {
	t.Helper()
	m := telemetry.New()
	reg := prometheus.NewPedanticRegistry()
	if err := m.Register(reg); err != nil {
		t.Fatal(err)
	}
	config := scratch.UploaderConfig(t)
	config.RecoveryPeriod = queueRecoveryPeriod
	config.Events = m.Cloudability()
	cu := cldy.NewUploaderForTest(config, services, clock.Now)
	cu.SetClusterID(scratch.ClusterID)
	return cu, reg
}

func droppedTotal(got map[string]float64) float64 {
	var sum float64
	for k, v := range got {
		if strings.HasPrefix(k, "finops_agent_data_dropped_total{") {
			sum += v
		}
	}
	return sum
}

// A quarantined item evicted to bound the quarantine is counted once, as dropped when it was
// quarantined, and apart as quarantine_evicted: through the real quarantine and trim.
func TestQuarantineEvictionCountedOnceInPrometheus(t *testing.T) {
	restore := cldy.SetQuarantineLimitsForTest(1, 7*24*time.Hour) // any quarantined item is over the bound
	defer restore()
	clock := newFakeClock(time.Now().Truncate(time.Second))
	scratch := newProdScratch(t, t.TempDir(), "cid-quarantine-prom")
	if err := os.WriteFile(filepath.Join(scratch.UploadDir(), "junk.txt"), []byte("junk"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, reg := promUploader(t, scratch, nil, clock)

	got := gathered(t, reg)
	if got[`finops_agent_data_dropped_total{emitter="cloudability",reason="invalid_payload"}`] != 1 ||
		got["finops_agent_cldy_quarantine_evicted_total"] != 1 || droppedTotal(got) != 1 {
		t.Errorf("invalid_payload %v, quarantine_evicted %v, data_dropped_total sum %v; want 1, 1, 1",
			got[`finops_agent_data_dropped_total{emitter="cloudability",reason="invalid_payload"}`],
			got["finops_agent_cldy_quarantine_evicted_total"], droppedTotal(got))
	}
}

// Disk-pressure evictions through the real disk budget are counted in Prometheus as the uploader
// counts them.
func TestDiskPressureEvictionsReachPrometheus(t *testing.T) {
	clock := newFakeClock(time.Now().Truncate(time.Second))
	scratch := newProdScratch(t, t.TempDir(), "cid-disk-prom")
	for i := range 3 {
		scratch.AddUpload(t, clock.Now().Add(time.Duration(i-3)*time.Hour))
	}
	client := &scriptedClient{status: always(503)}
	cu, reg := promUploader(t, scratch, []cldy.StorageService{apptioService(client)}, clock)
	restore := cldy.SetDiskAvailableForTest(diskFullWhileUploadsOver(scratch, 1))
	defer restore()
	scratch.AddCompleteSample(t, clock.Now(), 0)
	clock.Advance(10 * time.Minute)
	cu.UploadCycleForTest()

	got := gathered(t, reg)
	counts := cu.EventsForTest()
	evicted := got[`finops_agent_data_dropped_total{emitter="cloudability",reason="disk_pressure"}`]
	if evicted == 0 || evicted != float64(counts.Dropped[cldy.DropReasonDiskPressure]) || droppedTotal(got) != evicted {
		t.Errorf("disk_pressure in Prometheus %v, uploader counted %v, total %v", evicted, counts.Dropped, droppedTotal(got))
	}
}
