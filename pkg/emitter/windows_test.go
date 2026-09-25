package emitter

// Window bookkeeping tests (docs/reliability/FINDINGS.md chunk 06: F-06, F-39).

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// windowLedger records, per resolution and window start, whether a delivered snapshot covered
// the window at or after its end.
type windowLedger map[time.Duration]map[time.Time]bool

func (l windowLedger) record(at time.Time, summary *MetricsSummary) {
	for res, snaps := range map[time.Duration][]*MetricsSnapshot{
		10 * time.Minute: summary.Minutely, time.Hour: summary.Hourly, 24 * time.Hour: summary.Daily,
	} {
		for _, m := range snaps {
			if !at.Before(*m.Window.End()) {
				if l[res] == nil {
					l[res] = map[time.Time]bool{}
				}
				l[res][*m.Window.Start()] = true
			}
		}
	}
}

// Every closed window is either delivered after it closed or reported as a gap, never both,
// under random metrics and delivery failures, with and without the Prometheus-mode cache.
func TestWindowsDeliveredPlusGapsEqualElapsed(t *testing.T) {
	for _, useCache := range []bool{false, true} {
		t.Run(map[bool]string{false: "collector", true: "prometheus cache"}[useCache], func(t *testing.T) {
			rng := rand.New(rand.NewPCG(6, uint64(map[bool]int{false: 1, true: 2}[useCache])))
			clock := newTimeBender()
			ds := newReproDataSource()
			querier := &recordingQuerier{MetricsQuerier: ds.metrics, now: clock.now}
			ds.metrics = querier
			provider := NewConcurrentSnapshotProvider(&SnapshotConfig{
				UseMetricsCache:        useCache,
				MinutelyMetricsEnabled: true,
				KubernetesSnapshot:     NewKubernetesSnapshotConfig().EnableAll(),
				Now:                    clock.now,
			}).(*ConcurrentSnapshotProvider)

			start := mustTime(t, "2026-01-01T07:03:00Z")
			end := start.Add(50 * time.Hour)
			ledger := windowLedger{}
			outage := 0
			for at := start; at.Before(end); at = at.Add(time.Minute) {
				clock.current = at
				// outages of up to 3h, and single failed deliveries
				if outage == 0 && rng.IntN(200) == 0 {
					outage = rng.IntN(180) + 1
				}
				querier.fail.Store(outage > 0)
				if outage > 0 {
					outage--
				}
				snap, err := provider.SnapshotOf(ds)
				if err != nil {
					t.Fatalf("snapshot at %s: %v", at, err)
				}
				if snap.Metrics == nil || rng.IntN(20) == 0 {
					continue // metrics failed, or the consumer's Emit failed: not delivered
				}
				ledger.record(at, snap.Metrics)
				provider.CommitWindows(snap)
			}
			// deliver once more with everything healthy, so the last outage is settled
			clock.current = end
			querier.fail.Store(false)
			snap, _ := provider.SnapshotOf(ds)
			ledger.record(end, snap.Metrics)
			provider.CommitWindows(snap)

			for _, res := range []time.Duration{10 * time.Minute, time.Hour, 24 * time.Hour} {
				// windows fully inside [first delivered window, end)
				first := start.Truncate(res)
				if !first.Equal(start) {
					first = first.Add(res)
				}
				elapsed := uint64(0)
				for w := first; !w.Add(res).After(end.Truncate(res)); w = w.Add(res) {
					elapsed++
				}
				delivered := uint64(0)
				for w := range ledger[res] {
					if !w.Before(first) {
						delivered++
					}
				}
				gaps := provider.WindowGapsTotal(res)
				if delivered+gaps != elapsed {
					t.Errorf("%s: %d delivered + %d reported gaps != %d windows elapsed", res, delivered, gaps, elapsed)
				}
			}
			if provider.WindowGapsTotal(10*time.Minute) == 0 {
				t.Error("setup: expected outages long enough to report 10m gaps")
			}
		})
	}
}

// A snapshot whose metrics weren't delivered doesn't advance the watermark: the closed window
// is re-queried after it closes by the next delivered snapshot.
func TestUndeliveredWindowsAreRequeried(t *testing.T) {
	clock := newTimeBender()
	ds := newReproDataSource()
	provider := NewConcurrentSnapshotProvider(&SnapshotConfig{
		KubernetesSnapshot: NewKubernetesSnapshotConfig().EnableAll(),
		Now:                clock.now,
	}).(*ConcurrentSnapshotProvider)

	clock.current = mustTime(t, "2026-01-01T09:30:00Z")
	snap, _ := provider.SnapshotOf(ds)
	provider.CommitWindows(snap)

	// 10:00 and 10:30: taken, never delivered
	for _, at := range []string{"2026-01-01T10:00:00Z", "2026-01-01T10:30:00Z"} {
		clock.current = mustTime(t, at)
		if _, err := provider.SnapshotOf(ds); err != nil {
			t.Fatal(err)
		}
	}
	clock.current = mustTime(t, "2026-01-01T10:31:00Z")
	snap, _ = provider.SnapshotOf(ds)
	var starts []string
	for _, m := range snap.Metrics.Hourly {
		starts = append(starts, m.Window.Start().Format("15:04"))
	}
	if len(starts) != 2 || starts[0] != "09:00" || starts[1] != "10:00" {
		t.Errorf("hourly windows after undelivered snapshots = %v; want [09:00 10:00]", starts)
	}
}

// The watermarks are persisted on commit; a new provider loads them, backfills the last closed
// window and reports the older ones as a gap.
func TestWatermarksPersistAcrossRestart(t *testing.T) {
	file := filepath.Join(t.TempDir(), "state", "watermarks.json")
	clock := newTimeBender()
	ds := newReproDataSource()
	newProvider := func() *ConcurrentSnapshotProvider {
		return NewConcurrentSnapshotProvider(&SnapshotConfig{
			MinutelyMetricsEnabled: true,
			KubernetesSnapshot:     NewKubernetesSnapshotConfig().EnableAll(),
			Now:                    clock.now,
			WatermarkFile:          file,
		}).(*ConcurrentSnapshotProvider)
	}

	clock.current = mustTime(t, "2026-01-01T09:05:00Z")
	before := newProvider()
	snap, _ := before.SnapshotOf(ds)
	before.CommitWindows(snap)
	if err := before.WatermarkPersistError(); err != nil {
		t.Fatalf("persist: %v", err)
	}

	var state watermarkState
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("watermarks not persisted: %v", err)
	}
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatal(err)
	}
	if got := state.Watermarks["10m0s"]; !got.Equal(mustTime(t, "2026-01-01T09:00:00Z")) {
		t.Errorf("persisted 10m watermark = %s; want 09:00", got)
	}
	if entries, _ := os.ReadDir(filepath.Dir(file)); len(entries) != 1 {
		t.Errorf("state dir has %d entries; want only watermarks.json (no temp files)", len(entries))
	}

	// "restart" 40 minutes later
	clock.current = mustTime(t, "2026-01-01T09:45:00Z")
	after := newProvider()
	if got := after.Watermarks()[10*time.Minute]; !got.Equal(mustTime(t, "2026-01-01T09:00:00Z")) {
		t.Fatalf("loaded 10m watermark = %s; want 09:00", got)
	}
	snap, _ = after.SnapshotOf(ds)
	var starts []string
	for _, m := range snap.Metrics.Minutely {
		starts = append(starts, m.Window.Start().Format("15:04"))
	}
	if len(starts) != 2 || starts[0] != "09:30" || starts[1] != "09:40" {
		t.Errorf("10m windows after restart = %v; want the backfilled 09:30 and the current 09:40", starts)
	}
	after.CommitWindows(snap)
	if got := after.WindowGapsTotal(10 * time.Minute); got != 3 {
		t.Errorf("reported 10m gap = %d windows; want 3 (09:00-09:30)", got)
	}
	if got := after.WindowGapsTotal(time.Hour); got != 0 {
		t.Errorf("reported 1h gap = %d; want 0", got)
	}
}

// A corrupt watermark file is reported and ignored; the provider starts from the current windows.
func TestCorruptWatermarkFileIgnored(t *testing.T) {
	file := filepath.Join(t.TempDir(), "watermarks.json")
	if err := os.WriteFile(file, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	provider := NewConcurrentSnapshotProvider(&SnapshotConfig{WatermarkFile: file}).(*ConcurrentSnapshotProvider)
	if got := provider.Watermarks(); len(got) != 0 {
		t.Errorf("watermarks from a corrupt file = %v; want none", got)
	}
}

// requiringEmitter counts Emit calls and declares its requirements; Emit fails while failing is set.
type requiringEmitter struct {
	id       EmitterID
	required []SnapshotComponent
	emits    atomic.Int32
	failing  atomic.Bool
}

func (e *requiringEmitter) ID() EmitterID                           { return e.id }
func (e *requiringEmitter) RequiredComponents() []SnapshotComponent { return e.required }
func (e *requiringEmitter) Init(*ClusterSnapshot) error             { return nil }
func (e *requiringEmitter) Emit(context.Context, *ClusterSnapshot) error {
	e.emits.Add(1)
	if e.failing.Load() {
		return errors.New("emit failed")
	}
	return nil
}

// I5: a failing node-stats collection doesn't stop Kubecost, and a failing metrics source doesn't
// stop Cloudability. The skipped emitter's skips are counted by reason.
func TestFailedComponentSkipsOnlyEmittersThatNeedIt(t *testing.T) {
	ds := newReproDataSource()
	querier := &recordingQuerier{MetricsQuerier: ds.metrics, now: defaultNow}
	ds.metrics = querier
	provider := NewConcurrentSnapshotProvider(DefaultSnapshotConfig())
	kc := &requiringEmitter{id: KubecostEmitterID, required: []SnapshotComponent{ComponentClusterInfo, ComponentKubernetes, ComponentMetrics}}
	cl := &requiringEmitter{id: CldyEmitterID, required: []SnapshotComponent{ComponentKubernetes, ComponentNodeStats}}
	exp := NewExporter(ds, provider, kc, cl)
	if !exp.Start(10 * time.Millisecond) {
		t.Fatal("failed to start exporter")
	}
	defer exp.Stop()

	for _, tc := range []struct {
		fail    func(bool)
		runs    *requiringEmitter
		skipped *requiringEmitter
		reason  string
	}{
		{func(f bool) { ds.statsFail.Store(f) }, kc, cl, "missing_node_stats"},
		{func(f bool) { querier.fail.Store(f) }, cl, kc, "missing_metrics"},
	} {
		tc.fail(true)
		time.Sleep(30 * time.Millisecond) // let an in-flight cycle finish
		runs, skipped := tc.runs.emits.Load(), tc.skipped.emits.Load()
		if !waitFor(2*time.Second, func() bool { return tc.runs.emits.Load() > runs+3 }) {
			t.Errorf("%s: %s stopped emitting while a component it doesn't need failed", tc.reason, tc.runs.id)
		}
		if got := tc.skipped.emits.Load(); got > skipped+1 {
			t.Errorf("%s: %s emitted %d times without its required component", tc.reason, tc.skipped.id, got-skipped)
		}
		status := exp.Status()
		for _, es := range status.Emitters {
			if es.ID == tc.skipped.id && es.SkippedTotal[tc.reason] == 0 {
				t.Errorf("%s: no skips counted for %s: %v", tc.reason, es.ID, es.SkippedTotal)
			}
		}
		tc.fail(false)
	}
}

// The exporter commits a snapshot's windows only after every emitter that needs metrics
// received it: while Kubecost's Emit fails, the watermark stays put.
func TestExporterCommitsWindowsOnlyAfterDelivery(t *testing.T) {
	var clock atomic.Pointer[time.Time]
	setClock := func(s string) {
		ts := mustTime(t, s)
		clock.Store(&ts)
	}
	setClock("2026-01-01T09:30:00Z")
	ds := newReproDataSource()
	config := DefaultSnapshotConfig()
	config.Now = func() time.Time { return *clock.Load() }
	provider := NewConcurrentSnapshotProvider(config).(*ConcurrentSnapshotProvider)
	kc := &requiringEmitter{id: KubecostEmitterID, required: []SnapshotComponent{ComponentClusterInfo, ComponentKubernetes, ComponentMetrics}}
	exp := NewExporter(ds, provider, kc)
	if !exp.Start(10 * time.Millisecond) {
		t.Fatal("failed to start exporter")
	}
	defer exp.Stop()

	nine := mustTime(t, "2026-01-01T09:00:00Z")
	if !waitFor(2*time.Second, func() bool { return provider.Watermarks()[time.Hour].Equal(nine) }) {
		t.Fatalf("Init didn't commit the current window: %v", provider.Watermarks())
	}
	kc.failing.Store(true)
	setClock("2026-01-01T11:30:00Z")
	emits := kc.emits.Load()
	if !waitFor(2*time.Second, func() bool { return kc.emits.Load() > emits+3 }) {
		t.Fatal("kubecost stopped emitting")
	}
	if got := provider.Watermarks()[time.Hour]; !got.Equal(nine) {
		t.Errorf("hourly watermark advanced to %s while Emit failed", got)
	}
	kc.failing.Store(false)
	eleven := mustTime(t, "2026-01-01T11:00:00Z")
	if !waitFor(2*time.Second, func() bool { return provider.Watermarks()[time.Hour].Equal(eleven) }) {
		t.Errorf("hourly watermark = %s after Emit recovered; want 11:00", provider.Watermarks()[time.Hour])
	}
	if got := provider.WindowGapsTotal(time.Hour); got != 1 {
		t.Errorf("hourly gaps = %d; want 1 (09:00, beyond the backfill limit)", got)
	}
}
