package adapters

import (
	"testing"
	"time"

	"github.com/ibm/finops-agent/internal/mocks"
	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/opencost/opencost/core/pkg/opencost"
)

func TestMetricsQuerierSnapshotSelect(t *testing.T) {
	t.Setenv("MINUTE_METRICS_ENABLED", "true")

	ds := mocks.NewMockDataSource()
	config := emitter.NewSnapshotConfigFromEnv()

	// deterministic time to avoid boundary cases
	now, _ := time.Parse(time.RFC3339, "2025-01-01T15:06:22Z")
	config.Now = func() time.Time { return now }

	snapshotter := emitter.NewConcurrentSnapshotProvider(config)

	snapshot, err := snapshotter.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("failed to create snapshot: %v", err)
	}

	metricsQuerier := NewMetricsQuerierAdapter(snapshot.Metrics)

	s10m := now.Truncate(10 * time.Minute)
	e10m := s10m.Add(10 * time.Minute)
	snap10m := metricsQuerier.metricsSnapshotFor(s10m, e10m)

	if snap10m == nil {
		t.Fatalf("expected snapshot for 10m interval, got nil")
	}

	s1h := now.Truncate(time.Hour)
	e1h := s1h.Add(time.Hour)
	snap1h := metricsQuerier.metricsSnapshotFor(s1h, e1h)
	if snap1h == nil {
		t.Fatalf("expected snapshot for 1h interval, got nil")
	}

	s1d := now.Truncate(24 * time.Hour)
	e1d := s1d.Add(24 * time.Hour)
	snap1d := metricsQuerier.metricsSnapshotFor(s1d, e1d)
	if snap1d == nil {
		t.Fatalf("expected snapshot for 24h interval, got nil")
	}
}

func TestUpdateNewerSnapshots(t *testing.T) {
	t.Setenv("MINUTE_METRICS_ENABLED", "true")

	ds := mocks.NewMockDataSource()
	config := emitter.NewSnapshotConfigFromEnv()
	snapshotter := emitter.NewConcurrentSnapshotProvider(config)

	snapshot, err := snapshotter.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("failed to create snapshot: %v", err)
	}

	snapshot2, err := snapshotter.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("failed to create snapshot: %v", err)
	}

	metricsQuerier := NewMetricsQuerierAdapter(snapshot.Metrics)

	n10m := *snapshot.Metrics.Minutely[0].Window.Start()
	s10m := n10m.Truncate(10 * time.Minute)
	e10m := s10m.Add(10 * time.Minute)

	n1h := *snapshot.Metrics.Hourly[0].Window.Start()
	s1h := n1h.Truncate(time.Hour)
	e1h := s1h.Add(time.Hour)

	n1d := *snapshot.Metrics.Daily[0].Window.Start()
	s1d := n1d.Truncate(24 * time.Hour)
	e1d := s1d.Add(24 * time.Hour)

	// directly update the snapshot2 metrics to look like "future" metrics
	newMetrics := snapshot2.Metrics

	nexts10m := e10m
	nexte10m := nexts10m.Add(10 * time.Minute)

	newMetrics.Minutely[0].Window = opencost.NewClosedWindow(nexts10m, nexte10m)

	nexts1h := e1h
	nexte1h := nexts1h.Add(time.Hour)

	newMetrics.Hourly[0].Window = opencost.NewClosedWindow(nexts1h, nexte1h)

	nexts1d := e1d
	nexte1d := nexts1d.Add(24 * time.Hour)

	newMetrics.Daily[0].Window = opencost.NewClosedWindow(nexts1d, nexte1d)

	metricsQuerier.Update(newMetrics)

	// now check that the metricsQuerier can select from current and older snapshots
	snap10m := metricsQuerier.metricsSnapshotFor(s10m, e10m)
	if snap10m == nil {
		t.Fatalf("expected snapshot for 10m interval, got nil")
	}

	snap1h := metricsQuerier.metricsSnapshotFor(s1h, e1h)
	if snap1h == nil {
		t.Fatalf("expected snapshot for 1h interval, got nil")
	}

	snap1d := metricsQuerier.metricsSnapshotFor(s1d, e1d)
	if snap1d == nil {
		t.Fatalf("expected snapshot for 24h interval, got nil")
	}

	// check that the metricsQuerier can select from the new snapshot
	snap10m = metricsQuerier.metricsSnapshotFor(nexts10m, nexte10m)
	if snap10m == nil {
		t.Fatalf("expected snapshot for 10m interval, got nil")
	}

	snap1h = metricsQuerier.metricsSnapshotFor(nexts1h, nexte1h)
	if snap1h == nil {
		t.Fatalf("expected snapshot for 1h interval, got nil")
	}

	snap1d = metricsQuerier.metricsSnapshotFor(nexts1d, nexte1d)
	if snap1d == nil {
		t.Fatalf("expected snapshot for 24h interval, got nil")
	}
}

func TestMinuteMetricsTick(t *testing.T) {
	t.Setenv("MINUTE_METRICS_ENABLED", "true")

	ds := mocks.NewMockDataSource()
	config := emitter.NewSnapshotConfigFromEnv()

	// deterministic time to avoid boundary cases
	c, _ := time.Parse(time.RFC3339, "2025-01-01T15:06:22Z")
	current := &c

	config.Now = func() time.Time { return *current }

	snapshotter := emitter.NewConcurrentSnapshotProvider(config)

	snapshot, err := snapshotter.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("failed to create snapshot: %v", err)
	}

	metricsQuerier := NewMetricsQuerierAdapter(snapshot.Metrics)

	// tick an hour
	for i := 0; i < 60; i++ {
		// this will advance the "now" in the snapshotter context
		*current = current.Add(time.Minute)

		snapshot, err := snapshotter.SnapshotOf(ds)
		if err != nil {
			t.Fatalf("failed to create snapshot: %v", err)
		}

		metricsQuerier.Update(snapshot.Metrics)

		/*
			for k := range metricsQuerier.tenMinuteResolution.snapshots {
				t.Logf("%s", time.Unix(k, 0).Format(time.RFC3339))
			}
			t.Logf("-------------")
		*/
	}

	totalMinuteSnapshots := len(metricsQuerier.tenMinuteResolution.snapshots)
	totalHourlySnapshots := len(metricsQuerier.hourlyResolution.snapshots)
	totalDailySnapshots := len(metricsQuerier.dailyResolution.snapshots)

	if totalMinuteSnapshots != MaxBackfillSnapshots {
		t.Fatalf("Total 10m snapshots is: %d, expected: %d", totalMinuteSnapshots, MaxBackfillSnapshots)
	}
	if totalHourlySnapshots != MaxBackfillSnapshots {
		t.Fatalf("Total 1h snapshots is: %d, expected: %d", totalHourlySnapshots, MaxBackfillSnapshots)
	}
	if totalDailySnapshots != 1 {
		t.Fatalf("Total 24h snapshots is: %d, expected: 1", totalDailySnapshots)
	}
}
