package emitter

import (
	"testing"
	"time"

	"github.com/ibm/finops-agent/internal/mocks"
)

func TestSnapshotWindowsForZeroLastSnapshot(t *testing.T) {
	now := time.Date(2025, 1, 15, 9, 5, 30, 0, time.UTC)
	windows := snapshotWindowsFor(now, time.Time{}, 10*time.Minute)

	if len(windows) != 1 {
		t.Fatalf("Expected 1 window for zero lastSnapshot, got %d", len(windows))
	}

	start := windows[0].Start()
	end := windows[0].End()
	if !start.Equal(time.Date(2025, 1, 15, 9, 0, 0, 0, time.UTC)) {
		t.Errorf("Unexpected window start: %s", *start)
	}
	if !end.Equal(time.Date(2025, 1, 15, 9, 10, 0, 0, time.UTC)) {
		t.Errorf("Unexpected window end: %s", *end)
	}
}

func TestSnapshotWindowsForSameWindow(t *testing.T) {
	now := time.Date(2025, 1, 15, 9, 7, 30, 0, time.UTC)
	last := time.Date(2025, 1, 15, 9, 3, 0, 0, time.UTC)
	windows := snapshotWindowsFor(now, last, 10*time.Minute)

	if len(windows) != 1 {
		t.Fatalf("Expected 1 window (same boundary), got %d", len(windows))
	}
}

func TestSnapshotWindowsForBoundaryCrossing(t *testing.T) {
	now := time.Date(2025, 1, 15, 9, 11, 0, 0, time.UTC)
	last := time.Date(2025, 1, 15, 9, 9, 0, 0, time.UTC)
	windows := snapshotWindowsFor(now, last, 10*time.Minute)

	if len(windows) != 2 {
		t.Fatalf("Expected 2 windows (boundary crossing), got %d", len(windows))
	}

	s0 := windows[0].Start()
	e0 := windows[0].End()
	if !s0.Equal(time.Date(2025, 1, 15, 9, 0, 0, 0, time.UTC)) {
		t.Errorf("Window[0] unexpected start: %s", *s0)
	}
	if !e0.Equal(time.Date(2025, 1, 15, 9, 10, 0, 0, time.UTC)) {
		t.Errorf("Window[0] unexpected end: %s", *e0)
	}

	s1 := windows[1].Start()
	e1 := windows[1].End()
	if !s1.Equal(time.Date(2025, 1, 15, 9, 10, 0, 0, time.UTC)) {
		t.Errorf("Window[1] unexpected start: %s", *s1)
	}
	if !e1.Equal(time.Date(2025, 1, 15, 9, 20, 0, 0, time.UTC)) {
		t.Errorf("Window[1] unexpected end: %s", *e1)
	}
}

func TestSnapshotWindowsHourlyBoundaryCrossing(t *testing.T) {
	now := time.Date(2025, 1, 15, 16, 0, 30, 0, time.UTC)
	last := time.Date(2025, 1, 15, 15, 59, 0, 0, time.UTC)
	windows := snapshotWindowsFor(now, last, time.Hour)

	if len(windows) != 2 {
		t.Fatalf("Expected 2 windows (hourly boundary crossing), got %d", len(windows))
	}
}

func TestSnapshotWindowsDailyBoundaryCrossing(t *testing.T) {
	now := time.Date(2025, 1, 16, 0, 0, 30, 0, time.UTC)
	last := time.Date(2025, 1, 15, 23, 59, 0, 0, time.UTC)
	windows := snapshotWindowsFor(now, last, 24*time.Hour)

	if len(windows) != 2 {
		t.Fatalf("Expected 2 windows (daily boundary crossing), got %d", len(windows))
	}
}

func TestSnapshotMinutelyMetricsEnabled(t *testing.T) {
	t.Setenv("MINUTE_METRICS_ENABLED", "true")

	ds := mocks.NewMockDataSource()
	config := NewSnapshotConfigFromEnv()

	provider := NewConcurrentSnapshotProvider(config)
	snapshot, err := provider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	if snapshot.Metrics.Minutely == nil {
		t.Error("Expected minutely metrics to be populated when enabled")
	}
}

func TestSnapshotMinutelyMetricsDisabled(t *testing.T) {
	ds := mocks.NewMockDataSource()
	config := DefaultSnapshotConfig()
	config.MinutelyMetricsEnabled = false

	provider := NewConcurrentSnapshotProvider(config)
	snapshot, err := provider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	if len(snapshot.Metrics.Minutely) != 0 {
		t.Error("Expected minutely metrics to be empty when disabled")
	}
}

func TestSequentialSnapshotsTrackWindows(t *testing.T) {
	ds := mocks.NewMockDataSource()
	bender := newTimeBender()

	config := DefaultSnapshotConfig()
	config.MinutelyMetricsEnabled = true
	config.Now = bender.now

	provider := NewConcurrentSnapshotProvider(config)

	bender.current = time.Date(2025, 6, 1, 15, 5, 0, 0, time.UTC)
	snap1, err := provider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("First snapshot failed: %v", err)
	}

	if len(snap1.Metrics.Hourly) != 1 {
		t.Errorf("First snapshot: expected 1 hourly window, got %d", len(snap1.Metrics.Hourly))
	}

	bender.current = time.Date(2025, 6, 1, 16, 1, 0, 0, time.UTC)
	snap2, err := provider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("Second snapshot failed: %v", err)
	}

	if len(snap2.Metrics.Hourly) != 2 {
		t.Errorf("Second snapshot: expected 2 hourly windows, got %d", len(snap2.Metrics.Hourly))
	}
}
