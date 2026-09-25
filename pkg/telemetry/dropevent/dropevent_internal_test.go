package dropevent

import (
	"testing"
	"time"
)

// Every drop is logged in one structured form, leaving out what isn't known.
func TestFormatDrop(t *testing.T) {
	start := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		d    Drop
		want string
	}{
		{Drop{Emitter: "cloudability", Reason: "backlog_age", Count: 2, Bytes: 10, WindowStart: start, WindowEnd: start.Add(time.Hour), Detail: "why"},
			"event=data_dropped emitter=cloudability reason=backlog_age count=2 bytes=10 window_start=2026-09-25T10:00:00Z window_end=2026-09-25T11:00:00Z: why"},
		{Drop{Emitter: "exporter", Reason: "snapshot_failed", Count: 5},
			"event=data_dropped emitter=exporter reason=snapshot_failed count=5"},
	} {
		if got := formatDrop("data_dropped", tc.d); got != tc.want {
			t.Errorf("got  %s\nwant %s", got, tc.want)
		}
	}
}

// A batch sums each reason's items, bytes and time range, and empties on Flush.
func TestBatch(t *testing.T) {
	t0 := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	b := NewBatch("cloudability")
	b.Add("disk_pressure", 1, 100, t0.Add(time.Hour), "second")
	b.Add("disk_pressure", 1, 50, t0, "first")
	b.Add(QuarantineEvicted, 1, 7, time.Time{}, "q")
	d := b.drops["disk_pressure"]
	if d.Count != 2 || d.Bytes != 150 || !d.WindowStart.Equal(t0) || !d.WindowEnd.Equal(t0.Add(time.Hour)) || d.Detail != "second" {
		t.Errorf("batched drop = %+v", *d)
	}
	if len(b.order) != 2 {
		t.Errorf("order = %v", b.order)
	}
	b.Flush()
	if len(b.drops) != 0 || len(b.order) != 0 {
		t.Errorf("Flush left %v", b.order)
	}
}
