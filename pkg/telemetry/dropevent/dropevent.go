// Package dropevent logs data drops in the agent's one structured form (docs/reliability/
// FINDINGS.md chunk 09, invariant I1). Counting them is pkg/telemetry's job.
package dropevent

import (
	"fmt"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
)

// Drop is one data-loss event for Log.
type Drop struct {
	// Emitter and Reason are the labels of finops_agent_data_dropped_total.
	Emitter string
	Reason  string
	// Count is the number of items lost: samples, payloads, pods.
	Count int
	// Bytes is their size on disk, when known.
	Bytes int64
	// WindowStart and WindowEnd bound the time the lost data covers, when known.
	WindowStart, WindowEnd time.Time
	// Detail says what was lost and why. It may name files; it is logged, never a label.
	Detail string
}

// QuarantineEvicted is the reason of a quarantined item removed to bound the quarantine. It is
// logged at Warn: the item was logged as dropped when it was quarantined.
const QuarantineEvicted = "quarantine_evicted"

// Log logs a drop at Error in the one structured form every drop uses:
//
//	event=data_dropped emitter=… reason=… count=… bytes=… window_start=… window_end=…: detail
//
// bytes and the window are left out when unknown. It only logs: the caller counts the drop in
// finops_agent_data_dropped_total through its sink or source. Callers that lose many items in one
// cycle log them once, summarised (Batch).
func Log(d Drop) {
	log.Errorf("%s", formatDrop("data_dropped", d))
}

// LogQuarantineEvicted logs, at Warn, quarantined items removed to bound the quarantine. They
// were counted and logged as dropped when quarantined.
func LogQuarantineEvicted(d Drop) {
	log.Warnf("%s", formatDrop(QuarantineEvicted, d))
}

func formatDrop(event string, d Drop) string {
	var b strings.Builder
	fmt.Fprintf(&b, "event=%s emitter=%s reason=%s count=%d", event, d.Emitter, d.Reason, d.Count)
	if d.Bytes > 0 {
		fmt.Fprintf(&b, " bytes=%d", d.Bytes)
	}
	if !d.WindowStart.IsZero() {
		fmt.Fprintf(&b, " window_start=%s", d.WindowStart.UTC().Format(time.RFC3339))
	}
	if !d.WindowEnd.IsZero() {
		fmt.Fprintf(&b, " window_end=%s", d.WindowEnd.UTC().Format(time.RFC3339))
	}
	if d.Detail != "" {
		b.WriteString(": ")
		b.WriteString(d.Detail)
	}
	return b.String()
}

// Batch accumulates the drops of one cycle, per reason, to log each reason once (Log).
// The zero value is ready to use. It is not safe for concurrent use.
type Batch struct {
	emitter string
	drops   map[string]*Drop
	order   []string
}

// NewBatch returns a batch of emitter's drops.
func NewBatch(emitter string) *Batch {
	return &Batch{emitter: emitter}
}

// Add adds count items of bytes, covering the time at, lost for reason. detail is kept from the
// first item of each reason.
func (b *Batch) Add(reason string, count int, bytes int64, at time.Time, detail string) {
	if b.drops == nil {
		b.drops = map[string]*Drop{}
	}
	d, ok := b.drops[reason]
	if !ok {
		d = &Drop{Emitter: b.emitter, Reason: reason, Detail: detail}
		b.drops[reason] = d
		b.order = append(b.order, reason)
	}
	d.Count += count
	d.Bytes += bytes
	if !at.IsZero() {
		if d.WindowStart.IsZero() || at.Before(d.WindowStart) {
			d.WindowStart = at
		}
		if at.After(d.WindowEnd) {
			d.WindowEnd = at
		}
	}
}

// Flush logs each reason's drops once and empties the batch.
func (b *Batch) Flush() {
	for _, reason := range b.order {
		d := *b.drops[reason]
		if d.Count > 1 {
			d.Detail = fmt.Sprintf("%d items, the first: %s", d.Count, d.Detail)
		}
		if reason == QuarantineEvicted {
			LogQuarantineEvicted(d)
		} else {
			Log(d)
		}
	}
	b.drops, b.order = nil, nil
}
