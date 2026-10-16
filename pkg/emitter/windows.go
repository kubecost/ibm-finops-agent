package emitter

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
)

// MaxBackfillWindows is how many closed windows per resolution a snapshot re-queries after
// metrics were not delivered (a failure, a stall or a restart). Kubecost only consumes the
// previous and the current window (its adapter keeps MaxBackfillWindows+1 snapshots and OpenCost
// exports the current window plus pending ones), so querying further back would be wasted work.
// Older closed windows are reported as gaps.
const MaxBackfillWindows = 1

// windowCommit is what a snapshot's metrics cover at one resolution: every closed window
// before current has been queried (after it closed) or falls in the gap [gapStart, gapEnd).
type windowCommit struct {
	resolution time.Duration
	// current is the start of the current window, and the watermark once delivered.
	current time.Time
	// gapStart and gapEnd bound the closed windows older than the backfill limit that were not
	// queried; they are equal when there is no gap.
	gapStart, gapEnd time.Time
}

// planWindows returns the windows to query at resolution: up to MaxBackfillWindows closed windows
// since watermark (the end of the last delivered closed window) plus the current window. A zero
// watermark means nothing was delivered yet, and only the current window is queried.
func planWindows(now, watermark time.Time, resolution time.Duration) ([]opencost.Window, windowCommit) {
	current := now.Truncate(resolution)
	plan := windowCommit{resolution: resolution, current: current}

	var windows []opencost.Window
	if !watermark.IsZero() && watermark.Before(current) {
		first := watermark.Truncate(resolution)
		if limit := current.Add(-MaxBackfillWindows * resolution); first.Before(limit) {
			plan.gapStart, plan.gapEnd = first, limit
			first = limit
		}
		for start := first; start.Before(current); start = start.Add(resolution) {
			windows = append(windows, opencost.NewClosedWindow(start, start.Add(resolution)))
		}
	}
	windows = append(windows, opencost.NewClosedWindow(current, current.Add(resolution)))
	return windows, plan
}

// watermarkState is the persisted form of the window watermarks.
type watermarkState struct {
	Version int `json:"version"`
	// Watermarks maps a resolution (time.Duration string) to the end of its last delivered
	// closed window.
	Watermarks map[string]time.Time `json:"watermarks"`
}

const watermarkStateVersion = 1

// CommitWindows implements WindowCommitter. It advances each resolution's watermark to the
// snapshot's current window, counts and logs the closed windows that were never queried as gaps,
// and persists the watermarks. Commits never move a watermark backwards, so a late snapshot or a
// repeated commit is harmless.
func (csp *ConcurrentSnapshotProvider) CommitWindows(snapshot *ClusterSnapshot) {
	if snapshot == nil || len(snapshot.windows) == 0 {
		return
	}

	changed := false
	csp.mu.Lock()
	for _, wc := range snapshot.windows {
		wm := csp.watermarks[wc.resolution]
		if !wc.current.After(wm) {
			continue
		}
		// A gap already covered by an earlier commit isn't reported twice.
		gapStart := wc.gapStart
		if wm.After(gapStart) {
			gapStart = wm
		}
		if wc.gapEnd.After(gapStart) {
			n := uint64(wc.gapEnd.Sub(gapStart) / wc.resolution)
			csp.windowGaps[wc.resolution] += n
			log.Errorf("metrics windows at %s resolution from %s to %s (%d windows) were never delivered and are older than the backfill limit; they are lost",
				wc.resolution, gapStart.Format(time.RFC3339), wc.gapEnd.Format(time.RFC3339), n)
		}
		csp.watermarks[wc.resolution] = wc.current
		changed = true
	}
	csp.mu.Unlock()

	if changed {
		csp.persistWatermarks()
	}
}

// WindowGapsTotal returns the number of closed windows at resolution reported lost over the life
// of the provider (window_gaps_total{resolution}).
func (csp *ConcurrentSnapshotProvider) WindowGapsTotal(resolution time.Duration) uint64 {
	csp.mu.Lock()
	defer csp.mu.Unlock()
	return csp.windowGaps[resolution]
}

// Watermarks returns, per resolution, the end of the last delivered closed window.
func (csp *ConcurrentSnapshotProvider) Watermarks() map[time.Duration]time.Time {
	csp.mu.Lock()
	defer csp.mu.Unlock()
	return maps.Clone(csp.watermarks)
}

// WatermarkPersistError returns the error of the last attempt to persist the watermarks, or nil.
func (csp *ConcurrentSnapshotProvider) WatermarkPersistError() error {
	csp.persistMu.Lock()
	defer csp.persistMu.Unlock()
	return csp.persistErr
}

// persistWatermarks writes the current watermarks to the watermark file, atomically. Failures
// are logged once per episode; the watermarks stay in memory either way.
func (csp *ConcurrentSnapshotProvider) persistWatermarks() {
	path := csp.config.WatermarkFile
	if path == "" {
		return
	}

	// Serialised, and reading the watermarks inside the lock, so the last write is the newest.
	csp.persistMu.Lock()
	defer csp.persistMu.Unlock()

	state := watermarkState{Version: watermarkStateVersion, Watermarks: map[string]time.Time{}}
	csp.mu.Lock()
	for res, wm := range csp.watermarks {
		state.Watermarks[res.String()] = wm
	}
	csp.mu.Unlock()

	err := writeFileAtomic(path, state)
	if err != nil && csp.persistErr == nil {
		log.Errorf("failed to persist metrics window watermarks to %s; a restart won't know which windows were delivered: %v", path, err)
	} else if err == nil && csp.persistErr != nil {
		log.Infof("persisting metrics window watermarks to %s again", path)
	}
	csp.persistErr = err
}

// writeFileAtomic writes v as JSON to a temporary file next to path, syncs it and renames it over
// path, then syncs the directory.
func writeFileAtomic(path string, v any) (err error) {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(tmp.Name())
		}
	}()
	if _, err = tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err = d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}

// loadWatermarks reads the watermark file, if configured, and logs the windows that elapsed
// since the last run beyond the backfill limit; they are counted as gaps when the first
// snapshot's metrics are delivered.
func (csp *ConcurrentSnapshotProvider) loadWatermarks() {
	path := csp.config.WatermarkFile
	if path == "" {
		return
	}

	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		log.Infof("no metrics window watermarks at %s; starting with the current windows", path)
		return
	}
	var state watermarkState
	if err == nil {
		err = json.Unmarshal(data, &state)
	}
	if err == nil && state.Version != watermarkStateVersion {
		err = fmt.Errorf("unsupported version %d", state.Version)
	}
	if err != nil {
		log.Errorf("failed to read metrics window watermarks from %s; windows missed before this start can't be detected: %v", path, err)
		return
	}

	now := csp.now()
	for key, wm := range state.Watermarks {
		res, err := time.ParseDuration(key)
		if err != nil || res <= 0 {
			log.Warnf("ignoring metrics window watermark with invalid resolution %q in %s", key, path)
			continue
		}
		csp.watermarks[res] = wm
		if _, plan := planWindows(now, wm, res); plan.gapEnd.After(plan.gapStart) {
			log.Warnf("metrics windows at %s resolution from %s to %s elapsed while the agent was down and are older than the backfill limit; they will be reported as a gap",
				res, plan.gapStart.Format(time.RFC3339), plan.gapEnd.Format(time.RFC3339))
		}
	}
	log.Infof("loaded metrics window watermarks from %s", path)
}
