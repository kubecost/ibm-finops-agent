package kubecost

import (
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/exporter/pathing"
	"github.com/opencost/opencost/core/pkg/heartbeat"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/core/pkg/util/atomic"
)

// EventStorageCleaner periodically removes expired heartbeat and diagnostics
// objects from federated storage for a single cluster prefix.
type EventStorageCleaner struct {
	store                storage.Storage
	appName              string
	clusterName          string
	heartbeatRetention   time.Duration
	diagnosticsRetention time.Duration
	runState             atomic.AtomicRunState
}

// NewEventStorageCleaner creates a cleaner scoped to the agent's own
// app/cluster heartbeat and diagnostics prefixes.
func NewEventStorageCleaner(
	store storage.Storage,
	appName string,
	clusterName string,
	heartbeatRetention time.Duration,
	diagnosticsRetention time.Duration,
) *EventStorageCleaner {
	return &EventStorageCleaner{
		store:                store,
		appName:              appName,
		clusterName:          clusterName,
		heartbeatRetention:   heartbeatRetention,
		diagnosticsRetention: diagnosticsRetention,
	}
}

// Enabled reports whether either retention window is configured.
func (c *EventStorageCleaner) Enabled() bool {
	return c.heartbeatRetention > 0 || c.diagnosticsRetention > 0
}

// Start begins periodic cleanup on the provided interval. Returns false if the
// cleaner is already running or no retention windows are configured.
func (c *EventStorageCleaner) Start(interval time.Duration) bool {
	if !c.Enabled() {
		return false
	}
	if interval <= 0 {
		log.Warnf("EventStorageCleaner: invalid cleanup interval %s; cleanup will not start", interval)
		return false
	}

	c.runState.WaitForReset()
	if !c.runState.Start() {
		return false
	}

	go func() {
		// Run once immediately so upgrades begin reclaiming space without waiting
		// for the first interval to elapse.
		c.Cleanup()

		for {
			select {
			case <-c.runState.OnStop():
				c.runState.Reset()
				return
			case <-time.After(interval):
				c.Cleanup()
			}
		}
	}()

	return true
}

// Stop halts the cleanup loop.
func (c *EventStorageCleaner) Stop() {
	c.runState.Stop()
}

// Cleanup deletes expired heartbeat and diagnostics objects for this cluster.
// Individual delete failures are logged and do not stop processing.
func (c *EventStorageCleaner) Cleanup() {
	now := time.Now().UTC()

	if c.heartbeatRetention > 0 {
		dir := path.Join(c.appName, c.clusterName, heartbeat.HeartbeatEventName)
		deleted, skipped, errCount := cleanupExpiredObjects(c.store, dir, now.Add(-c.heartbeatRetention))
		logCleanupSummary(heartbeat.HeartbeatEventName, dir, deleted, skipped, errCount)
	}

	if c.diagnosticsRetention > 0 {
		dir := path.Join(c.appName, c.clusterName, diagnostics.DiagnosticsEventName)
		deleted, skipped, errCount := cleanupExpiredObjects(c.store, dir, now.Add(-c.diagnosticsRetention))
		logCleanupSummary(diagnostics.DiagnosticsEventName, dir, deleted, skipped, errCount)
	}
}

func logCleanupSummary(valueType, dir string, deleted, skipped, errCount int) {
	if deleted == 0 && errCount == 0 {
		log.Debugf("EventStorageCleaner: %s cleanup complete for %s (deleted=0, skipped=%d)", valueType, dir, skipped)
		return
	}
	log.Infof("EventStorageCleaner: %s cleanup complete for %s (deleted=%d, skipped=%d, errors=%d)", valueType, dir, deleted, skipped, errCount)
}

// cleanupExpiredObjects lists objects under dir and removes those whose age is
// older than cutoff. Age prefers the event timestamp encoded in the filename
// (YYYYMMDDHHmmss.json); ModTime is used when the filename cannot be parsed.
func cleanupExpiredObjects(store storage.Storage, dir string, cutoff time.Time) (deleted, skipped, errCount int) {
	files, err := store.List(dir)
	if err != nil {
		log.Errorf("EventStorageCleaner: failed to list %s: %v", dir, err)
		return 0, 0, 1
	}

	for _, file := range files {
		if file == nil || file.Name == "" || strings.HasSuffix(file.Name, "/") {
			skipped++
			continue
		}

		objectAge, ok := objectAge(file)
		if !ok {
			log.Debugf("EventStorageCleaner: skipping unparseable object %s/%s", dir, file.Name)
			skipped++
			continue
		}

		if !objectAge.Before(cutoff) {
			skipped++
			continue
		}

		objectPath := path.Join(dir, file.Name)
		if err := store.Remove(objectPath); err != nil {
			log.Errorf("EventStorageCleaner: failed to delete %s: %v", objectPath, err)
			errCount++
			continue
		}
		deleted++
	}

	return deleted, skipped, errCount
}

func objectAge(file *storage.StorageInfo) (time.Time, bool) {
	if ts, err := parseEventFilenameTimestamp(file.Name); err == nil {
		return ts, true
	}
	if !file.ModTime.IsZero() {
		return file.ModTime.UTC(), true
	}
	return time.Time{}, false
}

func parseEventFilenameTimestamp(name string) (time.Time, error) {
	base := path.Base(name)
	// Expected: YYYYMMDDHHmmss.json or optionally prefix.YYYYMMDDHHmmss.json
	parts := strings.Split(base, ".")
	if len(parts) < 2 {
		return time.Time{}, fmt.Errorf("unexpected filename format: %s", name)
	}

	// Prefer the segment immediately before the extension when present.
	timestampPart := parts[0]
	if len(parts) >= 2 {
		timestampPart = parts[len(parts)-2]
	}

	ts, err := time.Parse(pathing.EventStorageTimeFormat, timestampPart)
	if err != nil {
		return time.Time{}, err
	}
	return ts.UTC(), nil
}
