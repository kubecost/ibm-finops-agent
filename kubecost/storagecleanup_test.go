package kubecost

import (
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/exporter/pathing"
	"github.com/opencost/opencost/core/pkg/heartbeat"
	"github.com/opencost/opencost/core/pkg/storage"
)

var testFileContent = []byte(`{"test":true}`)

func writeEventFile(t *testing.T, store storage.Storage, baseDir, dir, fileName string, modTime time.Time) string {
	t.Helper()

	objectPath := path.Join(dir, fileName)
	if err := store.Write(objectPath, testFileContent); err != nil {
		t.Fatalf("failed to write %s: %v", objectPath, err)
	}

	fullPath := filepath.Join(baseDir, objectPath)
	if err := os.Chtimes(fullPath, modTime, modTime); err != nil {
		t.Fatalf("failed to set modtime for %s: %v", fullPath, err)
	}

	return objectPath
}

func TestEventStorageCleaner_Cleanup_DeletesExpiredObjects(t *testing.T) {
	baseDir := t.TempDir()
	store := storage.NewFileStorage(baseDir)

	appName := "finops-agent"
	clusterName := "cluster-1"
	retention := 7 * 24 * time.Hour
	now := time.Now().UTC()

	heartbeatDir := path.Join(appName, clusterName, heartbeat.HeartbeatEventName)
	diagnosticsDir := path.Join(appName, clusterName, diagnostics.DiagnosticsEventName)
	otherClusterDir := path.Join(appName, "other-cluster", heartbeat.HeartbeatEventName)

	oldHeartbeat := writeEventFile(t, store, baseDir, heartbeatDir, now.Add(-8*24*time.Hour).Format(pathing.EventStorageTimeFormat)+".json", now.Add(-8*24*time.Hour))
	recentHeartbeat := writeEventFile(t, store, baseDir, heartbeatDir, now.Add(-1*time.Hour).Format(pathing.EventStorageTimeFormat)+".json", now.Add(-1*time.Hour))
	oldDiagnostics := writeEventFile(t, store, baseDir, diagnosticsDir, now.Add(-10*24*time.Hour).Format(pathing.EventStorageTimeFormat)+".json", now.Add(-10*24*time.Hour))
	recentDiagnostics := writeEventFile(t, store, baseDir, diagnosticsDir, now.Add(-2*time.Hour).Format(pathing.EventStorageTimeFormat)+".json", now.Add(-2*time.Hour))
	otherClusterOld := writeEventFile(t, store, baseDir, otherClusterDir, now.Add(-30*24*time.Hour).Format(pathing.EventStorageTimeFormat)+".json", now.Add(-30*24*time.Hour))

	cleaner := NewEventStorageCleaner(store, appName, clusterName, retention, retention)
	cleaner.Cleanup()

	assertExists(t, store, oldHeartbeat, false)
	assertExists(t, store, recentHeartbeat, true)
	assertExists(t, store, oldDiagnostics, false)
	assertExists(t, store, recentDiagnostics, true)
	assertExists(t, store, otherClusterOld, true)
}

func TestEventStorageCleaner_Cleanup_RetentionZeroDisablesPrefix(t *testing.T) {
	baseDir := t.TempDir()
	store := storage.NewFileStorage(baseDir)

	appName := "finops-agent"
	clusterName := "cluster-1"
	now := time.Now().UTC()

	heartbeatDir := path.Join(appName, clusterName, heartbeat.HeartbeatEventName)
	diagnosticsDir := path.Join(appName, clusterName, diagnostics.DiagnosticsEventName)

	oldHeartbeat := writeEventFile(t, store, baseDir, heartbeatDir, now.Add(-30*24*time.Hour).Format(pathing.EventStorageTimeFormat)+".json", now.Add(-30*24*time.Hour))
	oldDiagnostics := writeEventFile(t, store, baseDir, diagnosticsDir, now.Add(-30*24*time.Hour).Format(pathing.EventStorageTimeFormat)+".json", now.Add(-30*24*time.Hour))

	cleaner := NewEventStorageCleaner(store, appName, clusterName, 0, 7*24*time.Hour)
	cleaner.Cleanup()

	assertExists(t, store, oldHeartbeat, true)
	assertExists(t, store, oldDiagnostics, false)
}

func TestEventStorageCleaner_Cleanup_FallsBackToModTime(t *testing.T) {
	baseDir := t.TempDir()
	store := storage.NewFileStorage(baseDir)

	appName := "finops-agent"
	clusterName := "cluster-1"
	retention := 7 * 24 * time.Hour
	now := time.Now().UTC()

	heartbeatDir := path.Join(appName, clusterName, heartbeat.HeartbeatEventName)
	oldUnparseable := writeEventFile(t, store, baseDir, heartbeatDir, "not-a-timestamp.json", now.Add(-10*24*time.Hour))
	recentUnparseable := writeEventFile(t, store, baseDir, heartbeatDir, "also-not-a-timestamp.json", now.Add(-1*time.Hour))

	cleaner := NewEventStorageCleaner(store, appName, clusterName, retention, 0)
	cleaner.Cleanup()

	assertExists(t, store, oldUnparseable, false)
	assertExists(t, store, recentUnparseable, true)
}

func TestParseEventFilenameTimestamp(t *testing.T) {
	ts, err := parseEventFilenameTimestamp("20251022153000.json")
	if err != nil {
		t.Fatalf("expected parse success, got %v", err)
	}
	expected := time.Date(2025, 10, 22, 15, 30, 0, 0, time.UTC)
	if !ts.Equal(expected) {
		t.Fatalf("expected %s, got %s", expected, ts)
	}

	ts, err = parseEventFilenameTimestamp("prefix.20251022153000.json")
	if err != nil {
		t.Fatalf("expected prefixed parse success, got %v", err)
	}
	if !ts.Equal(expected) {
		t.Fatalf("expected %s, got %s", expected, ts)
	}

	if _, err := parseEventFilenameTimestamp("badname"); err == nil {
		t.Fatal("expected parse failure for badname")
	}
}

func assertExists(t *testing.T, store storage.Storage, objectPath string, want bool) {
	t.Helper()

	exists, err := store.Exists(objectPath)
	if err != nil {
		t.Fatalf("Exists(%s) failed: %v", objectPath, err)
	}
	if exists != want {
		t.Fatalf("Exists(%s)=%v, want %v", objectPath, exists, want)
	}
}
