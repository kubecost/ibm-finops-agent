package emitter

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSnapshotStatePersistAndRecover(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "snapshot-state-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a snapshot config with the temp directory
	config := &SnapshotConfig{
		ScratchDir: tempDir,
		Now:        defaultNow,
	}

	// Create first provider and set a timestamp
	provider1 := NewConcurrentSnapshotProvider(config).(*ConcurrentSnapshotProvider)
	testTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	provider1.lastSnapshot = testTime

	// Persist the state
	if err := provider1.PersistState(); err != nil {
		t.Fatalf("Failed to persist state: %v", err)
	}

	// Verify the state file was created
	stateFile := filepath.Join(tempDir, snapshotStateFilename)
	if _, err := os.Stat(stateFile); os.IsNotExist(err) {
		t.Fatalf("State file was not created")
	}

	// Create a new provider and verify it recovers the state
	provider2 := NewConcurrentSnapshotProvider(config).(*ConcurrentSnapshotProvider)
	if !provider2.lastSnapshot.Equal(testTime) {
		t.Errorf("Expected recovered timestamp %v, got %v", testTime, provider2.lastSnapshot)
	}
}

func TestSnapshotStateRecoverMissingFile(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "snapshot-state-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := &SnapshotConfig{
		ScratchDir: tempDir,
		Now:        defaultNow,
	}

	// Create provider without any existing state file
	provider := NewConcurrentSnapshotProvider(config).(*ConcurrentSnapshotProvider)

	// Verify lastSnapshot is zero (cold start)
	if !provider.lastSnapshot.IsZero() {
		t.Errorf("Expected zero timestamp on cold start, got %v", provider.lastSnapshot)
	}
}

func TestSnapshotStateRecoverCorruptFile(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "snapshot-state-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Write corrupt data to the state file
	stateFile := filepath.Join(tempDir, snapshotStateFilename)
	if err := os.WriteFile(stateFile, []byte("invalid json {{{"), 0644); err != nil {
		t.Fatalf("Failed to write corrupt state file: %v", err)
	}

	config := &SnapshotConfig{
		ScratchDir: tempDir,
		Now:        defaultNow,
	}

	// Create provider with corrupt state file
	provider := NewConcurrentSnapshotProvider(config).(*ConcurrentSnapshotProvider)

	// Verify lastSnapshot is zero (cold start due to corrupt file)
	if !provider.lastSnapshot.IsZero() {
		t.Errorf("Expected zero timestamp on corrupt file, got %v", provider.lastSnapshot)
	}
}

func TestSnapshotStatePersistWithoutScratchDir(t *testing.T) {
	config := &SnapshotConfig{
		ScratchDir: "", // No scratch directory configured
		Now:        defaultNow,
	}

	provider := NewConcurrentSnapshotProvider(config).(*ConcurrentSnapshotProvider)
	provider.lastSnapshot = time.Now()

	// Attempt to persist should return an error
	err := provider.PersistState()
	if err == nil {
		t.Error("Expected error when persisting without scratch directory, got nil")
	}
}

func TestSnapshotStateFileFormat(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "snapshot-state-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := &SnapshotConfig{
		ScratchDir: tempDir,
		Now:        defaultNow,
	}

	provider := NewConcurrentSnapshotProvider(config).(*ConcurrentSnapshotProvider)
	testTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	provider.lastSnapshot = testTime

	// Persist the state
	if err := provider.PersistState(); err != nil {
		t.Fatalf("Failed to persist state: %v", err)
	}

	// Read and verify the file format
	stateFile := filepath.Join(tempDir, snapshotStateFilename)
	data, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatalf("Failed to read state file: %v", err)
	}

	var state SnapshotState
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("Failed to unmarshal state file: %v", err)
	}

	if !state.LastSnapshot.Equal(testTime) {
		t.Errorf("Expected timestamp %v in file, got %v", testTime, state.LastSnapshot)
	}
}

func TestSnapshotStateMultiplePersists(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "snapshot-state-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := &SnapshotConfig{
		ScratchDir: tempDir,
		Now:        defaultNow,
	}

	provider := NewConcurrentSnapshotProvider(config).(*ConcurrentSnapshotProvider)

	// Persist multiple times with different timestamps
	time1 := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	provider.lastSnapshot = time1
	if err := provider.PersistState(); err != nil {
		t.Fatalf("Failed to persist state (first): %v", err)
	}

	time2 := time.Date(2024, 1, 15, 11, 0, 0, 0, time.UTC)
	provider.lastSnapshot = time2
	if err := provider.PersistState(); err != nil {
		t.Fatalf("Failed to persist state (second): %v", err)
	}

	// Create new provider and verify it has the latest timestamp
	provider2 := NewConcurrentSnapshotProvider(config).(*ConcurrentSnapshotProvider)
	if !provider2.lastSnapshot.Equal(time2) {
		t.Errorf("Expected recovered timestamp %v, got %v", time2, provider2.lastSnapshot)
	}
}
