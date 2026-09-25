package kubecost

// Kubecost export health (docs/reliability/FINDINGS.md chunk 07): the WAL gate (F-41), the bucket
// canary, and Stop.

import (
	"context"
	"errors"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ibm/finops-agent/pkg/condition"
	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/opencost/opencost/core/pkg/storage"
)

// fakeWAL is a WALGuard whose conditions the test sets.
type fakeWAL struct{ set *condition.Set }

func (w *fakeWAL) Conditions() []condition.Condition { return w.set.List() }

// healthStore is an in-memory bucket whose operations can fail or be slowed down.
type healthStore struct {
	*storage.MemoryStorage
	fail                      atomic.Bool
	writeDelay                atomic.Int64 // nanoseconds
	writesStarted, writesDone atomic.Int32
}

func newHealthStore() *healthStore { return &healthStore{MemoryStorage: storage.NewMemoryStorage()} }

var errDenied = errors.New("403 AccessDenied: https://bucket.example.com/x?X-Amz-Signature=secret")

func (s *healthStore) Write(path string, data []byte) error {
	s.writesStarted.Add(1)
	defer s.writesDone.Add(1)
	time.Sleep(time.Duration(s.writeDelay.Load()))
	if s.fail.Load() {
		return errDenied
	}
	return s.MemoryStorage.Write(path, data)
}

func (s *healthStore) Read(path string) ([]byte, error) {
	if s.fail.Load() {
		return nil, errDenied
	}
	return s.MemoryStorage.Read(path)
}

func (s *healthStore) Remove(path string) error {
	if s.fail.Load() {
		return errDenied
	}
	return s.MemoryStorage.Remove(path)
}

func healthSnapshot(t *testing.T) *emitter.ClusterSnapshot {
	t.Helper()
	_, _, snap := reproSnapshot(t)
	return snap
}

// newHealthEmitter returns an emitter with a readable bucket config backed by store.
func newHealthEmitter(t *testing.T, store storage.Storage) *KubecostEmitter {
	t.Helper()
	ke, bucketFile := newLifecycleEmitter(t)
	if err := os.WriteFile(bucketFile, []byte(lifecycleBucketConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	ke.newBucketStorage = func([]byte) (storage.Storage, error) { return store, nil }
	return ke
}

func waitUntil(d time.Duration, cond func() bool) bool {
	for deadline := time.Now().Add(d); time.Now().Before(deadline); time.Sleep(2 * time.Millisecond) {
		if cond() {
			return true
		}
	}
	return cond()
}

// F-41: while the collector runs without its WAL, Init starts nothing and reports
// wal_unavailable; once the WAL is there, Init succeeds, so the WAL is present before the first
// Kubecost export.
func TestKubecostInitWaitsForWAL(t *testing.T) {
	ke := newHealthEmitter(t, newHealthStore())
	wal := &fakeWAL{set: condition.NewSet("test-wal")}
	wal.set.Raise("wal_unavailable", "store_unavailable", "bucket config unreadable")
	ke.config.WAL = wal
	snap := healthSnapshot(t)

	runtime.GC()
	before := runtime.NumGoroutine()
	if err := ke.Init(snap); err == nil {
		t.Fatal("F-41: Init started the export controllers while the collector has no WAL")
	}
	time.Sleep(20 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before {
		t.Errorf("a WAL-gated Init started %d goroutines", after-before)
	}
	if ke.dataSource != nil || ke.pipelineControllers != nil {
		t.Error("a WAL-gated Init left emitter state behind")
	}
	if !condition.Has(ke.Conditions(), "wal_unavailable") {
		t.Errorf("Kubecost conditions don't include wal_unavailable: %v", ke.Conditions())
	}

	wal.set.Clear("wal_unavailable")
	if err := ke.Init(snap); err != nil {
		t.Fatalf("Init failed after the WAL became available: %v", err)
	}
	defer ke.Stop(context.Background())
}

// The canary sets bucket_unavailable within one interval of the bucket failing (e.g. revoked
// credentials) and clears it within one interval of the bucket recovering.
func TestBucketCanaryRaisesAndClears(t *testing.T) {
	const interval = 50 * time.Millisecond
	store := newHealthStore()
	ke := newHealthEmitter(t, store)
	ke.config.BucketCanaryInterval = interval
	if err := ke.Init(healthSnapshot(t)); err != nil {
		t.Fatal(err)
	}
	defer ke.Stop(context.Background())

	time.Sleep(interval)
	if condition.Has(ke.Conditions(), "bucket_unavailable") {
		t.Fatalf("bucket_unavailable raised on a healthy bucket: %v", ke.Conditions())
	}

	store.fail.Store(true)
	start := time.Now()
	if !waitUntil(time.Second, func() bool { return condition.Has(ke.Conditions(), "bucket_unavailable") }) {
		t.Fatal("bucket_unavailable not raised within 1s of the bucket denying access")
	}
	if took := time.Since(start); took > 2*interval {
		t.Errorf("bucket_unavailable took %s to raise; want within one %s interval (plus scheduling)", took, interval)
	}
	for _, c := range ke.Conditions() {
		if c.Type == "bucket_unavailable" && (c.Message == "" || strings.Contains(c.Message, "secret")) {
			t.Errorf("bucket_unavailable message is empty or leaks a signature: %q", c.Message)
		}
	}

	store.fail.Store(false)
	if !waitUntil(2*interval+50*time.Millisecond, func() bool { return !condition.Has(ke.Conditions(), "bucket_unavailable") }) {
		t.Error("bucket_unavailable not cleared within one interval of the bucket recovering")
	}
}

// Stop stops every controller and waits for a write already in flight, however slow, within its
// bound; no write starts after it returns.
func TestStopWaitsForInFlightWrites(t *testing.T) {
	store := newHealthStore()
	store.writeDelay.Store(int64(300 * time.Millisecond))
	ke := newHealthEmitter(t, store)
	ke.config.HeartbeatExportEnabled = true
	ke.config.ExportIntervals.HeartbeatInterval = 10 * time.Millisecond
	if err := ke.Init(healthSnapshot(t)); err != nil {
		t.Fatal(err)
	}
	if !waitUntil(2*time.Second, func() bool { return store.writesStarted.Load() > 0 }) {
		t.Fatal("setup: the heartbeat controller never wrote")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ke.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if started, done := store.writesStarted.Load(), store.writesDone.Load(); done != started {
		t.Fatalf("Stop returned with %d of %d bucket writes still in flight", started-done, started)
	}
	startedAtStop := store.writesStarted.Load()
	time.Sleep(100 * time.Millisecond)
	if got := store.writesStarted.Load(); got != startedAtStop {
		t.Errorf("%d bucket writes started after Stop returned", got-startedAtStop)
	}
}

// Stop's wait is bounded by its context: a write that hangs doesn't hang shutdown.
func TestStopIsBounded(t *testing.T) {
	store := newHealthStore()
	store.writeDelay.Store(int64(3 * time.Second))
	ke := newHealthEmitter(t, store)
	ke.config.HeartbeatExportEnabled = true
	ke.config.ExportIntervals.HeartbeatInterval = 10 * time.Millisecond
	if err := ke.Init(healthSnapshot(t)); err != nil {
		t.Fatal(err)
	}
	if !waitUntil(2*time.Second, func() bool { return store.writesStarted.Load() > 0 }) {
		t.Fatal("setup: the heartbeat controller never wrote")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := ke.Stop(ctx)
	if took := time.Since(start); took > time.Second {
		t.Errorf("Stop took %s with a 100ms bound", took)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Stop with a hung write returned %v; want a deadline error", err)
	}
}

// A computation that finished just before Stop still gets its window written: OpenCost exports a
// closed window only once, so refusing that write would lose it. Writes are refused only after
// Stop's bound expires, and new computations as soon as Stop starts.
func TestDrainKeepsWritesOpenUntilBound(t *testing.T) {
	activity := &exportActivity{}
	store := &exportStore{Storage: storage.NewMemoryStorage(), activity: activity}

	if err := activity.drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if activity.begin(false) {
		t.Error("a computation started after Stop")
	}
	if err := store.Write("w/late", []byte("x")); err != nil {
		t.Errorf("a write after a clean drain was refused: %v", err)
	}

	activity.begin(true) // a write that never finishes
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := activity.drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drain with a hung write returned %v", err)
	}
	if err := store.Write("w/after-bound", []byte("x")); !errors.Is(err, errStopped) {
		t.Errorf("a write after Stop's bound expired returned %v; want errStopped", err)
	}
	if got := activity.rejectedAfterStopTotal.Load(); got != 1 {
		t.Errorf("rejectedAfterStopTotal = %d; want 1", got)
	}
}
