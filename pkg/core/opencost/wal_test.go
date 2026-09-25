package opencost

// F-41, F-20/F-34 (docs/reliability/FINDINGS.md): the collector's write-ahead log is the only
// thing that stops a restart from overwriting in-progress Kubecost windows with partial data.
// It used to be dropped silently when the bucket store couldn't be built at startup, and a
// restore that failed was log-only.

import (
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ibm/finops-agent/pkg/condition"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/modules/collector-source/pkg/collector"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
)

const walBucketConfigFile = "/var/configs/federated-store.yaml"

// faultyStore is an in-memory bucket whose List, Read and Write can be made to fail.
type faultyStore struct {
	*storage.MemoryStorage
	failList, failRead, failWrite atomic.Bool
}

func newFaultyStore() *faultyStore { return &faultyStore{MemoryStorage: storage.NewMemoryStorage()} }

func (s *faultyStore) List(path string) ([]*storage.StorageInfo, error) {
	if s.failList.Load() {
		return nil, errors.New("list: 403 access denied")
	}
	return s.MemoryStorage.List(path)
}

func (s *faultyStore) Read(path string) ([]byte, error) {
	if s.failRead.Load() {
		return nil, errors.New("read: 403 access denied")
	}
	return s.MemoryStorage.Read(path)
}

func (s *faultyStore) Write(path string, data []byte) error {
	if s.failWrite.Load() {
		return errors.New("write: 403 access denied")
	}
	return s.MemoryStorage.Write(path, data)
}

// walinatorOn does what collector.NewCollectorDataSource does with the store it's given: build
// a Walinator over a metric repository and Start it, which replays the WAL synchronously.
func walinatorOn(t *testing.T, store storage.Storage) *metric.Walinator {
	t.Helper()
	res, err := util.NewResolution(util.ResolutionConfiguration{Interval: "1h", Retention: 3})
	if err != nil {
		t.Fatal(err)
	}
	resolutions := []*util.Resolution{res}
	repo := metric.NewMetricRepository(resolutions, collector.NewOpenCostMetricStore)
	w, err := metric.NewWalinator("wal-cluster", "wal-app", store, resolutions, repo)
	if err != nil {
		t.Fatal(err)
	}
	w.Start()
	return w
}

// testWALOptions retries fast. readFile fails until readable is set.
func testWALOptions(readable *atomic.Bool, store storage.Storage) walOptions {
	return walOptions{
		readFile: func(string) ([]byte, error) {
			if !readable.Load() {
				return nil, errors.New("open /var/configs/federated-store.yaml: no such file or directory")
			}
			return []byte("type: S3"), nil
		},
		newStorage:           func([]byte) (storage.Storage, error) { return store, nil },
		startupBudget:        2 * time.Second,
		initialBackoff:       5 * time.Millisecond,
		maxBackoff:           20 * time.Millisecond,
		backgroundMaxBackoff: 20 * time.Millisecond,
	}
}

func conditionTypes(cs []condition.Condition) []string {
	var types []string
	for _, c := range cs {
		types = append(types, c.Type+"/"+c.Reason)
	}
	return types
}

func waitFor(cond func() bool) bool {
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if cond() {
			return true
		}
	}
	return cond()
}

// F-41: a bucket config that is unreadable at start and readable a moment later still gets the
// collector a WAL, because the store is retried before the collector is built.
func TestWALStoreRetriedUntilBucketConfigReadable(t *testing.T) {
	var readable atomic.Bool
	opts := testWALOptions(&readable, newFaultyStore())
	time.AfterFunc(50*time.Millisecond, func() { readable.Store(true) })

	var got storage.Storage
	wal := openWAL(walBucketConfigFile, opts, func(store storage.Storage) {
		got = store
		if store != nil {
			walinatorOn(t, store)
		}
	})
	defer wal.Close()

	if got == nil {
		t.Fatal("F-41: the collector was built without a WAL although the bucket config became readable 50ms into startup")
	}
	if cs := wal.Conditions(); len(cs) != 0 {
		t.Errorf("unexpected WAL conditions: %v", conditionTypes(cs))
	}
}

// F-41: if the store still can't be built when the startup budget runs out, the collector starts
// without a WAL and wal_unavailable says so. Retries continue, and once the bucket is back the
// condition says a restart is needed to attach the WAL.
func TestWALUnavailableWhenStoreCannotBeBuilt(t *testing.T) {
	var readable atomic.Bool
	opts := testWALOptions(&readable, newFaultyStore())
	opts.startupBudget = 50 * time.Millisecond

	var got storage.Storage
	var constructed bool
	wal := openWAL(walBucketConfigFile, opts, func(store storage.Storage) { got, constructed = store, true })
	defer wal.Close()

	if !constructed || got != nil {
		t.Fatalf("the collector should start without a WAL once the startup budget is spent (constructed=%v)", constructed)
	}
	if !condition.Has(wal.Conditions(), ConditionWALUnavailable) {
		t.Fatalf("F-41: collector running without its WAL and no %s condition: %v", ConditionWALUnavailable, conditionTypes(wal.Conditions()))
	}

	readable.Store(true)
	restartRequired := func() bool {
		i := slices.IndexFunc(wal.Conditions(), func(c condition.Condition) bool { return c.Type == ConditionWALUnavailable })
		return i >= 0 && wal.Conditions()[i].Reason == "restart_required"
	}
	if !waitFor(restartRequired) {
		t.Errorf("bucket reachable again but %s doesn't say a restart is required: %v", ConditionWALUnavailable, conditionTypes(wal.Conditions()))
	}
}

// F-20/F-34: a WAL whose restore can't list the bucket starts from an empty repository. The first
// export then overwrites the day's windows with post-restart data; wal_restore_failed reports it.
func TestWALRestoreListFailureRaisesCondition(t *testing.T) {
	var readable atomic.Bool
	readable.Store(true)
	store := newFaultyStore()
	store.failList.Store(true)

	wal := openWAL(walBucketConfigFile, testWALOptions(&readable, store), func(s storage.Storage) { walinatorOn(t, s) })
	defer wal.Close()

	if !condition.Has(wal.Conditions(), ConditionWALRestoreFailed) {
		t.Errorf("F-34: WAL restore couldn't list the bucket and no %s condition was raised: %v", ConditionWALRestoreFailed, conditionTypes(wal.Conditions()))
	}
}

// F-20/F-34: a WAL object that can't be read is skipped by the restore, leaving a hole.
func TestWALRestoreReadFailureRaisesCondition(t *testing.T) {
	store := newFaultyStore()
	w := walinatorOn(t, store)
	w.Update(&metric.UpdateSet{Timestamp: time.Now().UTC(), Updates: []metric.Update{{Name: "node_total_hourly_cost", Value: 1}}})
	if files, _ := store.MemoryStorage.List("wal-app/wal-cluster/collector"); len(files) == 0 {
		t.Fatal("setup: expected a WAL object in the bucket")
	}

	var readable atomic.Bool
	readable.Store(true)
	store.failRead.Store(true)
	wal := openWAL(walBucketConfigFile, testWALOptions(&readable, store), func(s storage.Storage) { walinatorOn(t, s) })
	defer wal.Close()

	if !condition.Has(wal.Conditions(), ConditionWALRestoreFailed) {
		t.Errorf("F-34: WAL objects couldn't be read during restore and no %s condition was raised: %v", ConditionWALRestoreFailed, conditionTypes(wal.Conditions()))
	}
}

// Control: a clean restore raises nothing.
func TestWALRestoreSucceeds(t *testing.T) {
	store := newFaultyStore()
	w := walinatorOn(t, store)
	w.Update(&metric.UpdateSet{Timestamp: time.Now().UTC(), Updates: []metric.Update{{Name: "node_total_hourly_cost", Value: 1}}})

	var readable atomic.Bool
	readable.Store(true)
	wal := openWAL(walBucketConfigFile, testWALOptions(&readable, store), func(s storage.Storage) { walinatorOn(t, s) })
	defer wal.Close()

	if cs := wal.Conditions(); len(cs) != 0 {
		t.Errorf("unexpected WAL conditions after a clean restore: %v", conditionTypes(cs))
	}
}

// F-34: WAL writes that fail after startup are lost at the next restart; wal_write_failing
// reports them while they last.
func TestWALWriteFailuresRaiseAndClearCondition(t *testing.T) {
	var readable atomic.Bool
	readable.Store(true)
	store := newFaultyStore()
	var w *metric.Walinator
	var mu sync.Mutex
	wal := openWAL(walBucketConfigFile, testWALOptions(&readable, store), func(s storage.Storage) {
		mu.Lock()
		defer mu.Unlock()
		w = walinatorOn(t, s)
	})
	defer wal.Close()

	update := func() {
		w.Update(&metric.UpdateSet{Timestamp: time.Now().UTC(), Updates: []metric.Update{{Name: "node_total_hourly_cost", Value: 1}}})
	}
	store.failWrite.Store(true)
	update()
	if !condition.Has(wal.Conditions(), ConditionWALWriteFailing) {
		t.Errorf("F-34: a WAL write failed and no %s condition was raised: %v", ConditionWALWriteFailing, conditionTypes(wal.Conditions()))
	}
	store.failWrite.Store(false)
	time.Sleep(1100 * time.Millisecond) // WAL objects are named by the second
	update()
	if condition.Has(wal.Conditions(), ConditionWALWriteFailing) {
		t.Errorf("%s still raised after a successful WAL write", ConditionWALWriteFailing)
	}
}

// If the collector doesn't start the WAL it was given (NewWalinator failed, log-only), the WAL is
// as absent as in F-41.
func TestWALNotStartedRaisesUnavailable(t *testing.T) {
	var readable atomic.Bool
	readable.Store(true)
	wal := openWAL(walBucketConfigFile, testWALOptions(&readable, newFaultyStore()), func(storage.Storage) {})
	defer wal.Close()

	if !condition.Has(wal.Conditions(), ConditionWALUnavailable) {
		t.Errorf("collector ignored its WAL store and no %s condition was raised: %v", ConditionWALUnavailable, conditionTypes(wal.Conditions()))
	}
}
