package opencost

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ibm/finops-agent/pkg/condition"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/storage"
)

// Conditions raised by the collector's write-ahead log. Chunk 08 folds them into readiness.
const (
	// ConditionWALUnavailable: a bucket is configured but the collector runs without a WAL.
	// Kubecost export controllers don't start while it is raised.
	ConditionWALUnavailable = "wal_unavailable"
	// ConditionWALRestoreFailed: the WAL could not be fully replayed at startup.
	ConditionWALRestoreFailed = "wal_restore_failed"
	// ConditionWALWriteFailing: the last write to the WAL failed.
	ConditionWALWriteFailing = "wal_write_failing"
)

// Reasons for ConditionWALUnavailable.
const (
	// walReasonStoreUnavailable: the bucket store still can't be built; retries continue.
	walReasonStoreUnavailable = "store_unavailable"
	// walReasonRestartRequired: the bucket store can be built now, but the collector already
	// started without a WAL and it can't be attached to a running collector.
	walReasonRestartRequired = "restart_required"
	// walReasonNotStarted: the store was built but the collector didn't start the WAL.
	walReasonNotStarted = "not_started"
)

// WAL reports the state of the collector's write-ahead log (docs/reliability/FINDINGS.md F-20,
// F-34, F-41). The WAL is what stops a restart from overwriting Kubecost's in-progress export
// windows with post-restart data only.
type WAL struct {
	configured bool
	conditions *condition.Set

	restoring                   atomic.Bool
	restoreLists                atomic.Uint64
	restoreErrors               atomic.Uint64
	writesTotal, writeFailures  atomic.Uint64
	storeAttempts, storeFailure atomic.Uint64

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// WALStatus is a snapshot of the WAL's counters. The counters only ever increase.
type WALStatus struct {
	Configured bool
	// StoreAttemptsTotal and StoreFailuresTotal count attempts to build the WAL's bucket store.
	StoreAttemptsTotal, StoreFailuresTotal uint64
	// RestoreErrorsTotal counts bucket List and Read failures while the WAL was replayed.
	RestoreErrorsTotal uint64
	// WritesTotal and WriteFailuresTotal count WAL writes (one per scrape) and their failures.
	WritesTotal, WriteFailuresTotal uint64
}

// Configured reports whether a bucket is configured, so the collector is meant to run with a WAL.
func (w *WAL) Configured() bool { return w.configured }

// Conditions returns the WAL's active conditions.
func (w *WAL) Conditions() []condition.Condition { return w.conditions.List() }

// Status returns the WAL's counters.
func (w *WAL) Status() WALStatus {
	return WALStatus{
		Configured:         w.configured,
		StoreAttemptsTotal: w.storeAttempts.Load(),
		StoreFailuresTotal: w.storeFailure.Load(),
		RestoreErrorsTotal: w.restoreErrors.Load(),
		WritesTotal:        w.writesTotal.Load(),
		WriteFailuresTotal: w.writeFailures.Load(),
	}
}

// Close stops the background store retries, if any, and waits for them.
func (w *WAL) Close() {
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()
}

// walOptions are openWAL's dependencies and retry budget.
type walOptions struct {
	readFile   func(string) ([]byte, error)
	newStorage func([]byte) (storage.Storage, error)
	// startupBudget bounds the retries before the collector is started.
	startupBudget time.Duration
	// initialBackoff and maxBackoff shape the startup retries; backgroundMaxBackoff caps the
	// retries after the startup budget is spent.
	initialBackoff, maxBackoff, backgroundMaxBackoff time.Duration
}

func defaultWALOptions() walOptions {
	return walOptions{
		readFile:             os.ReadFile,
		newStorage:           storage.NewBucketStorage,
		startupBudget:        2 * time.Minute,
		initialBackoff:       time.Second,
		maxBackoff:           15 * time.Second,
		backgroundMaxBackoff: 5 * time.Minute,
	}
}

// openWAL builds the WAL's bucket store from bucketConfigFile and calls construct, which builds
// the collector (and replays the WAL synchronously), with it. construct gets nil when no bucket
// is configured.
//
// Building the store is retried with backoff for up to opts.startupBudget. If it still fails,
// the collector is built without a WAL, so metrics keep flowing for everything else, and
// wal_unavailable is raised: the Kubecost emitter won't start its export controllers, since
// exporting without a WAL is what lets a restart overwrite windows with partial data (F-41).
// Retries continue in the background; the WAL can't be attached to a running collector, so
// success turns the condition's reason into restart_required.
func openWAL(bucketConfigFile string, opts walOptions, construct func(storage.Storage)) *WAL {
	wal := &WAL{configured: bucketConfigFile != "", conditions: condition.NewSet("collector-wal")}
	if !wal.configured {
		construct(nil)
		return wal
	}

	ctx, cancel := context.WithTimeout(context.Background(), opts.startupBudget)
	store, inFlight, err := wal.buildStore(ctx, bucketConfigFile, opts, opts.maxBackoff)
	cancel()
	if err != nil {
		log.Errorf("Collector starting WITHOUT a write-ahead log: its bucket store could not be built within %s: %s. Kubecost exports stay stopped until the agent restarts with a working bucket.", opts.startupBudget, err)
		wal.conditions.Raise(ConditionWALUnavailable, walReasonStoreUnavailable, err.Error())
		construct(nil)
		wal.retryInBackground(bucketConfigFile, opts, inFlight)
		return wal
	}

	wal.restoring.Store(true)
	construct(&walStore{Storage: store, wal: wal})
	wal.restoring.Store(false)

	// The Walinator lists the bucket as it starts. No List means the collector didn't create it
	// (NewWalinator failed, which is log-only) and runs without a WAL.
	if wal.restoreLists.Load() == 0 {
		log.Errorf("Collector started WITHOUT a write-ahead log although its bucket store was built; see the collector's walinator error. Kubecost exports stay stopped.")
		wal.conditions.Raise(ConditionWALUnavailable, walReasonNotStarted, "the collector did not start its write-ahead log")
		return wal
	}

	if n := wal.restoreErrors.Load(); n > 0 {
		msg := fmt.Sprintf("%d bucket list or read errors while replaying the WAL; in-progress windows restored incompletely will be overwritten with partial data by the next export", n)
		log.Errorf("Collector WAL restore failed: %s", msg)
		wal.conditions.Raise(ConditionWALRestoreFailed, "restore_errors", msg)
	}
	return wal
}

// attemptResult is the outcome of one attempt to build the store.
type attemptResult struct {
	store storage.Storage
	err   error
}

// attempt reads the bucket config and builds the store in its own goroutine, since
// NewBucketStorage takes no context and can block on the network (Azure, GCS).
func (w *WAL) attempt(file string, opts walOptions) <-chan attemptResult {
	w.storeAttempts.Add(1)
	ch := make(chan attemptResult, 1)
	go func() {
		data, err := opts.readFile(file)
		if err != nil {
			ch <- attemptResult{err: fmt.Errorf("reading bucket config: %w", err)}
			return
		}
		store, err := opts.newStorage(data)
		if err != nil {
			err = fmt.Errorf("creating bucket storage: %w", err)
		}
		ch <- attemptResult{store: store, err: err}
	}()
	return ch
}

// buildStore retries attempt with capped exponential backoff and full jitter until it succeeds
// or ctx ends. One attempt runs at a time; one still running when ctx ends is returned as
// inFlight so the caller can keep waiting for it instead of starting another.
func (w *WAL) buildStore(ctx context.Context, file string, opts walOptions, maxBackoff time.Duration) (store storage.Storage, inFlight <-chan attemptResult, err error) {
	backoff := opts.initialBackoff
	lastErr := errors.New("no attempt completed")
	for {
		ch := w.attempt(file, opts)
		select {
		case r := <-ch:
			if r.err == nil {
				return r.store, nil, nil
			}
			w.storeFailure.Add(1)
			lastErr = r.err
			log.Warnf("Collector WAL: %s; retrying", r.err)
		case <-ctx.Done():
			return nil, ch, fmt.Errorf("%w (attempt still running at the deadline)", lastErr)
		}

		timer := time.NewTimer(backoff/2 + rand.N(backoff/2+1))
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, nil, lastErr
		}
		backoff = min(2*backoff, maxBackoff)
	}
}

// retryInBackground keeps trying to build the store after the collector started without it,
// to tell operators when a restart would attach the WAL.
func (w *WAL) retryInBackground(file string, opts walOptions, inFlight <-chan attemptResult) {
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	w.wg.Go(func() {
		if inFlight != nil {
			select {
			case r := <-inFlight:
				if r.err == nil {
					w.restartRequired()
					return
				}
				w.storeFailure.Add(1)
			case <-ctx.Done():
				return
			}
		}
		if _, _, err := w.buildStore(ctx, file, opts, opts.backgroundMaxBackoff); err == nil {
			w.restartRequired()
		}
	})
}

func (w *WAL) restartRequired() {
	log.Errorf("Collector WAL: the bucket store can be built now, but the collector is running without a write-ahead log. Restart the agent to enable it; Kubecost exports stay stopped until then.")
	w.conditions.Raise(ConditionWALUnavailable, walReasonRestartRequired, "the bucket is reachable again, but the collector started without a write-ahead log; restart the agent to enable it")
}

// walStore is the WAL's view of the bucket. It observes the Walinator's calls: List and Read
// failures while the WAL is replayed at startup (the Walinator only logs them), and the outcome
// of every WAL write afterwards.
type walStore struct {
	storage.Storage
	wal *WAL
}

func (s *walStore) restoreError(err error) {
	if err != nil && s.wal.restoring.Load() {
		s.wal.restoreErrors.Add(1)
	}
}

func (s *walStore) List(path string) ([]*storage.StorageInfo, error) {
	if s.wal.restoring.Load() {
		s.wal.restoreLists.Add(1)
	}
	infos, err := s.Storage.List(path)
	s.restoreError(err)
	return infos, err
}

func (s *walStore) Read(path string) ([]byte, error) {
	data, err := s.Storage.Read(path)
	s.restoreError(err)
	return data, err
}

func (s *walStore) Write(path string, data []byte) error {
	err := s.Storage.Write(path, data)
	s.writeResult(err)
	return err
}

func (s *walStore) WriteStream(path string) (io.WriteCloser, error) {
	w, err := s.Storage.WriteStream(path)
	if err != nil {
		s.writeResult(err)
		return nil, err
	}
	return &walStreamWriter{WriteCloser: w, store: s}, nil
}

func (s *walStore) writeResult(err error) {
	s.wal.writesTotal.Add(1)
	if err != nil {
		s.wal.writeFailures.Add(1)
		s.wal.conditions.Raise(ConditionWALWriteFailing, "write_failed", "WAL write failed; scrapes since the last successful write will be missing after a restart: "+err.Error())
		return
	}
	s.wal.conditions.Clear(ConditionWALWriteFailing)
}

// walStreamWriter records a streamed WAL write's outcome when it is closed.
type walStreamWriter struct {
	io.WriteCloser
	store  *walStore
	failed bool
	once   sync.Once
}

func (w *walStreamWriter) Write(p []byte) (int, error) {
	n, err := w.WriteCloser.Write(p)
	if err != nil {
		w.failed = true
	}
	return n, err
}

func (w *walStreamWriter) Close() error {
	err := w.WriteCloser.Close()
	w.once.Do(func() {
		result := err
		if result == nil && w.failed {
			result = errors.New("stream write failed")
		}
		w.store.writeResult(result)
	})
	return err
}
