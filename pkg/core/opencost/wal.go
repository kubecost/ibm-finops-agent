package opencost

import (
	"os"
	"time"

	"github.com/ibm/finops-agent/pkg/condition"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/storage"
)

// Conditions raised by the collector's write-ahead log. Chunk 08 folds them into readiness.
const (
	// ConditionWALUnavailable: a bucket is configured but the collector runs without a WAL.
	ConditionWALUnavailable = "wal_unavailable"
	// ConditionWALRestoreFailed: the WAL could not be fully replayed at startup.
	ConditionWALRestoreFailed = "wal_restore_failed"
	// ConditionWALWriteFailing: the last write to the WAL failed.
	ConditionWALWriteFailing = "wal_write_failing"
)

// WAL reports the state of the collector's write-ahead log (docs/reliability/FINDINGS.md F-20,
// F-34, F-41).
type WAL struct {
	configured bool
	conditions *condition.Set
}

// Configured reports whether a bucket is configured, so the collector is meant to run with a WAL.
func (w *WAL) Configured() bool { return w.configured }

// Conditions returns the WAL's active conditions.
func (w *WAL) Conditions() []condition.Condition { return w.conditions.List() }

// Close stops any background work.
func (w *WAL) Close() {}

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
// the collector, with it. construct gets nil when no bucket is configured.
func openWAL(bucketConfigFile string, opts walOptions, construct func(storage.Storage)) *WAL {
	wal := &WAL{configured: bucketConfigFile != "", conditions: condition.NewSet("collector-wal")}

	var store storage.Storage
	if bucketConfigFile != "" {
		bucketConfig, err := opts.readFile(bucketConfigFile)
		if err != nil {
			log.Errorf("Failed to initialize bucket output storage, please check your configuration and bucket security settings: %s", err)
		} else {
			store, err = opts.newStorage(bucketConfig)
			if err != nil {
				log.Errorf("Failed to create bucket storage, please check your configuration and bucket security settings: %s", err)
			}
		}
	}

	construct(store)
	return wal
}
