package kubecost

// Kubecost emitter lifecycle under the exporter supervisor (docs/reliability/FINDINGS.md F-10, I5).

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ibm/finops-agent/internal/mocks"
	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/opencost/opencost/core/pkg/diagnostics"
)

// lifecycleBucketConfig is a syntactically valid S3 bucket config. NewBucketStorage does not
// contact the endpoint, and the export controllers' first tick never fires within a test.
const lifecycleBucketConfig = `type: S3
config:
  bucket: lifecycle
  endpoint: 127.0.0.1:1
  access_key: lifecycle
  secret_key: lifecycle
  insecure: true
`

func newLifecycleEmitter(t *testing.T) (*KubecostEmitter, string) {
	t.Helper()
	bucketFile := filepath.Join(t.TempDir(), "federated-store.yaml") // written later by the test
	return NewKubecostEmitter(nil, diagnostics.NewDiagnosticService(), &EmitterConfig{
		ClusterUID:       "lifecycle-cluster",
		ClusterName:      "lifecycle",
		AppName:          "lifecycle",
		BucketConfigFile: bucketFile,
		ExportIntervals:  DefaultExportIntervalConfig(),
		QueryResolution:  time.Minute,
	}), bucketFile
}

// initCounter wraps an emitter and counts its Init calls and successes.
type initCounter struct {
	emitter.Emitter
	inits, successes atomic.Int32
}

func (c *initCounter) Init(s *emitter.ClusterSnapshot) error {
	c.inits.Add(1)
	err := c.Emitter.Init(s)
	if err == nil {
		c.successes.Add(1)
	}
	return err
}

// steadyEmitter stands in for the Cloudability emitter and counts its emissions.
type steadyEmitter struct{ emits atomic.Int32 }

func (e *steadyEmitter) ID() emitter.EmitterID               { return emitter.CldyEmitterID }
func (e *steadyEmitter) Init(*emitter.ClusterSnapshot) error { return nil }
func (e *steadyEmitter) Emit(context.Context, *emitter.ClusterSnapshot) error {
	e.emits.Add(1)
	return nil
}

// F-10 / I5: Kubecost Init fails while its bucket config is missing, is retried until the config
// appears, succeeds exactly once, and the Cloudability emitter emits throughout.
func TestKubecostInitRetriedWhileCloudabilityEmits(t *testing.T) {
	ke, bucketFile := newLifecycleEmitter(t)
	kc := &initCounter{Emitter: ke}
	cldy := &steadyEmitter{}
	ds := mocks.NewMockDataSource()
	provider := emitter.NewConcurrentSnapshotProvider(emitter.DefaultSnapshotConfig())

	exp := emitter.NewExporter(ds, provider, kc, cldy)
	if !exp.Start(20 * time.Millisecond) {
		t.Fatal("failed to start exporter")
	}
	defer exp.Stop()

	waitUntil := func(cond func() bool) bool {
		for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
			if cond() {
				return true
			}
		}
		return false
	}
	if !waitUntil(func() bool { return kc.inits.Load() >= 3 }) {
		t.Fatalf("F-10: Kubecost Init attempted %d times in 3s while its bucket config was missing; want retries every cycle", kc.inits.Load())
	}
	emitsWhileFailing := cldy.emits.Load()

	if err := os.WriteFile(bucketFile, []byte(lifecycleBucketConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	if !waitUntil(func() bool { return kc.successes.Load() == 1 }) {
		t.Fatalf("F-10: Kubecost emitter never initialised after its bucket config appeared (%d Init attempts)", kc.inits.Load())
	}
	initsAtSuccess := kc.inits.Load()
	time.Sleep(100 * time.Millisecond)
	exp.Stop()

	if emitsWhileFailing == 0 {
		t.Errorf("I5: Cloudability did not emit while Kubecost Init was failing")
	}
	if got := kc.inits.Load(); got != initsAtSuccess {
		t.Errorf("Kubecost Init called %d more times after it succeeded; a second Init would start duplicate controllers", got-initsAtSuccess)
	}
	if got := kc.successes.Load(); got != 1 {
		t.Errorf("Kubecost Init succeeded %d times; want exactly 1", got)
	}
}

// Retrying a failed Kubecost Init is safe only because Init fails before starting anything.
// A failed Init must leave no goroutines (export controllers) and no state behind.
func TestKubecostFailedInitStartsNothing(t *testing.T) {
	ke, _ := newLifecycleEmitter(t)
	ds := mocks.NewMockDataSource()
	snap, err := emitter.NewConcurrentSnapshotProvider(emitter.DefaultSnapshotConfig()).SnapshotOf(ds)
	if err != nil {
		t.Fatal(err)
	}

	// Warm up anything lazily started by the first call so it doesn't count as a leak.
	_ = ke.Init(snap)
	runtime.GC()
	before := runtime.NumGoroutine()
	for range 5 {
		if err := ke.Init(snap); err == nil {
			t.Fatal("Init unexpectedly succeeded without a bucket config file")
		}
	}
	time.Sleep(20 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before {
		t.Errorf("5 failed Inits started %d goroutines", after-before)
	}
	if ke.dataSource != nil || ke.pipelineControllers != nil || ke.heartbeatController != nil || ke.diagController != nil {
		t.Errorf("a failed Init left emitter state behind")
	}
}
