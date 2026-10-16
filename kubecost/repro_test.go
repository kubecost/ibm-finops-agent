package kubecost

// F-10 reproduction (docs/reliability/FINDINGS.md): a failed Init used to leave the Kubecost
// emitter permanently uninitialised, with every Emit panicking. Fixed in chunk 05: Emit before
// Init returns an error, and the exporter retries Init until it succeeds.

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ibm/finops-agent/internal/mocks"
	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/opencost/opencost/core/pkg/diagnostics"
)

// reproBucketConfig is a syntactically valid S3 bucket config. NewBucketStorage does not
// contact the endpoint, and the export controllers' 10-minute first tick never fires in the test.
const reproBucketConfig = `type: S3
config:
  bucket: repro
  endpoint: 127.0.0.1:1
  access_key: repro
  secret_key: repro
  insecure: true
`

func reproEmitter(t *testing.T) (*KubecostEmitter, string) {
	t.Helper()
	bucketFile := filepath.Join(t.TempDir(), "federated-store.yaml") // not written yet
	return NewKubecostEmitter(nil, diagnostics.NewDiagnosticService(), &EmitterConfig{
		ClusterUID:       "repro-cluster",
		ClusterName:      "repro",
		AppName:          "repro",
		BucketConfigFile: bucketFile,
		ExportIntervals:  DefaultExportIntervalConfig(),
		QueryResolution:  time.Minute,
	}), bucketFile
}

func reproSnapshot(t *testing.T) (*mocks.MockDataSource, emitter.SnapshotProvider, *emitter.ClusterSnapshot) {
	t.Helper()
	ds := mocks.NewMockDataSource()
	provider := emitter.NewConcurrentSnapshotProvider(emitter.DefaultSnapshotConfig())
	snap, err := provider.SnapshotOf(ds)
	if err != nil {
		t.Fatalf("building snapshot: %v", err)
	}
	return ds, provider, snap
}

func TestReproF10EmitPanicsAfterFailedInit(t *testing.T) {
	ke, _ := reproEmitter(t)
	_, _, snap := reproSnapshot(t)
	if err := ke.Init(snap); err == nil {
		t.Fatal("Init unexpectedly succeeded without a bucket config file")
	}

	panicked := func() (p any) {
		defer func() { p = recover() }()
		_ = ke.Emit(context.Background(), snap)
		return nil
	}()
	if panicked != nil {
		t.Fatalf("F-10: Emit after a failed Init panics: %v", panicked)
	}
}

func TestReproF10NeverInitialisesAfterTransientFailure(t *testing.T) {
	ke, bucketFile := reproEmitter(t)
	ds, provider, _ := reproSnapshot(t)

	exporter := emitter.NewExporter(ds, provider, ke)
	if !exporter.Start(20 * time.Millisecond) {
		t.Fatal("failed to start exporter")
	}
	defer exporter.Stop()

	// The first Init fails on the missing bucket config; then the config becomes available
	// (e.g. the secret volume is populated).
	time.Sleep(100 * time.Millisecond)
	if err := os.WriteFile(bucketFile, []byte(reproBucketConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	// Stop and let an in-flight cycle finish before reading the emitter's state.
	exporter.Stop()
	time.Sleep(50 * time.Millisecond)

	// Sanity: the config is good, so a fresh emitter initialises from it.
	fresh := NewKubecostEmitter(nil, diagnostics.NewDiagnosticService(), ke.config)
	_, _, snap := reproSnapshot(t)
	if err := fresh.Init(snap); err != nil {
		t.Fatalf("bucket config should be valid, but Init failed: %v", err)
	}

	if ke.dataSource == nil {
		t.Fatalf("F-10: Kubecost emitter still uninitialised 500ms after its bucket config became available; Init is never retried, nothing exports, and there is no health signal")
	}
}
