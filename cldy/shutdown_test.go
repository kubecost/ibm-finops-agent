package cldy_test

// Graceful shutdown of the Cloudability uploader (docs/reliability/FINDINGS.md chunk 10, F-16):
// the upload loop has an owner that stops it, and Stop packages and uploads what the last cycle
// left behind, bounded by the shutdown budget.

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ibm/finops-agent/cldy"
)

// blockingStorage is a StorageService whose uploads hang until release is closed, as against a
// backend that accepts the connection and never answers. Every upload then fails.
type blockingStorage struct {
	release chan struct{}

	mu      sync.Mutex
	started int
}

func (s *blockingStorage) Upload(cldy.UploadPayload) error {
	s.mu.Lock()
	s.started++
	s.mu.Unlock()
	<-s.release
	return errors.New("connection reset")
}

// goroutinesIn counts the goroutines with fn on their stack.
func goroutinesIn(fn string) int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return strings.Count(string(buf[:n]), fn+"(")
		}
		buf = make([]byte, 2*len(buf))
	}
}

// SIGTERM with one unpackaged sample and a reachable backend: Stop stops the upload loop,
// packages the sample and delivers it before returning.
func TestStopDeliversUnpackagedSample(t *testing.T) {
	scratch := newProdScratch(t, t.TempDir(), "cid-stop")
	config := scratch.UploaderConfig(t)
	config.UploadFrequency = 24 * time.Hour // the loop's own cycle never runs in the test
	svc := &fakeStorage{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cu := cldy.StartUploaderForTest(ctx, config, []cldy.StorageService{svc}, nil)
	cu.SetClusterID(scratch.ClusterID)
	scratch.AddCompleteSample(t, time.Now().Add(-time.Minute), 0)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopCancel()
	if err := cu.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	uploads := svc.Uploaded()
	if len(uploads) != 1 {
		t.Fatalf("F-16: %d payloads delivered at shutdown, want the one unpackaged sample", len(uploads))
	}
	if uploads[0].Err != nil {
		t.Errorf("delivered payload is not a complete archive: %v", uploads[0].Err)
	}
	if left := scratch.Uploads(t); len(left) != 0 {
		t.Errorf("payloads left in upload/ after delivery: %v", left)
	}
	if left := cu.QueuedSamplesForTest(); len(left) != 0 {
		t.Errorf("samples left unpackaged: %v", left)
	}
	if n := goroutinesIn("cldy.(*CldyUploader).uploadLoop"); n != 0 {
		t.Errorf("F-16: %d upload loops still running after Stop", n)
	}
}

// SIGTERM with an unreachable backend: Stop returns within its budget, and the sample is on disk
// as a complete payload that the next start recovers and uploads.
func TestStopWithUnreachableBackendKeepsPayloadForNextStart(t *testing.T) {
	scratch := newProdScratch(t, t.TempDir(), "cid-stop-down")
	config := scratch.UploaderConfig(t)
	config.UploadFrequency = 24 * time.Hour
	down := &blockingStorage{release: make(chan struct{})}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cu := cldy.StartUploaderForTest(ctx, config, []cldy.StorageService{down}, nil)
	cu.SetClusterID(scratch.ClusterID)
	scratch.AddCompleteSample(t, time.Now().Add(-time.Minute), 0)

	// Long enough to package the sample even under the race detector; the upload then hangs
	// until the budget runs out.
	const budget = 3 * time.Second
	stopCtx, stopCancel := context.WithTimeout(context.Background(), budget)
	defer stopCancel()
	start := time.Now()
	err := cu.Stop(stopCtx)
	if elapsed := time.Since(start); elapsed > budget+time.Second {
		t.Fatalf("F-16: Stop took %s against a hung backend, over its %s budget", elapsed, budget)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Stop = %v, want the budget's DeadlineExceeded", err)
	}
	down.mu.Lock()
	started := down.started
	down.mu.Unlock()
	if started != 1 {
		t.Fatalf("%d uploads started before the budget ran out, want 1", started)
	}

	// The process exits here. The payload was written atomically before the upload started.
	uploads := scratch.Uploads(t)
	if len(uploads) != 1 {
		t.Fatalf("upload/ holds %v after an interrupted shutdown upload, want one payload", uploads)
	}
	if _, err := readTGZ(scratch.UploadDir() + "/" + uploads[0]); err != nil {
		t.Fatalf("payload left by the interrupted shutdown is not complete: %v", err)
	}
	close(down.release)
	cu.WaitStoppedForTest()

	// Next start: recovery finds the payload and the first cycle delivers it.
	svc := &fakeStorage{}
	next := cldy.NewUploaderForTest(config, []cldy.StorageService{svc}, nil)
	next.SetClusterID(scratch.ClusterID)
	next.UploadCycleForTest()
	if got := svc.Uploaded(); len(got) != 1 || got[0].FileName != uploads[0] || got[0].Err != nil {
		t.Fatalf("next start delivered %+v, want the complete payload %s", got, uploads[0])
	}
}

// The upload loop belongs to the context it was started with: cancelling it stops the loop, so
// neither the process nor a test leaks it.
func TestUploadLoopStopsWithItsContext(t *testing.T) {
	scratch := newProdScratch(t, t.TempDir(), "cid-loop")
	config := scratch.UploaderConfig(t)
	config.UploadFrequency = 24 * time.Hour

	before := goroutinesIn("cldy.(*CldyUploader).uploadLoop")
	ctx, cancel := context.WithCancel(context.Background())
	cldy.StartUploaderForTest(ctx, config, nil, nil)
	if n := goroutinesIn("cldy.(*CldyUploader).uploadLoop"); n != before+1 {
		t.Fatalf("%d upload loops running after start, want %d", n, before+1)
	}
	cancel()

	deadline := time.Now().Add(5 * time.Second)
	for goroutinesIn("cldy.(*CldyUploader).uploadLoop") != before {
		if time.Now().After(deadline) {
			t.Fatalf("F-16: the upload loop is still running 5s after its context was cancelled")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
