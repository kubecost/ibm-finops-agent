package cldy_test

// The upload queue (docs/reliability/FINDINGS.md chunk 02: F-02, F-03, F-23, and the uploader
// halves of F-18 and F-49). The F-02 and F-03 tests were reliability_repro reproductions. Every
// scenario drives upload cycles directly with a fake clock; the Cloudability path runs the real
// ApptioServiceImpl against a scripted ClientService, so no request leaves the process.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/ibm/finops-agent/cldy"
)

// queueRecoveryPeriod stands in for the production default of CLOUDABILITY_RECOVERY_PERIOD.
const queueRecoveryPeriod = 72 * time.Hour

// Request stages as the scripted client sees them.
const (
	stageLogin   = "login"
	stagePresign = "presign"
	stagePut     = "put"
)

// scriptedClient is a cldy.ClientService standing in for Frontdoor, Cloudability and S3. status
// decides each request's outcome: an HTTP status, or a transport error. PUTs that get a 200 are
// recorded, in order, as delivered.
type scriptedClient struct {
	status func(stage, fileName string) (int, error)

	mu        sync.Mutex
	calls     map[string]int
	delivered []string
}

func (c *scriptedClient) Do(r *http.Request, _ string) (*http.Response, error) {
	var stage, fileName string
	var body []byte
	if r.Body != nil {
		body, _ = io.ReadAll(r.Body)
		_ = r.Body.Close()
	}
	switch {
	case strings.HasSuffix(r.URL.Path, "/service/apikeylogin"):
		stage = stageLogin
	case strings.HasSuffix(r.URL.Path, "/clusters/upload"), strings.HasSuffix(r.URL.Path, "/metricsample"):
		stage = stagePresign
		var req struct {
			FileName string `json:"fileName"`
		}
		_ = json.Unmarshal(body, &req)
		fileName = req.FileName // empty for the metrics-collector, which doesn't send it
	default:
		stage = stagePut
		fileName = path.Base(r.URL.Path)
	}
	c.mu.Lock()
	if c.calls == nil {
		c.calls = map[string]int{}
	}
	c.calls[stage]++
	c.mu.Unlock()

	code, err := c.status(stage, fileName)
	if err != nil {
		return nil, err
	}
	resp := &http.Response{StatusCode: code, Status: fmt.Sprintf("%d %s", code, http.StatusText(code)),
		Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}
	if code != http.StatusOK {
		return resp, nil
	}
	switch stage {
	case stageLogin:
		resp.Header.Set("Apptio-Opentoken", "token")
		resp.Header.Set("valid_till", strconv.FormatInt(time.Now().Add(time.Hour).UnixMilli(), 10))
	case stagePresign:
		loc, _ := json.Marshal(map[string]any{
			"result":   map[string]string{"location": "https://s3.test/put/" + fileName},
			"location": "https://s3.test/put/" + fileName,
		})
		resp.Body = io.NopCloser(bytes.NewReader(loc))
	case stagePut:
		c.mu.Lock()
		c.delivered = append(c.delivered, fileName)
		c.mu.Unlock()
	}
	return resp, nil
}

func (c *scriptedClient) Delivered() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.delivered)
}

func (c *scriptedClient) Calls(stage string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[stage]
}

// apptioService is the production Cloudability service on a scripted client.
func apptioService(client cldy.ClientService) *cldy.ApptioServiceImpl {
	return &cldy.ApptioServiceImpl{
		CldyUploadClient: client,
		SecretManager:    cldy.NewKeyValueSecretManager("access", "secret"),
		EnvID:            "env",
		FrontdoorURL:     "https://frontdoor.test",
		CloudabilityURL:  "https://api.test",
	}
}

// always returns status for every request.
func always(code int) func(string, string) (int, error) {
	return func(string, string) (int, error) { return code, nil }
}

var errConnRefused = &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")}

// newQueueUploader builds an uploader on scratch with the production configuration, a 72h
// recovery period, the given services and clock, and sets the live cluster ID.
func newQueueUploader(t *testing.T, scratch *prodScratch, services []cldy.StorageService, clock *fakeClock) *cldy.CldyUploader {
	t.Helper()
	config := scratch.UploaderConfig(t)
	config.RecoveryPeriod = queueRecoveryPeriod
	cu := cldy.NewUploaderForTest(config, services, clock.Now)
	cu.SetClusterID(scratch.ClusterID)
	return cu
}

func sortedCopy(s []string) []string {
	c := slices.Clone(s)
	sort.Strings(c)
	return c
}

// F-02: with no storage service, uploadData looped over nothing, returned nil and deleted the
// payload.
func TestReproF02ZeroServicesKeepsPayload(t *testing.T) {
	clock := newFakeClock(time.Now().Truncate(time.Second))
	scratch := newProdScratch(t, t.TempDir(), "cid-f02")
	cu := newQueueUploader(t, scratch, nil, clock) // e.g. no upload configuration at all
	scratch.AddCompleteSample(t, clock.Now(), 0)
	for range 3 {
		clock.Advance(10 * time.Minute)
		cu.UploadCycleForTest()
	}

	if len(scratch.Uploads(t)) != 1 {
		t.Fatalf("F-02: with zero storage services the upload cycle deleted the payload as if delivered; "+
			"upload/ holds %v and %d sample files are left", scratch.Uploads(t), scratch.ScratchFileCount())
	}
	ev := cu.EventsForTest()
	if !ev.Conditions[cldy.ConditionUploaderUnconfigured] {
		t.Errorf("F-02: condition %s not raised with zero storage services", cldy.ConditionUploaderUnconfigured)
	}
	if len(ev.Dropped) != 0 {
		t.Errorf("F-02: drops %v with zero services and room on disk", ev.Dropped)
	}
}

// F-02: with zero services the backlog is kept until the disk budget forces drops, which are
// counted as no_uploader.
func TestZeroServicesDropsOnlyForDiskBudget(t *testing.T) {
	clock := newFakeClock(time.Now().Truncate(time.Second))
	scratch := newProdScratch(t, t.TempDir(), "cid-f02-budget")
	for i := range 3 {
		scratch.AddUpload(t, clock.Now().Add(time.Duration(i-3)*time.Hour))
	}
	cu := newQueueUploader(t, scratch, nil, clock)
	restore := cldy.SetDiskAvailableForTest(diskFullWhileUploadsOver(scratch, 1))
	defer restore()
	scratch.AddCompleteSample(t, clock.Now(), 0)
	clock.Advance(10 * time.Minute)
	cu.UploadCycleForTest()

	if got := cu.EventsForTest().Dropped; got[cldy.DropReasonNoUploader] == 0 || len(got) != 1 {
		t.Errorf("F-02: disk-budget evictions with no uploader counted as %v, want only %s", got, cldy.DropReasonNoUploader)
	}
}

// diskFullWhileUploadsOver reports no free space while upload/ holds more than keep payloads.
func diskFullWhileUploadsOver(scratch *prodScratch, keep int) func(string) (uint64, error) {
	return func(string) (uint64, error) {
		entries, _ := os.ReadDir(scratch.UploadDir())
		n := 0
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".tgz") {
				n++
			}
		}
		if n > keep {
			return 0, nil
		}
		return 1 << 40, nil
	}
}

// F-03: operateAndRemove removed keys only when every upload succeeded, but uploadData deleted
// each delivered file, so a partial failure left ghosts that failed every later cycle.
func TestReproF03PartialFailureLeavesNoGhosts(t *testing.T) {
	clock := newFakeClock(time.Now().Truncate(time.Second))
	scratch := newProdScratch(t, t.TempDir(), "cid-f03")
	for i := range 3 {
		scratch.AddUpload(t, clock.Now().Add(-time.Duration(30-10*i)*time.Minute))
	}
	svc := &fakeStorage{failOn: func(call int, _ cldy.UploadPayload) error {
		if call == 3 {
			return errors.New("injected upload failure")
		}
		return nil
	}}
	cu := newQueueUploader(t, scratch, []cldy.StorageService{svc}, clock)
	if cu.RecoveredUploads != 3 {
		t.Fatalf("setup: expected 3 queued payloads, got %d", cu.RecoveredUploads)
	}

	// Cycle 1 packages a fresh sample (4 queued) and the 3rd upload fails.
	scratch.AddCompleteSample(t, clock.Now(), 0)
	cu.UploadCycleForTest()
	// Cycle 2: the service has recovered.
	clock.Advance(10 * time.Minute)
	scratch.AddCompleteSample(t, clock.Now(), 1)
	cu.UploadCycleForTest()

	if ghosts := ghostUploads(cu); len(ghosts) > 0 {
		t.Errorf("F-03: after one partial upload failure the queue holds %d ghost entries (delivered and deleted, still "+
			"queued) %v; every later cycle stops on ENOENT and the queue is never pruned again", len(ghosts), ghosts)
	}
	if left := scratch.Uploads(t); len(left) > 0 {
		t.Errorf("F-03: %d payloads still undelivered after the service recovered: %v", len(left), left)
	}
	var names []string
	for _, u := range svc.Uploaded() {
		names = append(names, u.FileName)
	}
	if len(names) != 5 {
		t.Errorf("F-03: %d payloads delivered, want 5 (3 recovered, 2 packaged): %v", len(names), names)
	}
}

// F-03 (b): disk-pressure cleanup deleted payload files that were still queued. Eviction now
// works on the files themselves, oldest first, and every later cycle delivers what is left.
func TestReproF03DiskPressureLeavesNoGhosts(t *testing.T) {
	clock := newFakeClock(time.Now().Truncate(time.Second))
	scratch := newProdScratch(t, t.TempDir(), "cid-f03b")
	oldest := scratch.AddUpload(t, clock.Now().Add(-20*time.Minute))
	scratch.AddUpload(t, clock.Now().Add(-10*time.Minute))
	svc := &fakeStorage{}
	cu := newQueueUploader(t, scratch, []cldy.StorageService{svc}, clock)

	restore := cldy.SetDiskAvailableForTest(diskFullWhileUploadsOver(scratch, 1))
	scratch.AddCompleteSample(t, clock.Now(), 0)
	clock.Advance(10 * time.Minute)
	cu.UploadCycleForTest()
	restore()

	if ghosts := ghostUploads(cu); len(ghosts) > 0 {
		t.Fatalf("F-03: disk-pressure cleanup deleted %d queued payloads but left them in the upload queue as ghosts %v",
			len(ghosts), ghosts)
	}
	if got := cu.EventsForTest().Dropped[cldy.DropReasonDiskPressure]; got != 1 {
		t.Errorf("F-03: %d payloads evicted for disk pressure, want the oldest one", got)
	}
	var names []string
	for _, u := range svc.Uploaded() {
		names = append(names, u.FileName)
	}
	if slices.Contains(names, filepath.Base(oldest)) || len(names) != 2 {
		t.Errorf("delivered %v, want everything but the evicted oldest payload %s", names, filepath.Base(oldest))
	}
}

// ghostUploads returns the queued payloads whose files no longer exist.
func ghostUploads(cu *cldy.CldyUploader) []string {
	var ghosts []string
	for _, p := range cu.QueuedUploadsForTest() {
		if _, err := os.Stat(p); errors.Is(err, os.ErrNotExist) {
			ghosts = append(ghosts, filepath.Base(p))
		}
	}
	return ghosts
}

// A 2h outage followed by recovery delivers everything, oldest first (F-23), and the heartbeat
// shows the outage and the recovery.
func TestUploadOutageThenRecoveryDeliversOldestFirst(t *testing.T) {
	clock := newFakeClock(time.Now().Truncate(time.Second))
	scratch := newProdScratch(t, t.TempDir(), "cid-outage")
	var down atomic.Bool
	down.Store(true)
	client := &scriptedClient{status: func(string, string) (int, error) {
		if down.Load() {
			return 0, errConnRefused
		}
		return http.StatusOK, nil
	}}
	cu := newQueueUploader(t, scratch, []cldy.StorageService{apptioService(client)}, clock)

	const cycles = 12 // 2h of 10-minute cycles
	for i := range cycles {
		scratch.AddCompleteSample(t, clock.Now(), i)
		clock.Advance(10 * time.Minute)
		cu.UploadCycleForTest()
	}
	if got := client.Delivered(); len(got) != 0 {
		t.Fatalf("setup: delivered %v during the outage", got)
	}
	hb := cu.UploadHeartbeat()
	if hb.ConsecutiveFailures != cycles || hb.BacklogFiles != cycles || hb.BacklogBytes == 0 || !hb.LastSuccess.IsZero() {
		t.Errorf("heartbeat during the outage %+v, want %d consecutive failures, %d backlog files and no success",
			hb, cycles, cycles)
	}
	if !hb.LastCycleEnd.Equal(clock.Now()) || hb.LastCycleStart.After(hb.LastCycleEnd) {
		t.Errorf("heartbeat cycle times %+v, want the last cycle ending at %s", hb, clock.Now())
	}

	down.Store(false)
	clock.Advance(10 * time.Minute)
	cu.UploadCycleForTest()

	delivered := client.Delivered()
	if len(delivered) != cycles {
		t.Errorf("delivered %d payloads after the outage, want %d: %v", len(delivered), cycles, delivered)
	}
	if !slices.Equal(delivered, sortedCopy(delivered)) {
		t.Errorf("F-23: payloads delivered out of order: %v", delivered)
	}
	if left := scratch.Uploads(t); len(left) != 0 {
		t.Errorf("upload/ still holds %v", left)
	}
	ev := cu.EventsForTest()
	if len(ev.Dropped) != 0 || ev.HeadDeferred != 0 {
		t.Errorf("drops %v, head deferrals %d after a 2h outage; want none", ev.Dropped, ev.HeadDeferred)
	}
	hb = cu.UploadHeartbeat()
	if hb.ConsecutiveFailures != 0 || hb.BacklogFiles != 0 || hb.BacklogBytes != 0 || !hb.LastSuccess.Equal(clock.Now()) {
		t.Errorf("heartbeat after recovery %+v, want no failures, an empty backlog and a success now", hb)
	}
}

// 401s from Frontdoor for 30 minutes raise upload_auth_failed and delete nothing. The next
// delivery clears the condition.
func TestUploadAuthFailureDeletesNothing(t *testing.T) {
	clock := newFakeClock(time.Now().Truncate(time.Second))
	scratch := newProdScratch(t, t.TempDir(), "cid-auth")
	var refused atomic.Bool
	refused.Store(true)
	client := &scriptedClient{status: func(stage, _ string) (int, error) {
		if refused.Load() && stage == stageLogin {
			return http.StatusUnauthorized, nil
		}
		return http.StatusOK, nil
	}}
	cu := newQueueUploader(t, scratch, []cldy.StorageService{apptioService(client)}, clock)
	for i := range 3 {
		scratch.AddCompleteSample(t, clock.Now(), i)
		clock.Advance(10 * time.Minute)
		cu.UploadCycleForTest()
	}

	ev := cu.EventsForTest()
	if !ev.Conditions[cldy.ConditionUploadAuthFailed] {
		t.Errorf("condition %s not raised by 401s from Frontdoor", cldy.ConditionUploadAuthFailed)
	}
	if got := len(scratch.Uploads(t)); got != 3 {
		t.Errorf("upload/ holds %d payloads after 30 min of 401s, want all 3", got)
	}
	if len(ev.Dropped) != 0 {
		t.Errorf("drops %v on an auth failure", ev.Dropped)
	}
	if got := ev.UploadAttempts[cldy.UploadResultAuth]; got != 3 {
		t.Errorf("%d attempts counted as %s, want one per cycle (3)", got, cldy.UploadResultAuth)
	}

	refused.Store(false)
	clock.Advance(10 * time.Minute)
	cu.UploadCycleForTest()
	if got := client.Delivered(); len(got) != 3 {
		t.Errorf("delivered %v once the credentials were accepted, want 3 payloads", got)
	}
	if cu.EventsForTest().Conditions[cldy.ConditionUploadAuthFailed] {
		t.Errorf("condition %s still set after a delivery", cldy.ConditionUploadAuthFailed)
	}
}

// A 400 on one payload quarantines only that payload; the rest are delivered in order.
func TestUploadRejectedQuarantinesOnlyThatFile(t *testing.T) {
	clock := newFakeClock(time.Now().Truncate(time.Second))
	scratch := newProdScratch(t, t.TempDir(), "cid-400")
	var names []string
	for i := range 3 {
		names = append(names, filepath.Base(scratch.AddUpload(t, clock.Now().Add(time.Duration(i-3)*time.Hour))))
	}
	bad := names[1]
	client := &scriptedClient{status: func(stage, file string) (int, error) {
		if stage == stagePresign && file == bad {
			return http.StatusBadRequest, nil
		}
		return http.StatusOK, nil
	}}
	cu := newQueueUploader(t, scratch, []cldy.StorageService{apptioService(client)}, clock)
	cu.UploadCycleForTest()

	if got, want := client.Delivered(), []string{names[0], names[2]}; !slices.Equal(got, want) {
		t.Errorf("delivered %v, want %v", got, want)
	}
	if got := cu.EventsForTest().Dropped; got[cldy.DropReasonRejectedByBackend] != 1 || len(got) != 1 {
		t.Errorf("drops %v, want %s=1", got, cldy.DropReasonRejectedByBackend)
	}
	q := scratch.Quarantined(t)
	if len(q) != 1 || !strings.HasSuffix(q[0], bad) || !strings.Contains(q[0], cldy.DropReasonRejectedByBackend) {
		t.Errorf("quarantine holds %v, want only %s", q, bad)
	}
	if left := scratch.Uploads(t); len(left) != 0 {
		t.Errorf("upload/ still holds %v", left)
	}
}

// Design item 2: the backlog is uploaded every tick, even when no new sample arrives (a dead
// exporter or a failing Emit).
func TestExporterDeadBacklogStillDrains(t *testing.T) {
	clock := newFakeClock(time.Now().Truncate(time.Second))
	scratch := newProdScratch(t, t.TempDir(), "cid-dead-exporter")
	for i := range 3 {
		scratch.AddUpload(t, clock.Now().Add(time.Duration(i-3)*time.Hour))
	}
	svc := &fakeStorage{}
	cu := newQueueUploader(t, scratch, []cldy.StorageService{svc}, clock)
	clock.Advance(10 * time.Minute)
	cu.UploadCycleForTest() // no sample was emitted since the start

	if got := len(svc.Uploaded()); got != 3 {
		t.Errorf("with no new samples the cycle delivered %d of 3 recovered payloads", got)
	}
	if left := scratch.Uploads(t); len(left) != 0 {
		t.Errorf("upload/ still holds %v", left)
	}
}

// CLOUDABILITY_BACKLOG_MAX_MB caps the backlog, 2048 MiB by default. A value under 1 MiB or one
// that doesn't parse is a configuration error.
func TestBacklogMaxFromEnv(t *testing.T) {
	scratch := newProdScratch(t, t.TempDir(), "cid-backlog-env")
	if got := scratch.UploaderConfig(t).BacklogMaxBytes; got != 2048<<20 {
		t.Errorf("default backlog cap %d bytes, want 2048 MiB", got)
	}
	t.Setenv("CLOUDABILITY_BACKLOG_MAX_MB", "512")
	if got := scratch.UploaderConfig(t).BacklogMaxBytes; got != 512<<20 {
		t.Errorf("CLOUDABILITY_BACKLOG_MAX_MB=512 gave %d bytes", got)
	}
	for _, bad := range []string{"0", "-1", "lots", "1.5GB"} {
		t.Setenv("CLOUDABILITY_BACKLOG_MAX_MB", bad)
		if c, err := cldy.NewEmitterConfigFromEnv(); err == nil {
			t.Errorf("CLOUDABILITY_BACKLOG_MAX_MB=%q accepted as %d bytes, want a configuration error", bad, c.BacklogMaxBytes)
		}
	}
}

// Design item 6: the backlog is bounded in bytes and age; the oldest go first, each counted.
func TestBacklogBoundsEvictOldestWithCounter(t *testing.T) {
	clock := newFakeClock(time.Now().Truncate(time.Second))
	scratch := newProdScratch(t, t.TempDir(), "cid-bounds")
	var paths []string
	for i := range 4 {
		paths = append(paths, scratch.AddUpload(t, clock.Now().Add(time.Duration(i-4)*time.Hour)))
	}
	info, err := os.Stat(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	config := scratch.UploaderConfig(t)
	config.RecoveryPeriod = queueRecoveryPeriod
	config.BacklogMaxBytes = 2*info.Size() + info.Size()/2 // room for two payloads
	svc := &fakeStorage{failOn: func(int, cldy.UploadPayload) error { return errConnRefused }}
	cu := cldy.NewUploaderForTest(config, []cldy.StorageService{svc}, clock.Now)
	cu.SetClusterID(scratch.ClusterID)
	cu.UploadCycleForTest()

	if got := cu.EventsForTest().Dropped; got[cldy.DropReasonBacklogBytes] != 2 || len(got) != 1 {
		t.Errorf("drops %v, want %s=2", got, cldy.DropReasonBacklogBytes)
	}
	if got, want := scratch.Uploads(t), []string{filepath.Base(paths[2]), filepath.Base(paths[3])}; !slices.Equal(got, want) {
		t.Errorf("upload/ holds %v, want the two newest %v", got, want)
	}

	// Past the recovery period, the survivors age out.
	clock.Advance(queueRecoveryPeriod)
	cu.UploadCycleForTest()
	if got := cu.EventsForTest().Dropped[cldy.DropReasonBacklogAge]; got != 2 {
		t.Errorf("%d payloads older than the recovery period evicted as %s, want 2", got, cldy.DropReasonBacklogAge)
	}
	if left := scratch.Uploads(t); len(left) != 0 {
		t.Errorf("upload/ still holds %v", left)
	}
}

// F-18/F-49 (uploader half): the emitter's disk budget counts the upload backlog and evicts the
// oldest queued data first, whichever stage it is in; a statfs error evicts nothing.
func TestDiskBudgetIncludesUploadBacklog(t *testing.T) {
	data := loadTestSnapshot(t)
	dir := t.TempDir()
	scratch := newProdScratch(t, dir, defaultNamespaceUID)
	now := time.Now().Truncate(time.Minute)
	oldest := scratch.AddUpload(t, now.Add(-3*time.Hour))
	newer := scratch.AddUpload(t, now.Add(-2*time.Hour))
	clock := newFakeClock(now)
	config := scratch.UploaderConfig(t)
	config.RecoveryPeriod = queueRecoveryPeriod
	cu := cldy.NewUploaderForTest(config, []cldy.StorageService{&fakeStorage{}}, clock.Now)
	ce := cldy.NewEmitterForTest(cldy.EmitterConfig{UploaderConfig: config, EmitAsJson: true, EmissionInterval: productionInterval}, cu, clock.Now)
	if err := ce.Init(data); err != nil {
		t.Fatalf("Init: %v", err)
	}

	restore := cldy.SetDiskAvailableForTest(func(string) (uint64, error) { return 0, errors.New("statfs: input/output error") })
	clock.Advance(productionInterval)
	if err := ce.Emit(context.Background(), data); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	restore()
	if got := scratch.Uploads(t); len(got) != 2 {
		t.Fatalf("F-49: a statfs error removed payloads; upload/ holds %v", got)
	}

	restore = cldy.SetDiskAvailableForTest(diskFullWhileUploadsOver(scratch, 1))
	defer restore()
	clock.Advance(productionInterval)
	if err := ce.Emit(context.Background(), data); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if _, err := os.Stat(oldest); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("F-18: the oldest payload was not evicted under disk pressure (err %v)", err)
	}
	if _, err := os.Stat(newer); err != nil {
		t.Errorf("F-18: a newer payload was evicted although evicting the oldest made room: %v", err)
	}
	ev := cu.EventsForTest()
	if ev.Dropped[cldy.DropReasonDiskPressure] != 1 || ev.Dropped[cldy.DropReasonDiskPressureSkipped] != 0 {
		t.Errorf("drops %v, want %s=1 and the sample written", ev.Dropped, cldy.DropReasonDiskPressure)
	}
	if samples, _, _ := cldy.FinalisedSamplesForTest(scratch.ClusterScratchDir()); len(samples) != 2 {
		t.Errorf("%d finalised samples, want both emitted samples kept", len(samples))
	}
}

// The disk budget never evicts the payload being uploaded; the upload completes and removes it.
func TestEvictionSkipsPayloadBeingUploaded(t *testing.T) {
	data := loadTestSnapshot(t)
	dir := t.TempDir()
	scratch := newProdScratch(t, dir, defaultNamespaceUID)
	now := time.Now().Truncate(time.Minute)
	first := scratch.AddUpload(t, now.Add(-3*time.Hour))
	second := scratch.AddUpload(t, now.Add(-2*time.Hour))
	clock := newFakeClock(now)
	config := scratch.UploaderConfig(t)
	config.RecoveryPeriod = queueRecoveryPeriod
	var ce *cldy.Emitter
	var emitErr error
	svc := &fakeStorage{failOn: func(call int, _ cldy.UploadPayload) error {
		if call == 1 { // the emitter runs while the first payload is on the wire
			restore := cldy.SetDiskAvailableForTest(diskFullWhileUploadsOver(scratch, 1))
			clock.Advance(productionInterval)
			emitErr = ce.Emit(context.Background(), data)
			restore()
		}
		return nil
	}}
	cu := cldy.NewUploaderForTest(config, []cldy.StorageService{svc}, clock.Now)
	ce = cldy.NewEmitterForTest(cldy.EmitterConfig{UploaderConfig: config, EmitAsJson: true, EmissionInterval: productionInterval}, cu, clock.Now)
	if err := ce.Init(data); err != nil {
		t.Fatalf("Init: %v", err)
	}
	cu.UploadCycleForTest()
	if emitErr != nil {
		t.Fatalf("Emit: %v", emitErr)
	}

	uploads := svc.Uploaded()
	if len(uploads) != 1 || uploads[0].FileName != filepath.Base(first) || uploads[0].Err != nil {
		t.Errorf("uploads %+v, want only the complete in-flight payload %s", uploads, filepath.Base(first))
	}
	if got := cu.EventsForTest().Dropped[cldy.DropReasonDiskPressure]; got != 1 {
		t.Errorf("%d payloads evicted, want 1 (%s)", got, filepath.Base(second))
	}
	if left := scratch.Uploads(t); len(left) != 0 {
		t.Errorf("upload/ holds %v, want the delivered payload removed and the other evicted", left)
	}
}

// Design item 8: a payload that keeps failing at the head for more than 6h is moved behind the
// rest once; if it keeps failing there while others are delivered, it is quarantined as
// undeliverable.
func TestHeadOfLineDeferredThenQuarantined(t *testing.T) {
	clock := newFakeClock(time.Now().Truncate(time.Second))
	scratch := newProdScratch(t, t.TempDir(), "cid-hol")
	var names []string
	for i := range 3 {
		names = append(names, filepath.Base(scratch.AddUpload(t, clock.Now().Add(time.Duration(i-3)*time.Hour))))
	}
	stuck := names[0]
	client := &scriptedClient{status: func(stage, file string) (int, error) {
		if stage == stagePresign && file == stuck {
			return http.StatusInternalServerError, nil
		}
		return http.StatusOK, nil
	}}
	cu := newQueueUploader(t, scratch, []cldy.StorageService{apptioService(client)}, clock)

	for _, step := range []time.Duration{0, 3 * time.Hour, 3*time.Hour + time.Minute} {
		clock.Advance(step)
		cu.UploadCycleForTest()
	}
	if got := client.Delivered(); !slices.Equal(got, names[1:]) {
		t.Errorf("after the head was stuck for over 6h, delivered %v, want %v", got, names[1:])
	}
	if got := cu.EventsForTest().HeadDeferred; got != 1 {
		t.Errorf("head deferrals %d, want 1", got)
	}
	if got := cu.EventsForTest().Dropped; len(got) != 0 {
		t.Errorf("drops %v on the first deferral", got)
	}

	for _, step := range []time.Duration{10 * time.Minute, 3 * time.Hour, 3*time.Hour + time.Minute} {
		clock.Advance(step)
		cu.UploadCycleForTest()
	}
	if got := cu.EventsForTest().Dropped; got[cldy.DropReasonUndeliverable] != 1 || len(got) != 1 {
		t.Errorf("drops %v, want %s=1", got, cldy.DropReasonUndeliverable)
	}
	if q := scratch.Quarantined(t); len(q) != 1 || !strings.HasSuffix(q[0], stuck) {
		t.Errorf("quarantine holds %v, want %s", q, stuck)
	}
	if left := cu.QueuedUploadsForTest(); len(left) != 0 {
		t.Errorf("queue still holds %v", left)
	}
}

// The head-of-line rule never drops data in an outage that stops every upload: nothing is
// quarantined without another payload having been delivered since the deferral.
func TestHeadOfLineGlobalOutageDropsNothing(t *testing.T) {
	clock := newFakeClock(time.Now().Truncate(time.Second))
	scratch := newProdScratch(t, t.TempDir(), "cid-hol-outage")
	var names []string
	for i := range 2 {
		names = append(names, filepath.Base(scratch.AddUpload(t, clock.Now().Add(time.Duration(i-2)*time.Hour))))
	}
	var down atomic.Bool
	down.Store(true)
	client := &scriptedClient{status: func(string, string) (int, error) {
		if down.Load() {
			return http.StatusServiceUnavailable, nil
		}
		return http.StatusOK, nil
	}}
	cu := newQueueUploader(t, scratch, []cldy.StorageService{apptioService(client)}, clock)
	for range 48 { // 48h with the exporter dead, one cycle an hour
		clock.Advance(time.Hour)
		cu.UploadCycleForTest()
	}
	if got := cu.EventsForTest().Dropped; len(got) != 0 {
		t.Errorf("drops %v in a 48h outage inside the recovery period", got)
	}
	down.Store(false)
	clock.Advance(time.Hour)
	cu.UploadCycleForTest()
	if got := client.Delivered(); !slices.Equal(got, names) {
		t.Errorf("delivered %v after the outage, want %v in order", got, names)
	}
}

// uploadErrorFor runs one upload of a small payload through the real Cloudability service with
// the scripted client and returns its error.
func uploadErrorFor(t *testing.T, status func(stage, fileName string) (int, error)) (error, *scriptedClient) {
	t.Helper()
	client := &scriptedClient{status: status}
	return apptioService(client).Upload(testPayload(t)), client
}

func testPayload(t *testing.T) cldy.UploadPayload {
	t.Helper()
	p := filepath.Join(t.TempDir(), "cid_2026-01-02-03-04-05.tgz")
	if err := os.WriteFile(p, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	return cldy.UploadPayload{ClusterUID: "cid", FileName: filepath.Base(p), AgentVersion: "1.0.0", UploadHash: "hash", FilePath: p}
}

// stageStatus fails the named stage with code; every other request succeeds.
func stageStatus(stage string, code int) func(string, string) (int, error) {
	return func(s, _ string) (int, error) {
		if s == stage {
			return code, nil
		}
		return http.StatusOK, nil
	}
}

// Design item 3: one classification decides every upload outcome. Status rows run the real
// Cloudability, metrics-collector, S3 and Azure services; transport rows use real connections
// where they can be made locally.
func TestClassifyUpload(t *testing.T) {
	restore := cldy.SetRetryBackoffForTest(func(int) time.Duration { return 0 })
	defer restore()

	type row struct {
		name       string
		err        func(t *testing.T) error
		wantResult string
		wantAction string
	}
	cloudability := func(stage string, code int) func(t *testing.T) error {
		return func(t *testing.T) error {
			err, _ := uploadErrorFor(t, stageStatus(stage, code))
			return err
		}
	}
	rows := []row{
		{"200", cloudability(stagePut, http.StatusOK), cldy.UploadResultOK, "delete"},
		{"400 presign", cloudability(stagePresign, http.StatusBadRequest), cldy.UploadResultRejected, "quarantine"},
		{"400 PUT", cloudability(stagePut, http.StatusBadRequest), cldy.UploadResultRejected, "quarantine"},
		{"400 login", cloudability(stageLogin, http.StatusBadRequest), cldy.UploadResultRetryable, "stop"},
		{"401 login", cloudability(stageLogin, http.StatusUnauthorized), cldy.UploadResultAuth, "stop-auth"},
		{"403 login", cloudability(stageLogin, http.StatusForbidden), cldy.UploadResultAuth, "stop-auth"},
		{"401 presign", cloudability(stagePresign, http.StatusUnauthorized), cldy.UploadResultAuth, "stop-auth"},
		{"403 presign", cloudability(stagePresign, http.StatusForbidden), cldy.UploadResultAuth, "stop-auth"},
		{"403 PUT", cloudability(stagePut, http.StatusForbidden), cldy.UploadResultRetryable, "stop"},
		{"404 presign", cloudability(stagePresign, http.StatusNotFound), cldy.UploadResultRetryable, "stop"},
		{"404 PUT", cloudability(stagePut, http.StatusNotFound), cldy.UploadResultRetryable, "stop"},
		{"413 presign", cloudability(stagePresign, http.StatusRequestEntityTooLarge), cldy.UploadResultRejected, "quarantine"},
		{"413 PUT", cloudability(stagePut, http.StatusRequestEntityTooLarge), cldy.UploadResultRejected, "quarantine"},
		{"429 presign", cloudability(stagePresign, http.StatusTooManyRequests), cldy.UploadResultRetryable, "stop"},
		{"429 PUT", cloudability(stagePut, http.StatusTooManyRequests), cldy.UploadResultRetryable, "stop"},
		{"500 login", cloudability(stageLogin, http.StatusInternalServerError), cldy.UploadResultRetryable, "stop"},
		{"500 presign", cloudability(stagePresign, http.StatusInternalServerError), cldy.UploadResultRetryable, "stop"},
		{"502 PUT", cloudability(stagePut, http.StatusBadGateway), cldy.UploadResultRetryable, "stop"},
		{"503 presign", cloudability(stagePresign, http.StatusServiceUnavailable), cldy.UploadResultRetryable, "stop"},
		{"metrics-collector 403 presign", func(t *testing.T) error {
			client := &scriptedClient{status: stageStatus(stagePresign, http.StatusForbidden)}
			svc := &cldy.MetricsCollectorServiceImpl{APIKey: "key", BaseURL: "https://mc.test/metricsample", CldyUploadClient: client}
			return svc.Upload(testPayload(t))
		}, cldy.UploadResultAuth, "stop-auth"},
		{"metrics-collector 400 PUT", func(t *testing.T) error {
			client := &scriptedClient{status: stageStatus(stagePut, http.StatusBadRequest)}
			svc := &cldy.MetricsCollectorServiceImpl{APIKey: "key", BaseURL: "https://mc.test/metricsample", CldyUploadClient: client}
			return svc.Upload(testPayload(t))
		}, cldy.UploadResultRejected, "quarantine"},
		{"timeout", func(t *testing.T) error {
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-release:
				case <-r.Context().Done():
				}
			}))
			defer server.Close()
			defer close(release)
			svc := apptioService(cldy.NewApptioClient(cldy.ApptioConfig{Timeout: 50 * time.Millisecond}))
			svc.FrontdoorURL = server.URL
			return svc.Upload(testPayload(t))
		}, cldy.UploadResultTimeout, "stop"},
		{"connection refused", func(t *testing.T) error {
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			addr := l.Addr().String()
			_ = l.Close()
			svc := apptioService(cldy.NewApptioClient(cldy.ApptioConfig{Timeout: time.Second}))
			svc.FrontdoorURL = "http://" + addr
			return svc.Upload(testPayload(t))
		}, cldy.UploadResultRetryable, "stop"},
		{"DNS failure", func(t *testing.T) error {
			dnsErr := &url.Error{Op: "Post", URL: "https://frontdoor.invalid/service/apikeylogin",
				Err: &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{Err: "no such host", Name: "frontdoor.invalid", IsNotFound: true}}}
			return (&cldy.ApptioServiceImpl{CldyUploadClient: failingClient{dnsErr}, SecretManager: cldy.NewKeyValueSecretManager("a", "s"),
				FrontdoorURL: "https://frontdoor.invalid"}).Upload(testPayload(t))
		}, cldy.UploadResultRetryable, "stop"},
		{"S3 403", func(t *testing.T) error {
			return cldy.CustomS3Client{S3Bucket: "b", UploadClient: s3Failing{awserr.NewRequestFailure(awserr.New("AccessDenied", "denied", nil), 403, "r")}}.Upload(testPayload(t))
		}, cldy.UploadResultAuth, "stop-auth"},
		{"S3 400 (a region or signing problem, not this payload)", func(t *testing.T) error {
			return cldy.CustomS3Client{S3Bucket: "b", UploadClient: s3Failing{awserr.NewRequestFailure(awserr.New("AuthorizationHeaderMalformed", "region", nil), 400, "r")}}.Upload(testPayload(t))
		}, cldy.UploadResultRetryable, "stop"},
		{"S3 413", func(t *testing.T) error {
			return cldy.CustomS3Client{S3Bucket: "b", UploadClient: s3Failing{awserr.NewRequestFailure(awserr.New("EntityTooLarge", "big", nil), 413, "r")}}.Upload(testPayload(t))
		}, cldy.UploadResultRejected, "quarantine"},
		{"S3 503", func(t *testing.T) error {
			return cldy.CustomS3Client{S3Bucket: "b", UploadClient: s3Failing{awserr.NewRequestFailure(awserr.New("SlowDown", "slow", nil), 503, "r")}}.Upload(testPayload(t))
		}, cldy.UploadResultRetryable, "stop"},
		{"Azure 403", func(t *testing.T) error {
			return cldy.CustomBlobClient{BlobContainerName: "c", UploadClient: blobFailing{&azcore.ResponseError{StatusCode: 403, ErrorCode: "AuthorizationFailure"}}}.Upload(testPayload(t))
		}, cldy.UploadResultAuth, "stop-auth"},
		{"Azure 500", func(t *testing.T) error {
			return cldy.CustomBlobClient{BlobContainerName: "c", UploadClient: blobFailing{&azcore.ResponseError{StatusCode: 500}}}.Upload(testPayload(t))
		}, cldy.UploadResultRetryable, "stop"},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			err := r.err(t)
			result, action := cldy.ClassifyUploadForTest(err)
			if result != r.wantResult || action != r.wantAction {
				t.Errorf("error %v: classified %s/%s, want %s/%s", err, result, action, r.wantResult, r.wantAction)
			}
		})
	}
}

// A 403 on the presigned PUT means the URL expired: the service presigns once more and retries
// before giving up.
func TestPresignedPut403RepresignsOnce(t *testing.T) {
	var puts atomic.Int32
	err, client := uploadErrorFor(t, func(stage, _ string) (int, error) {
		if stage == stagePut && puts.Add(1) == 1 {
			return http.StatusForbidden, nil
		}
		return http.StatusOK, nil
	})
	if err != nil {
		t.Fatalf("upload after one expired presigned URL: %v", err)
	}
	if got := client.Calls(stagePresign); got != 2 {
		t.Errorf("%d presign requests, want 2 (the original and one after the 403)", got)
	}

	err, client = uploadErrorFor(t, stageStatus(stagePut, http.StatusForbidden))
	if err == nil {
		t.Fatal("upload succeeded with every PUT refused")
	}
	if p, u := client.Calls(stagePresign), client.Calls(stagePut); p != 2 || u != 2 {
		t.Errorf("%d presigns and %d PUTs with every PUT refused, want 2 of each (one retry)", p, u)
	}
}

type failingClient struct{ err error }

func (c failingClient) Do(r *http.Request, _ string) (*http.Response, error) {
	if r.Body != nil {
		_ = r.Body.Close()
	}
	return nil, c.err
}

type s3Failing struct{ err error }

func (s s3Failing) Do(*s3manager.UploadInput) error { return s.err }

type blobFailing struct{ err error }

func (b blobFailing) Do(*cldy.BlobUploadInput) error { return b.err }
