package cldy_test

// Crash-consistent startup recovery and payloads (docs/reliability/FINDINGS.md chunk 01: F-01,
// F-04, F-05, F-36, F-48, F-50 and the recovery half of F-22). The F-01, F-04, F-05 and F-36
// tests were reliability_repro reproductions.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ibm/finops-agent/cldy"
)

// testRecoveryPeriod is decision D8's default recovery period.
const testRecoveryPeriod = 72 * time.Hour

// F-01: startup recovery walks scratch/ and treats scratch/<clusterID>/ as the first sample; on
// reaching the first real sample it removes the whole cluster directory.
func TestReproF01RecoveryKeepsPendingSamples(t *testing.T) {
	scratch := newProdScratch(t, t.TempDir(), "cid-f01")
	now := time.Now()
	for i := range 3 {
		scratch.AddCompleteSample(t, now.Add(-time.Duration(30-10*i)*time.Minute), i)
	}
	config := scratch.UploaderConfig(t)
	config.RecoveryPeriod = testRecoveryPeriod // look past F-36

	cu := cldy.NewUploaderForTest(config, nil, nil)

	if got := len(scratch.Uploads(t)); got != 3 {
		t.Fatalf("F-01: startup recovery on the production layout turned %d of 3 complete samples into payloads "+
			"(RecoveredSamples=%d, %d sample files left in scratch/); the rest were deleted without being counted",
			got, cu.RecoveredSamples, scratch.ScratchFileCount())
	}
	if cu.RecoveredSamples != 3 || cu.RecoveredUploads != 3 {
		t.Errorf("RecoveredSamples=%d RecoveredUploads=%d, want 3 and 3", cu.RecoveredSamples, cu.RecoveredUploads)
	}
	if n := scratch.ScratchFileCount(); n != 0 {
		t.Errorf("%d sample files left in scratch/ after they were packaged", n)
	}
	if ev := cu.EventsForTest(); len(ev.Dropped) != 0 {
		t.Errorf("recovering complete samples counted drops: %v", ev.Dropped)
	}
}

// F-36: RecoveryPeriod is never set from configuration, so it is 0 in production and every
// payload in upload/ is deleted at startup.
func TestReproF36RestartKeepsUploadBacklog(t *testing.T) {
	scratch := newProdScratch(t, t.TempDir(), "cid-f36")
	path := scratch.AddUpload(t, time.Now().Add(-5*time.Minute))
	config := scratch.UploaderConfig(t) // exactly what production builds from the environment

	cu := cldy.NewUploaderForTest(config, nil, nil)

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("F-36: a 5-minute-old payload in upload/ was deleted at startup (production RecoveryPeriod=%v, "+
			"RecoveredUploads=%d): %v", config.RecoveryPeriod, cu.RecoveredUploads, err)
	}
	if cu.RecoveredUploads != 1 {
		t.Fatalf("F-36: payload survived but was not queued for upload (RecoveredUploads=%d)", cu.RecoveredUploads)
	}
}

// F-36 / D8: CLOUDABILITY_RECOVERY_PERIOD sets the recovery period, 72h by default. A value
// under one upload interval (including a bare number, which parses as nanoseconds) or one that
// doesn't parse is a configuration error.
func TestF36RecoveryPeriodFromEnv(t *testing.T) {
	scratch := newProdScratch(t, t.TempDir(), "cid-f36-env")
	if got := scratch.UploaderConfig(t).RecoveryPeriod; got != testRecoveryPeriod {
		t.Errorf("F-36: default recovery period %v, want %v", got, testRecoveryPeriod)
	}
	t.Setenv("CLOUDABILITY_RECOVERY_PERIOD", "6h")
	if got := scratch.UploaderConfig(t).RecoveryPeriod; got != 6*time.Hour {
		t.Errorf("CLOUDABILITY_RECOVERY_PERIOD=6h gave %v", got)
	}
	for _, bad := range []string{"0", "-1h", "soon", "72", "5m"} {
		t.Setenv("CLOUDABILITY_RECOVERY_PERIOD", bad)
		if c, err := cldy.NewEmitterConfigFromEnv(); err == nil {
			t.Errorf("CLOUDABILITY_RECOVERY_PERIOD=%q accepted as %v, want a configuration error", bad, c.RecoveryPeriod)
		}
	}
}

// F-36: data older than the recovery period is dropped at startup, counted and logged, and data
// inside it is kept.
func TestF36ExpiredDataIsCountedDrop(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	scratch := newProdScratch(t, t.TempDir(), "cid-f36-exp")
	oldUpload := scratch.AddUpload(t, now.Add(-testRecoveryPeriod-time.Hour))
	freshUpload := scratch.AddUpload(t, now.Add(-time.Hour))
	oldSample := scratch.AddCompleteSample(t, now.Add(-testRecoveryPeriod-2*time.Hour), 0)
	scratch.AddCompleteSample(t, now.Add(-10*time.Minute), 1)

	cu := cldy.NewUploaderForTest(scratch.UploaderConfig(t), nil, newFakeClock(now).Now)

	if _, err := os.Stat(oldUpload); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a payload older than the recovery period was kept: %v", err)
	}
	if _, err := os.Stat(oldSample); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a sample older than the recovery period was kept: %v", err)
	}
	if !slices.Contains(cu.QueuedUploadsForTest(), freshUpload) {
		t.Errorf("F-36: a payload inside the recovery period was not queued: %v", cu.QueuedUploadsForTest())
	}
	if cu.RecoveredSamples != 1 || cu.RecoveredUploads != 2 {
		t.Errorf("RecoveredSamples=%d RecoveredUploads=%d, want 1 and 2", cu.RecoveredSamples, cu.RecoveredUploads)
	}
	if got := cu.EventsForTest().Dropped; got[cldy.DropReasonRecoveryExpired] != 2 || len(got) != 1 {
		t.Errorf("F-36: drops %v, want %s=2", got, cldy.DropReasonRecoveryExpired)
	}
}

// F-36: under disk pressure only the oldest payloads are evicted, until there is room, each
// counted; a recent payload survives.
func TestF36DiskPressureKeepsRecentUploads(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	scratch := newProdScratch(t, t.TempDir(), "cid-f36-disk")
	recent := scratch.AddUpload(t, now.Add(-time.Hour))
	old := scratch.AddUpload(t, now.Add(-40*time.Hour))
	svc := &fakeStorage{failOn: func(int, cldy.UploadPayload) error { return errors.New("backend down") }}
	cu := cldy.NewUploaderForTest(scratch.UploaderConfig(t), []cldy.StorageService{svc}, newFakeClock(now).Now)
	cu.SetClusterID(scratch.ClusterID)
	if cu.RecoveredUploads != 2 {
		t.Fatalf("F-36: %d of 2 payloads inside the recovery period queued at startup", cu.RecoveredUploads)
	}

	restore := cldy.SetDiskAvailableForTest(diskFullWhileUploadsOver(scratch, 1))
	defer restore()
	scratch.AddCompleteSample(t, now, 0)
	cu.UploadCycleForTest()

	if _, err := os.Stat(recent); err != nil {
		t.Errorf("F-36: disk-pressure cleanup deleted a 1-hour-old payload: %v", err)
	}
	if _, err := os.Stat(old); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the oldest payload survived disk-pressure cleanup: %v", err)
	}
	if got := cu.QueuedUploadsForTest(); len(got) != 2 || got[0] != recent {
		t.Errorf("queue after cleanup %v, want %s and the new payload", got, recent)
	}
	if got := cu.EventsForTest().Dropped[cldy.DropReasonDiskPressure]; got != 1 {
		t.Errorf("disk-pressure deletions counted %d, want 1", got)
	}
}

// F-04: payloads are written in place. A crash mid-tar leaves a truncated .tgz under its final
// name, which recovery queues and uploads once F-36 is fixed.
func TestReproF04CrashMidTarNotUploaded(t *testing.T) {
	clock := newFakeClock(time.Now().Truncate(time.Second))
	scratch := newProdScratch(t, t.TempDir(), "cid-f04")
	config := scratch.UploaderConfig(t)
	config.RecoveryPeriod = testRecoveryPeriod

	// First process: killed while writing the payload.
	cu := cldy.NewUploaderForTest(config, nil, clock.Now)
	cu.SetClusterID(scratch.ClusterID)
	cu.AddSample(scratch.AddCompleteSample(t, clock.Now(), 0))
	onDisk := map[string][]byte{}
	headers := 0
	restore := cldy.SetTestHook(func(point string) error {
		if point != cldy.CrashMidTar {
			return nil
		}
		if headers++; headers < 5 {
			return nil
		}
		// Capture what a kill -9 at this point leaves on disk.
		matches, _ := filepath.Glob(filepath.Join(scratch.UploadDir(), "*"))
		dot, _ := filepath.Glob(filepath.Join(scratch.UploadDir(), ".*"))
		for _, m := range append(matches, dot...) {
			b, err := os.ReadFile(m)
			if err != nil {
				t.Fatalf("capturing %s: %v", m, err)
			}
			onDisk[m] = b
		}
		return errors.New("killed mid-tar")
	})
	cu.UploadCycleForTest()
	restore()
	if len(onDisk) == 0 {
		t.Fatalf("setup: crash point %q was not reached with a payload on disk", cldy.CrashMidTar)
	}
	for path, b := range onDisk {
		if err := os.WriteFile(path, b, 0o644); err != nil {
			t.Fatalf("restoring crash state: %v", err)
		}
	}

	// Second process: restarts, recovers, and uploads on its first cycle.
	clock.Advance(10 * time.Minute)
	svc := &fakeStorage{}
	cu2 := cldy.NewUploaderForTest(config, []cldy.StorageService{svc}, clock.Now)
	cu2.SetClusterID(scratch.ClusterID)
	cu2.AddSample(scratch.AddCompleteSample(t, clock.Now(), 1))
	cu2.UploadCycleForTest()

	for _, u := range svc.Uploaded() {
		if u.Err != nil {
			t.Fatalf("F-04: a payload truncated by a crash mid-tar was recovered and uploaded: %s: %v", u.FileName, u.Err)
		}
	}
	if got := len(svc.Uploaded()); got != 2 {
		t.Errorf("uploaded %d payloads, want 2 (the sample the crash interrupted, repackaged, and the new one)", got)
	}
	if ev := cu2.EventsForTest(); len(ev.Dropped) != 0 {
		t.Errorf("a crash mid-tar lost no finalised sample but counted drops: %v", ev.Dropped)
	}
}

// F-04: a temporary payload left by a crash is discarded at startup, not queued. Its samples are
// still on disk, so this is not a drop.
func TestF04LeftoverPartialDiscarded(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	scratch := newProdScratch(t, t.TempDir(), "cid-f04-partial")
	good := scratch.AddUpload(t, now.Add(-time.Hour))
	partial := filepath.Join(scratch.UploadDir(), "."+scratch.UploadName(now.Add(-time.Minute))+".partial")
	if err := os.WriteFile(partial, []byte("\x1f\x8b truncated"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	cu := cldy.NewUploaderForTest(scratch.UploaderConfig(t), nil, newFakeClock(now).Now)

	if _, err := os.Stat(partial); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("F-04: a leftover temporary payload survived startup: %v", err)
	}
	if got := cu.QueuedUploadsForTest(); len(got) != 1 || got[0] != good {
		t.Errorf("F-48: queue after startup %v, want only %s", got, good)
	}
	ev := cu.EventsForTest()
	if ev.UnfinalizedDiscarded != 1 || len(ev.Dropped) != 0 {
		t.Errorf("unfinalised discards %d, drops %v; want 1 and none", ev.UnfinalizedDiscarded, ev.Dropped)
	}
}

// F-04: every payload is read end to end just before it is uploaded. A truncated or empty one is
// quarantined and counted, never uploaded, and doesn't hold up the rest.
func TestF04CorruptPayloadQuarantinedBeforeUpload(t *testing.T) {
	clock := newFakeClock(time.Now().Truncate(time.Second))
	scratch := newProdScratch(t, t.TempDir(), "cid-f04-corrupt")
	good := scratch.AddUpload(t, clock.Now().Add(-30*time.Minute))
	truncated := scratch.AddUpload(t, clock.Now().Add(-20*time.Minute))
	b, err := os.ReadFile(truncated)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := os.WriteFile(truncated, b[:len(b)/2], 0o644); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	empty := filepath.Join(scratch.UploadDir(), scratch.UploadName(clock.Now().Add(-10*time.Minute)))
	if err := os.WriteFile(empty, nil, 0o644); err != nil { // renamed but its data never reached disk
		t.Fatalf("write: %v", err)
	}
	svc := &fakeStorage{}
	cu := cldy.NewUploaderForTest(scratch.UploaderConfig(t), []cldy.StorageService{svc}, clock.Now)
	cu.SetClusterID(scratch.ClusterID)
	cu.AddSample(scratch.AddCompleteSample(t, clock.Now(), 0))

	cu.UploadCycleForTest()

	var uploaded []string
	for _, u := range svc.Uploaded() {
		uploaded = append(uploaded, u.FileName)
		if u.Err != nil {
			t.Errorf("F-04: corrupt payload %s was uploaded: %v", u.FileName, u.Err)
		}
	}
	if !slices.Contains(uploaded, filepath.Base(good)) || len(uploaded) != 2 {
		t.Errorf("uploaded %v, want the good backlog payload and the new one", uploaded)
	}
	if q := scratch.Quarantined(t); len(q) != 2 {
		t.Errorf("quarantine holds %v, want the 2 corrupt payloads", q)
	}
	if left := scratch.Uploads(t); len(left) != 0 {
		t.Errorf("upload/ still holds %v", left)
	}
	if got := cu.EventsForTest().Dropped[cldy.DropReasonCorruptPayload]; got != 2 {
		t.Errorf("corrupt payloads counted %d, want 2", got)
	}
	if len(cu.QueuedUploadsForTest()) != 0 {
		t.Errorf("queue not empty after a clean cycle: %v", cu.QueuedUploadsForTest())
	}
}

// F-05: recovery runs in NewCldyUploader before Emitter.Init sets the cluster ID, so recovered
// payloads are named "_<ts>.tgz" and their tar entries lack the <clusterID>/ segment.
func TestReproF05RecoveredPayloadHasClusterID(t *testing.T) {
	scratch := newProdScratch(t, t.TempDir(), "cid-f05")
	scratch.AddCompleteSample(t, time.Now().Add(-10*time.Minute), 0)
	config := scratch.UploaderConfig(t)
	config.RecoveryPeriod = testRecoveryPeriod

	cldy.NewUploaderForTest(config, nil, nil)

	uploads := scratch.Uploads(t)
	if len(uploads) == 0 {
		t.Fatalf("F-05: no recovered payload to check; want one named %s_<ts>.tgz (masked here by F-01, "+
			"which deletes the sample first)", scratch.ClusterID)
	}
	for _, name := range uploads {
		if !strings.HasPrefix(name, scratch.ClusterID+"_") {
			t.Errorf("F-05: recovered payload %q is not named <clusterID>_<ts>.tgz", name)
		}
		entries, err := readTGZ(filepath.Join(scratch.UploadDir(), name))
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}
		for _, e := range entries {
			if !strings.Contains(e, "/"+scratch.ClusterID+"/") {
				t.Errorf("F-05: tar entry %q in recovered payload %s lacks the <clusterID>/ segment", e, name)
				break
			}
		}
	}
}

// F-05: no payload is built, and nothing is uploaded, before the live cluster ID is known.
func TestF05NoPayloadWithoutClusterID(t *testing.T) {
	scratch := newProdScratch(t, t.TempDir(), "cid-f05-empty")
	svc := &fakeStorage{}
	cu := cldy.NewUploaderForTest(scratch.UploaderConfig(t), []cldy.StorageService{svc}, nil)
	sample := scratch.AddCompleteSample(t, time.Now(), 0)
	cu.AddSample(sample)

	if path, err := cu.ConstructPayload(time.Now()); err == nil {
		t.Errorf("F-05: ConstructPayload built %q with an empty cluster ID", path)
	}
	cu.UploadCycleForTest()

	if got := scratch.Uploads(t); len(got) != 0 {
		t.Errorf("F-05: payloads built with an empty cluster ID: %v", got)
	}
	if len(svc.Uploaded()) != 0 {
		t.Errorf("uploaded before the cluster ID was known: %v", svc.Uploaded())
	}
	if err := cldy.ValidateSampleForTest(sample); err != nil {
		t.Errorf("sample damaged: %v", err)
	}
	if queued, _, _ := cldy.FinalisedSamplesForTest(scratch.ClusterScratchDir()); !slices.Contains(queued, sample) {
		t.Errorf("sample dequeued without being packaged")
	}
}

// F-05: recovery takes each sample's cluster ID from its directory and each payload's from its
// name. Once the live cluster ID is known, data for any other cluster is quarantined and
// counted, never uploaded under the live one.
func TestF05OtherClusterQuarantined(t *testing.T) {
	clock := newFakeClock(time.Now().Truncate(time.Second))
	dir := t.TempDir()
	live := newProdScratch(t, dir, "cid-live")
	other := newProdScratch(t, dir, "cid-other")
	live.AddCompleteSample(t, clock.Now().Add(-20*time.Minute), 0)
	live.AddUpload(t, clock.Now().Add(-time.Hour))
	other.AddCompleteSample(t, clock.Now().Add(-25*time.Minute), 0)
	other.AddUpload(t, clock.Now().Add(-2*time.Hour))
	svc := &fakeStorage{}
	cu := cldy.NewUploaderForTest(live.UploaderConfig(t), []cldy.StorageService{svc}, clock.Now)

	cu.SetClusterID(live.ClusterID)
	cu.AddSample(live.AddCompleteSample(t, clock.Now(), 1))
	cu.UploadCycleForTest()

	var uploaded []string
	for _, u := range svc.Uploaded() {
		uploaded = append(uploaded, u.FileName)
		if !strings.HasPrefix(u.FileName, live.ClusterID+"_") {
			t.Errorf("F-05: uploaded %s under cluster %s", u.FileName, live.ClusterID)
		}
		for _, e := range u.Entries {
			if !strings.Contains(e, "/"+live.ClusterID+"/") {
				t.Errorf("F-05: payload %s holds %s", u.FileName, e)
				break
			}
		}
	}
	if len(uploaded) != 3 {
		t.Errorf("uploaded %v, want the live cluster's 2 recovered payloads and the new one", uploaded)
	}
	if q := live.Quarantined(t); len(q) != 2 {
		t.Errorf("quarantine holds %v, want the other cluster's sample and payload", q)
	}
	if got := cu.EventsForTest().Dropped[cldy.DropReasonClusterIDMismatch]; got != 2 {
		t.Errorf("cluster ID mismatches counted %d, want 2", got)
	}
}

// F-48: one file name that doesn't parse stops the upload recovery walk, and the files after it
// are never queued.
func TestF48BadNamesDoNotStopUploadRecovery(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	scratch := newProdScratch(t, t.TempDir(), "cid-f48")
	a := scratch.AddUpload(t, now.Add(-2*time.Hour))
	b := scratch.AddUpload(t, now.Add(-time.Hour))
	for _, name := range []string{"0-stray.txt", "_" + now.UTC().Format("2006-01-02-15-04-05") + ".tgz", "cid-f48_notadate.tgz"} {
		if err := os.WriteFile(filepath.Join(scratch.UploadDir(), name), []byte("x"), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	cu := cldy.NewUploaderForTest(scratch.UploaderConfig(t), nil, newFakeClock(now).Now)

	queued := cu.QueuedUploadsForTest()
	slices.Sort(queued)
	if !slices.Equal(queued, []string{a, b}) {
		t.Errorf("F-48: queued %v after bad file names, want %v", queued, []string{a, b})
	}
	if q := scratch.Quarantined(t); len(q) != 3 {
		t.Errorf("quarantine holds %v, want the 3 bad names", q)
	}
	if got := cu.EventsForTest().Dropped[cldy.DropReasonInvalidPayload]; got != 3 {
		t.Errorf("invalid payload names counted %d, want 3", got)
	}
}

// F-50: a failure at any step before the payload is renamed into place leaves the samples on
// disk and queued, and leaves nothing in upload/.
func TestF50PayloadFailureKeepsSamples(t *testing.T) {
	for _, point := range []string{cldy.CrashAfterCreate, cldy.CrashMidTar, cldy.CrashBeforeRename} {
		t.Run(point, func(t *testing.T) {
			scratch := newProdScratch(t, t.TempDir(), "cid-f50")
			cu := cldy.NewUploaderForTest(scratch.UploaderConfig(t), nil, nil)
			cu.SetClusterID(scratch.ClusterID)
			sample := scratch.AddCompleteSample(t, time.Now(), 0)
			cu.AddSample(sample)
			restore := cldy.SetTestHook(func(p string) error {
				if p == point {
					return fmt.Errorf("injected failure at %s", p)
				}
				return nil
			})
			_, err := cu.ConstructPayload(time.Now())
			restore()
			if err == nil {
				t.Fatalf("setup: crash point %s not reached", point)
			}
			if err := cldy.ValidateSampleForTest(sample); err != nil {
				t.Errorf("F-50: a payload failure at %s damaged the sample: %v", point, err)
			}
			if !slices.Contains(cu.QueuedSamplesForTest(), sample) {
				t.Errorf("F-50: a payload failure at %s dequeued the sample", point)
			}
			entries, _ := os.ReadDir(scratch.UploadDir())
			if len(entries) != 0 {
				t.Errorf("F-04: a payload failure at %s left %v in upload/", point, entries)
			}

			path, err := cu.ConstructPayload(time.Now())
			if err != nil {
				t.Fatalf("retry: %v", err)
			}
			if _, err := readTGZ(path); err != nil {
				t.Errorf("retried payload invalid: %v", err)
			}
			if _, err := os.Stat(sample); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("sample not removed after it was packaged: %v", err)
			}
		})
	}
}

// F-22 (recovery half): a directory in scratch/<clusterID>/ that is neither staging nor a valid
// finalised sample (written before the manifest existed, or torn) is quarantined and counted.
func TestF22InvalidSampleDirsQuarantined(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	scratch := newProdScratch(t, t.TempDir(), "cid-f22")
	legacy := scratch.AddIncompleteSample(t, now.Add(-30*time.Minute), 0)
	torn := scratch.AddCompleteSample(t, now.Add(-20*time.Minute), 1)
	if err := os.WriteFile(filepath.Join(torn, "pods.jsonl"), []byte("{"), 0o644); err != nil {
		t.Fatalf("tear: %v", err)
	}
	// Same size, different bytes: only the hash catches it.
	flipped := scratch.AddCompleteSample(t, now.Add(-15*time.Minute), 2)
	nodes, err := os.ReadFile(filepath.Join(flipped, "nodes.jsonl"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	nodes[0] ^= 0xff
	if err := os.WriteFile(filepath.Join(flipped, "nodes.jsonl"), nodes, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	scratch.AddCompleteSample(t, now.Add(-10*time.Minute), 3)

	cu := cldy.NewUploaderForTest(scratch.UploaderConfig(t), nil, newFakeClock(now).Now)

	for _, d := range []string{legacy, torn, flipped} {
		if _, err := os.Stat(d); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("invalid sample %s left in place: %v", filepath.Base(d), err)
		}
	}
	if q := scratch.Quarantined(t); len(q) != 3 {
		t.Errorf("quarantine holds %v, want the 3 invalid samples", q)
	}
	if got := cu.EventsForTest().Dropped[cldy.DropReasonInvalidSample]; got != 3 {
		t.Errorf("invalid samples counted %d, want 3", got)
	}
	if cu.RecoveredSamples != 1 || scratch.ScratchFileCount() != 0 {
		t.Errorf("RecoveredSamples=%d with %d files left in scratch/, want 1 and 0", cu.RecoveredSamples, scratch.ScratchFileCount())
	}
}

// Staging directories from the previous process are never finalised samples: recovery removes
// them as unfinalised discards, not drops, and the emitter built around the uploader reports
// through the same sink.
func TestRecoveryDiscardsStagingWithoutDrop(t *testing.T) {
	data := loadTestSnapshot(t)
	now := time.Now().Truncate(time.Minute)
	dir := t.TempDir()
	scratch := newProdScratch(t, dir, defaultNamespaceUID)
	// What Init leaves (stats only) and what a finalised sample's successor holds (baselines).
	for i, f := range []string{"stats-summary-n1.json", "baseline-summary-n1.json"} {
		d := filepath.Join(scratch.ClusterScratchDir(), cldy.StagingPrefix+fmt.Sprintf("%d_%d", now.Add(-time.Minute).UnixMilli(), i))
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(d, f), []byte("{}"), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	scratch.AddCompleteSample(t, now.Add(-5*time.Minute), 2)
	clock := newFakeClock(now)

	cu := cldy.NewUploaderForTest(scratch.UploaderConfig(t), nil, clock.Now)

	if n := scratch.ScratchFileCount(); n != 0 {
		t.Errorf("%d files left in scratch/ after recovery", n)
	}
	ev := cu.EventsForTest()
	if ev.UnfinalizedDiscarded != 2 || len(ev.Dropped) != 0 {
		t.Errorf("unfinalised discards %d, drops %v; want 2 and none", ev.UnfinalizedDiscarded, ev.Dropped)
	}

	ce := newTestEmitter(dir, cu, clock)
	if err := ce.Init(data); err != nil {
		t.Fatalf("Init: %v", err)
	}
	clock.Advance(3 * time.Minute)
	if err := ce.Emit(context.Background(), data); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if got := ce.EventsForTest().UnfinalizedDiscarded; got != 2 {
		t.Errorf("the emitter reports %d unfinalised discards, want the uploader's 2 (one shared sink)", got)
	}
}

// Quarantine is bounded by age and size; each eviction is a counted drop.
func TestQuarantineBounded(t *testing.T) {
	restore := cldy.SetQuarantineLimitsForTest(3000, 24*time.Hour)
	defer restore()
	now := time.Now().Truncate(time.Second)
	scratch := newProdScratch(t, t.TempDir(), "cid-quarantine")
	if err := os.MkdirAll(scratch.QuarantineDir(), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	for name, age := range map[string]time.Duration{"a": 48 * time.Hour, "b": 4 * time.Hour, "c": 3 * time.Hour, "d": 2 * time.Hour, "e": time.Hour} {
		p := filepath.Join(scratch.QuarantineDir(), name)
		if err := os.WriteFile(p, bytes.Repeat([]byte("x"), 1000), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := os.Chtimes(p, now.Add(-age), now.Add(-age)); err != nil {
			t.Fatalf("chtimes: %v", err)
		}
	}

	cu := cldy.NewUploaderForTest(scratch.UploaderConfig(t), nil, newFakeClock(now).Now)

	if q := scratch.Quarantined(t); !slices.Equal(q, []string{"c", "d", "e"}) {
		t.Errorf("quarantine holds %v, want [c d e] (a too old, b the oldest over the size cap)", q)
	}
	if got := cu.EventsForTest().Dropped[cldy.DropReasonQuarantineEvicted]; got != 2 {
		t.Errorf("quarantine evictions counted %d, want 2", got)
	}
}

// Two payloads for the same second don't overwrite each other: the later one's timestamp is
// bumped by a second. The file-name format is unchanged.
func TestPayloadNameCollisionBumpsTimestamp(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	ts := now.Add(-10 * time.Minute)
	scratch := newProdScratch(t, t.TempDir(), "cid-collide")
	existing := scratch.AddUpload(t, ts.Add(time.Second))
	before, err := os.ReadFile(existing)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	scratch.AddCompleteSample(t, ts, 0)
	scratch.AddCompleteSample(t, ts.Add(100*time.Millisecond), 1)

	cu := cldy.NewUploaderForTest(scratch.UploaderConfig(t), nil, newFakeClock(now).Now)

	want := []string{scratch.UploadName(ts), scratch.UploadName(ts.Add(time.Second)), scratch.UploadName(ts.Add(2 * time.Second))}
	if got := scratch.Uploads(t); !slices.Equal(got, want) {
		t.Errorf("uploads %v, want %v", got, want)
	}
	after, err := os.ReadFile(existing)
	if err != nil || !bytes.Equal(before, after) {
		t.Errorf("an existing payload was overwritten by a recovered sample (err %v)", err)
	}
	if cu.RecoveredUploads != 3 {
		t.Errorf("RecoveredUploads=%d, want 3", cu.RecoveredUploads)
	}
}

// recordingUploader passes samples through to an uploader and reports each one.
type recordingUploader struct {
	cldy.Uploader
	added func(sample string)
}

func (u *recordingUploader) AddSample(sample string) {
	u.added(sample)
	u.Uploader.AddSample(sample)
}

// sampleOfEntry returns the sample directory name a payload's tar entry belongs to.
func sampleOfEntry(entry string) string {
	name, _, _ := strings.Cut(entry, "/")
	return name
}

// The crash-point matrix: a process is killed at every crash point in turn, a new process starts
// on what the kill left and runs normally. Every sample finalised before the kill is uploaded at
// least once, every upload is a complete archive, and nothing is dropped: a kill never costs a
// finalised sample, and an unfinalised one is a discard, not a drop.
func TestCrashPointMatrixNoSilentLoss(t *testing.T) {
	data := loadTestSnapshot(t)
	start := time.Now().Truncate(time.Minute)
	emitterConfig := func(dir string) cldy.EmitterConfig {
		return cldy.EmitterConfig{
			UploaderConfig:   cldy.UploaderConfig{ScratchDir: dir, RecoveryPeriod: testRecoveryPeriod, UploadFrequency: 10 * time.Minute},
			EmitAsJson:       true,
			EmissionInterval: productionInterval,
		}
	}

	// process runs one agent process on dir for ticks one-minute ticks, with an upload cycle at
	// each tick in cycles. If killAt > 0 it is killed at the killAt'th crash point: the state of
	// dir at that moment is copied to a new directory, and the process's later effects are
	// ignored.
	type result struct {
		dir       string
		points    int
		sequence  []string // the crash points reached, in order
		finalised map[string]bool
		delivered map[string]bool
		cu        *cldy.CldyUploader
		ce        *cldy.Emitter
		uploads   []fakeUpload
	}
	process := func(t *testing.T, dir string, clock *fakeClock, ticks int, cycles []int, killAt int) result {
		r := result{dir: dir, finalised: map[string]bool{}, delivered: map[string]bool{}}
		dead := false
		svc := &fakeStorage{failOn: func(int, cldy.UploadPayload) error {
			if dead {
				return errors.New("process killed")
			}
			return nil
		}}
		restore := cldy.SetTestHook(func(point string) error {
			if dead {
				return errors.New("process killed")
			}
			r.points++
			r.sequence = append(r.sequence, point)
			if r.points != killAt {
				return nil
			}
			dead = true
			r.dir = filepath.Join(t.TempDir(), "after-kill")
			if err := os.CopyFS(r.dir, os.DirFS(dir)); err != nil {
				t.Fatalf("capturing the state at the kill: %v", err)
			}
			return fmt.Errorf("killed at %s", point)
		})
		defer restore()

		config := emitterConfig(dir)
		r.cu = cldy.NewUploaderForTest(config.UploaderConfig, []cldy.StorageService{svc}, clock.Now)
		up := &recordingUploader{Uploader: r.cu, added: func(s string) {
			if !dead {
				r.finalised[filepath.Base(s)] = true
			}
		}}
		r.ce = cldy.NewEmitterForTest(config, up, clock.Now)
		_ = r.ce.Init(data)
		for i := 1; i <= ticks && !dead; i++ {
			clock.Advance(time.Minute)
			_ = r.ce.Emit(context.Background(), data)
			if slices.Contains(cycles, i) && !dead {
				r.cu.UploadCycleForTest()
			}
		}
		r.uploads = svc.Uploaded()
		for _, u := range r.uploads {
			for _, e := range u.Entries {
				r.delivered[sampleOfEntry(e)] = true
			}
		}
		if dead {
			// A kill after a sample's rename but before the emitter queued it still leaves a
			// finalised sample.
			clusterDirs, _ := filepath.Glob(filepath.Join(r.dir, "scratch", "*"))
			for _, cd := range clusterDirs {
				paths, _, _ := cldy.FinalisedSamplesForTest(cd)
				for _, p := range paths {
					r.finalised[filepath.Base(p)] = true
				}
			}
		}
		return r
	}

	run := func(t *testing.T, killAt int) []string {
		clock := newFakeClock(start)
		first := process(t, t.TempDir(), clock, 13, []int{5, 10}, killAt)
		if killAt == 0 && len(first.finalised) != 4 {
			t.Fatalf("setup: a clean run finalised %d samples, want 4", len(first.finalised))
		}
		clock.Advance(time.Minute)
		second := process(t, first.dir, clock, 10, []int{10}, 0)

		for _, u := range append(first.uploads, second.uploads...) {
			if u.Err != nil {
				t.Errorf("kill at crash point %d: uploaded an invalid payload %s: %v", killAt, u.FileName, u.Err)
			}
		}
		for s := range first.finalised {
			if !first.delivered[s] && !second.delivered[s] {
				t.Errorf("kill at crash point %d: finalised sample %s was never uploaded and not counted as a drop", killAt, s)
			}
		}
		for s := range second.finalised {
			if !second.delivered[s] {
				t.Errorf("kill at crash point %d: sample %s of the restarted process was never uploaded", killAt, s)
			}
		}
		for _, ev := range []cldy.EventCountsSnapshot{second.cu.EventsForTest(), second.ce.EventsForTest()} {
			if len(ev.Dropped) != 0 {
				t.Errorf("kill at crash point %d: the restarted process counted drops %v; a kill loses no finalised sample", killAt, ev.Dropped)
			}
		}
		left, _ := os.ReadDir(filepath.Join(first.dir, "upload"))
		if len(left) != 0 {
			t.Errorf("kill at crash point %d: upload/ holds %v after the restarted process's last cycle", killAt, left)
		}
		if q, _ := os.ReadDir(filepath.Join(first.dir, "scratch", cldy.QuarantineDirName)); len(q) != 0 {
			t.Errorf("kill at crash point %d: quarantined %v", killAt, q)
		}
		return first.sequence
	}

	sequence := run(t, 0)
	for _, point := range []string{
		cldy.CrashAfterCreate, cldy.CrashMidTar, cldy.CrashBeforeRename, cldy.CrashAfterRename, cldy.CrashAfterUpload,
		cldy.CrashSampleFileWritten, cldy.CrashSampleBeforeRename, cldy.CrashSampleAfterRename,
	} {
		if !slices.Contains(sequence, point) {
			t.Errorf("crash point %s not reached by the first process", point)
		}
	}
	// Every occurrence of every crash point, except that kills after each sample file and each tar
	// header are sampled (every 5th): those states differ only in how many files the staging
	// directory or the temporary payload holds. Chunk 04's
	// TestF22CrashAtEveryWriteStepLeavesOnlyFinalisedSamples kills after every sample file.
	occurrences := map[string]int{}
	for k, point := range sequence {
		if occurrences[point]++; (point == cldy.CrashSampleFileWritten || point == cldy.CrashMidTar) && occurrences[point]%5 != 1 {
			continue
		}
		t.Run(fmt.Sprintf("kill-at-%d-%s", k+1, point), func(t *testing.T) { run(t, k+1) })
	}
}
