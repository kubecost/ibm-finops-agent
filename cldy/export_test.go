package cldy

import "time"

// Test-only access to the package's seams for the external cldy_test package. Nothing here is
// compiled into production builds.

// Crash point names for SetTestHook.
const (
	CrashAfterCreate  = crashAfterCreate
	CrashMidTar       = crashMidTar
	CrashBeforeRename = crashBeforeRename
	CrashAfterRename  = crashAfterRename
	CrashAfterUpload  = crashAfterUpload

	CrashSampleFileWritten  = crashSampleFileWritten
	CrashSampleBeforeRename = crashSampleBeforeRename
	CrashSampleAfterRename  = crashSampleAfterRename
)

// The finalised-sample contract (sample.go).
const (
	StagingPrefix    = stagingPrefix
	ManifestFileName = manifestFileName
)

// Drop reasons and conditions.
const (
	DropReasonDiskPressure          = dropReasonDiskPressure
	DropReasonDiskPressureSkipped   = dropReasonDiskPressureSkipped
	DropReasonShortLivedPodOverflow = dropReasonShortLivedPodOverflow
	DropReasonRecoveryExpired       = dropReasonRecoveryExpired
	DropReasonClusterIDMismatch     = dropReasonClusterIDMismatch
	DropReasonCorruptPayload        = dropReasonCorruptPayload
	DropReasonInvalidSample         = dropReasonInvalidSample
	DropReasonInvalidPayload        = dropReasonInvalidPayload
	DropReasonQuarantineEvicted     = dropReasonQuarantineEvicted
	ConditionDiskPressure           = conditionDiskPressure
	ConditionDiskSpaceUnknown       = conditionDiskSpaceUnknown
	ConditionUninitialised          = conditionUninitialised
)

// MaxPendingShortLivedPods is the emitter's short-lived pod bound.
const MaxPendingShortLivedPods = maxPendingShortLivedPods

// FinalisedSamplesForTest lists the finalised samples under clusterDir, oldest first, and the
// directories without the staging prefix that failed validation.
func FinalisedSamplesForTest(clusterDir string) (paths, invalid []string, err error) {
	samples, invalid, err := listFinalisedSamples(clusterDir)
	for _, s := range samples {
		paths = append(paths, s.Path)
	}
	return paths, invalid, err
}

// ValidateSampleForTest validates dir's manifest, including every file's SHA-256.
func ValidateSampleForTest(dir string) error {
	_, err := validateSample(dir, true)
	return err
}

// WriteManifestForTest writes a manifest for the files already in dir, as the emitter does
// before finalising a sample.
func WriteManifestForTest(dir, clusterID string, ts time.Time, nodeCount int) error {
	_, err := writeManifest(dir, clusterID, ts, nodeCount)
	return err
}

// SetDiskAvailableForTest replaces the free-space probe and returns a func restoring it. Tests
// that set it must not run in parallel.
func SetDiskAvailableForTest(f func(dir string) (uint64, error)) (restore func()) {
	prev := diskAvailable
	diskAvailable = f
	return func() { diskAvailable = prev }
}

// EventsForTest returns the emitter's event counts. It fails if a different sink was installed.
func (ce *Emitter) EventsForTest() EventCountsSnapshot {
	return ce.events.(*EventCounts).Snapshot()
}

// QuarantineDirName is the directory under <ScratchDir>/scratch/ that holds quarantined items.
const QuarantineDirName = quarantineDirName

// SetQuarantineLimitsForTest replaces the quarantine bounds and returns a func restoring them.
// Tests that set it must not run in parallel.
func SetQuarantineLimitsForTest(maxBytes int64, maxAge time.Duration) (restore func()) {
	prevBytes, prevAge := quarantineMaxBytes, quarantineMaxAge
	quarantineMaxBytes, quarantineMaxAge = maxBytes, maxAge
	return func() { quarantineMaxBytes, quarantineMaxAge = prevBytes, prevAge }
}

// EventsForTest returns the uploader's event counts, which an emitter built around it shares.
// It fails if a different sink was installed.
func (cu *CldyUploader) EventsForTest() EventCountsSnapshot {
	return cu.events.(*EventCounts).Snapshot()
}

// NewUploaderForTest runs startup recovery exactly as NewCldyUploader does, but uses the given
// storage services and clock (nil means time.Now) and does not start uploadLoop. Drive upload
// cycles with UploadCycleForTest.
func NewUploaderForTest(config UploaderConfig, services []StorageService, now func() time.Time) *CldyUploader {
	return newCldyUploader(config, services, make(chan struct{}), now, nil)
}

// UploadCycleForTest runs one tick of uploadLoop.
func (cu *CldyUploader) UploadCycleForTest() {
	cu.uploadCycle()
}

// QueuedUploadsForTest returns the payload paths currently queued for upload.
func (cu *CldyUploader) QueuedUploadsForTest() []string {
	return cu.uploadSet.contents()
}

// QueuedSamplesForTest returns the sample directories currently queued for packaging.
func (cu *CldyUploader) QueuedSamplesForTest() []string {
	return cu.sampleSet.contents()
}

// NewEmitterForTest builds an Emitter around the given uploader with the given clock (nil means
// time.Now), without constructing a real uploader. Given a *CldyUploader, the emitter shares its
// event sink, as in production.
func NewEmitterForTest(config EmitterConfig, uploader Uploader, now func() time.Time) *Emitter {
	return newEmitter(config, uploader, now)
}

// SetTestHook installs hook at the package's crash points and returns a func restoring the
// previous hook. Tests that set it must not run in parallel.
func SetTestHook(hook func(point string) error) (restore func()) {
	prev := testHook
	testHook = hook
	return func() { testHook = prev }
}
