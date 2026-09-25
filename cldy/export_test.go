package cldy

import "time"

// Test-only access to the package's seams for the external cldy_test package. Nothing here is
// compiled into production builds.

// Crash point names for SetTestHook.
const (
	CrashAfterCreate  = crashAfterCreate
	CrashMidTar       = crashMidTar
	CrashBeforeRename = crashBeforeRename
	CrashAfterUpload  = crashAfterUpload
)

// NewUploaderForTest runs startup recovery exactly as NewCldyUploader does, but uses the given
// storage services and clock (nil means time.Now) and does not start uploadLoop. Drive upload
// cycles with UploadCycleForTest.
func NewUploaderForTest(config UploaderConfig, services []StorageService, now func() time.Time) *CldyUploader {
	return newCldyUploader(config, services, make(chan struct{}), now)
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
// time.Now), without constructing a real uploader.
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
