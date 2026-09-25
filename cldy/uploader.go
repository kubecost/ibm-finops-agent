package cldy

import (
	"archive/tar"
	"compress/flate"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ibm/finops-agent/pkg/version"
	"github.com/opencost/opencost/core/pkg/log"
)

// The head-of-line rule: a payload that fails headMaxFailures consecutive cycles at the head of
// the queue, over more than headMaxStuck, is moved behind the rest once. If it then fails the
// same way at the head again, it is quarantined as undeliverable, but only if another payload
// was delivered since it was moved, so an outage that stops every upload drops nothing.
const (
	headMaxFailures = 3
	headMaxStuck    = 6 * time.Hour
)

type Uploader interface {
	// AddSample tells the uploader that a sample was finalised. The uploader lists finalised
	// samples on disk when it packages them (queue.go), so this is only a notification.
	AddSample(sample string)
	SetClusterID(id string)
}

type CldyUploader struct {
	config           UploaderConfig
	mu               sync.RWMutex // guards clusterID
	clusterID        string
	agentVersion     string
	UploadPathDir    string
	StorageServices  []StorageService
	RecoveredSamples int
	RecoveredUploads int
	recoveryPeriod   time.Duration
	backlogMaxBytes  int64

	// queue is the upload queue on disk. The emitter built around this uploader shares it.
	queue *diskQueue

	// events receives drops, discards, conditions and upload attempts. conditions holds the
	// active conditions, for edge-triggered logging and health checks. The emitter built around
	// this uploader shares both.
	events     EventSink
	conditions *conditionStore

	// head is the payload at the head of the queue and its run of failures, for the
	// head-of-line rule. Only the upload loop touches it.
	head headOfLine

	// payloadRatio is the last payload's size over its samples' size, for payloadRoom. Guarded
	// by queue.mu.
	payloadRatio float64

	hbMu      sync.Mutex
	heartbeat UploadHeartbeat

	// cancelLoop stops uploadLoop, and loopDone is closed when it has returned. Both are nil
	// when the loop was never started. stopped is closed when Stop's last upload cycle ends.
	cancelLoop context.CancelFunc
	loopDone   chan struct{}
	stopOnce   sync.Once
	stopped    chan struct{}

	// now is the uploader's clock. It is nil in production (see clock) and only set by tests.
	now func() time.Time
}

type headOfLine struct {
	path     string
	failures int
	since    time.Time
}

// NewCldyUploader builds the uploader, runs startup recovery and starts the upload loop, which
// runs until ctx is cancelled or Stop is called.
func NewCldyUploader(ctx context.Context, config UploaderConfig) (*CldyUploader, error) {
	services, problems := newStorageServices(config)
	uploader, err := newCldyUploader(config, services, nil, nil)
	if err != nil {
		return nil, err
	}
	uploader.setCondition(conditionUploaderMisconfigured, problems.misconfigured,
		"the selected Cloudability upload path is missing settings or credentials; see the error above")
	uploader.setCondition(conditionUploadConnectivityFailed, problems.connectivityFailed,
		"the Cloudability upload connectivity test failed; uploads are still attempted every cycle")
	uploader.startLoop(ctx)
	return uploader, nil
}

// startLoop runs uploadLoop until ctx is cancelled or Stop is called.
func (cu *CldyUploader) startLoop(ctx context.Context) {
	ctx, cu.cancelLoop = context.WithCancel(ctx)
	cu.loopDone = make(chan struct{})
	go func() {
		defer close(cu.loopDone)
		cu.uploadLoop(ctx)
	}()
}

// Stop drains the uploader at shutdown. It stops the upload loop and waits for the cycle in
// flight, then runs one last upload cycle, which packages the samples finalised since the last
// cycle and uploads the queue. Both waits are bounded by ctx; Stop returns ctx's error if it
// expires. Whatever isn't delivered stays on disk as complete payloads (they are written
// atomically), which startup recovery uploads on the next start.
//
// An upload in flight when ctx expires can't be cancelled (StorageService.Upload takes no
// context), so its goroutine is left to end with the process. Stop runs the last cycle once.
func (cu *CldyUploader) Stop(ctx context.Context) error {
	err := errors.New("the Cloudability uploader was already stopped")
	cu.stopOnce.Do(func() { err = cu.stop(ctx) })
	return err
}

func (cu *CldyUploader) stop(ctx context.Context) error {
	cu.stopped = make(chan struct{})
	if cu.cancelLoop != nil {
		cu.cancelLoop()
		select {
		case <-cu.loopDone:
		case <-ctx.Done():
			close(cu.stopped)
			return fmt.Errorf("the Cloudability upload cycle in flight did not end in time; its payloads stay queued on disk: %w", ctx.Err())
		}
	}
	go func() {
		defer close(cu.stopped)
		cu.uploadCycle()
	}()
	select {
	case <-cu.stopped:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("the last Cloudability upload cycle did not end in time; undelivered payloads stay queued on disk: %w", ctx.Err())
	}
}

// storageServiceProblems records what went wrong building the storage service.
type storageServiceProblems struct {
	// misconfigured: settings or credentials are missing, so no service was built.
	misconfigured bool
	// connectivityFailed: the startup connectivity test failed. The service is still used.
	connectivityFailed bool
}

// newStorageServices builds the storage service selected by the upload configuration. A failed
// connectivity test is advisory: the service is still returned and retried every cycle.
func newStorageServices(config UploaderConfig) ([]StorageService, storageServiceProblems) {
	var storageServices []StorageService
	var problems storageServiceProblems
	add := func(kind string, service StorageService, err error) {
		switch {
		case errors.Is(err, errConnectivityTest):
			problems.connectivityFailed = true
			log.Errorf("The %s uploader failed its connectivity test; it will be retried every upload cycle: %v", kind, err)
		case err != nil:
			problems.misconfigured = true
			log.Errorf("Failed to create %s uploader: %v", kind, err)
		}
		if service != nil {
			storageServices = append(storageServices, service)
		}
	}

	// Legacy metrics-collector upload path (API key / API Gateway)
	if hasAPIKeyConfigured(config.APIKeySecretManager) {
		metricsCollectorService, err := NewMetricsCollectorService(config.ApptioConfig)
		add("metrics-collector", metricsCollectorService, err)

		// Apptio Frontdoor upload path
	} else if config.EnvID != "" {
		apptioService, err := NewApptioService(config.ApptioConfig)
		add("cloudability", apptioService, err)

		// S3 emitter
	} else if config.CustomS3UploadBucket != "" && config.CustomS3UploadRegion != "" {
		s3Client, err := NewCustomS3Client(config.CustomS3UploadBucket, config.CustomS3UploadRegion)
		add("custom s3", s3Client, err)
		if s3Client != nil {
			log.Infof("Successfully created custom s3 uploader")
		}

		// Azure emitter
	} else if config.CustomAzureBlobContainerName != "" && config.CustomAzureBlobUrl != "" {
		blobClient, err := NewCustomBlobClient(config.CustomAzureBlobContainerName, config.CustomAzureBlobUrl, config.CustomAzureTenantID,
			config.CustomAzureClientID, config.CustomAzureClientSecret)
		add("custom azure blob", blobClient, err)
		if blobClient != nil {
			log.Infof("Successfully created custom azure blob uploader")
		}
		// No env vars for any of the required configurations were set.
	} else {
		log.Errorf("No complete upload configurations were detected. Please ensure that you have set the required " +
			"environment variables for your upload type.")
	}
	return storageServices, problems
}

// newCldyUploader creates the upload directory and runs startup recovery (recovery.go), but does
// not start uploadLoop. now is the uploader's clock; nil means time.Now. Tests use it to drive
// upload cycles directly (uploadCycle) against fake storage services and a fake clock. events
// nil means a new EventCounts.
func newCldyUploader(config UploaderConfig, storageServices []StorageService, now func() time.Time, events EventSink) (*CldyUploader, error) {
	if events == nil {
		events = NewEventCounts()
	}
	uploadPathDir := config.ScratchDir + "/" + uploadPath
	err := createIfNotExists(uploadPathDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create the Cloudability upload directory: %w", err)
	}
	recoveryPeriod := config.RecoveryPeriod
	if recoveryPeriod <= 0 {
		log.Warnf("Cloudability recovery period not set; using the default %s", defaultRecoveryPeriod)
		recoveryPeriod = defaultRecoveryPeriod
	}
	backlogMaxBytes := config.BacklogMaxBytes
	if backlogMaxBytes <= 0 {
		backlogMaxBytes = defaultBacklogMaxBytes
	}

	uploader := &CldyUploader{
		config:        config,
		UploadPathDir: uploadPathDir,
		// TODO: dynamically pick client based upon upload config
		StorageServices: storageServices,
		recoveryPeriod:  recoveryPeriod,
		backlogMaxBytes: backlogMaxBytes,
		agentVersion:    version.Version,
		queue:           newDiskQueue(config.ScratchDir, events, now),
		events:          events,
		conditions:      newConditionStore(now),
		now:             now,
	}
	uploader.checkConfigured()
	err = uploader.recoverDataOnStartup()
	if err != nil {
		log.Errorf("Cloudability startup recovery was incomplete: %v", err)
	}
	if uploader.RecoveredUploads != 0 || uploader.RecoveredSamples != 0 {
		log.Infof("Cloudability successfully recovered %d samples and prepared %d uploads on startup",
			uploader.RecoveredSamples, uploader.RecoveredUploads)
	}
	return uploader, nil
}

// clock returns the current time from the injected clock, or time.Now when none is set.
func (cu *CldyUploader) clock() time.Time {
	if cu.now == nil {
		return time.Now()
	}
	return cu.now()
}

type UploaderConfig struct {
	ApptioConfig
	UploadFrequency time.Duration
	ScratchDir      string
	// RecoveryPeriod is how old pending data may be at startup and still be uploaded
	// (CLOUDABILITY_RECOVERY_PERIOD). Zero means defaultRecoveryPeriod. Queued data older than
	// this is also evicted at runtime.
	RecoveryPeriod time.Duration
	// BacklogMaxBytes caps the bytes queued for upload, samples and payloads together
	// (CLOUDABILITY_BACKLOG_MAX_MB). Zero means defaultBacklogMaxBytes.
	BacklogMaxBytes int64
}

// AddSample does nothing: the next upload cycle finds the sample on disk.
func (cu *CldyUploader) AddSample(string) {}

// SetClusterID sets the live cluster ID, which Emitter.Init reads from the snapshot. Startup
// recovery took each payload's cluster ID from its file name; payloads for any other cluster are
// quarantined and counted, never uploaded under this one (F-05).
func (cu *CldyUploader) SetClusterID(id string) {
	if id == "" {
		log.Errorf("refusing an empty Cloudability cluster ID")
		return
	}
	cu.mu.Lock()
	cu.clusterID = id
	cu.mu.Unlock()

	cu.queue.withLock(func() {
		payloads, _, err := cu.queue.payloads()
		if err != nil {
			log.Errorf("failed to list the Cloudability upload queue: %v", err)
			return
		}
		mismatched := false
		for _, p := range payloads {
			if p.clusterID == id || p.path == cu.queue.inFlight {
				continue
			}
			cu.quarantine(p.path, dropReasonClusterIDMismatch, fmt.Sprintf("payload for cluster %s, but this is cluster %s", p.clusterID, id))
			mismatched = true
		}
		if mismatched {
			cu.trimQuarantine()
		}
	})
}

// liveClusterID returns the cluster ID set by SetClusterID, or "" before it is known.
func (cu *CldyUploader) liveClusterID() string {
	cu.mu.RLock()
	defer cu.mu.RUnlock()
	return cu.clusterID
}

// uploadLoop runs an upload cycle every UploadFrequency. The first cycle comes after a random
// part of one interval, so that agents restarted together don't upload in step. It returns when
// ctx is done, after the cycle in flight.
func (cu *CldyUploader) uploadLoop(ctx context.Context) {
	cu.hbMu.Lock()
	cu.heartbeat.LoopStart = cu.clock()
	cu.hbMu.Unlock()
	interval := max(cu.config.UploadFrequency, time.Second)
	first := time.NewTimer(time.Duration(rand.Int64N(int64(interval))))
	defer first.Stop()
	select {
	case <-ctx.Done():
		return
	case <-first.C:
		cu.uploadCycle()
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cu.uploadCycle()
		}
	}
}

// uploadCycle is one tick of uploadLoop. It packages the finalised samples, applies the backlog
// bounds and uploads the queued payloads, oldest first. A packaging failure never stops the
// upload step, and the upload step runs whether or not there were new samples. Nothing is
// packaged or uploaded until the live cluster ID is known.
func (cu *CldyUploader) uploadCycle() {
	cu.hbMu.Lock()
	cu.heartbeat.LastCycleStart = cu.clock()
	cu.hbMu.Unlock()
	clusterID := cu.liveClusterID()
	failed := false
	defer func() {
		files, bytes := cu.queue.stats(clusterID)
		cu.hbMu.Lock()
		cu.heartbeat.LastCycleEnd = cu.clock()
		cu.heartbeat.BacklogFiles, cu.heartbeat.BacklogBytes = files, bytes
		if failed {
			cu.heartbeat.ConsecutiveFailures++
		} else {
			cu.heartbeat.ConsecutiveFailures = 0
		}
		hb := cu.heartbeat
		cu.hbMu.Unlock()
		cu.checkUploadsFailing(hb)
	}()

	cu.checkConfigured()
	if clusterID == "" {
		log.Warnf("Cloudability cluster ID is not known yet; not packaging or uploading samples")
		return
	}
	if err := cu.packageSamples(clusterID); err != nil {
		log.Errorf("failed to package Cloudability samples; they stay queued for the next cycle: %v", err)
	}
	cu.progress()
	cu.queue.enforceBounds(clusterID, cu.recoveryPeriod, cu.backlogMaxBytes)
	failed = !cu.uploadQueued(clusterID)
}

// checkConfigured raises uploader_unconfigured while there is no storage service. The queue is
// then kept, and only the disk budget and backlog bounds evict from it (as no_uploader).
func (cu *CldyUploader) checkConfigured() {
	none := len(cu.StorageServices) == 0
	cu.queue.noUploader.Store(none)
	cu.setCondition(conditionUploaderUnconfigured, none,
		"no Cloudability storage service is configured; samples are kept on disk and nothing is uploaded")
}

// ConstructPayload packages every finalised sample of the live cluster into one payload named
// for sampleTime and returns its path, or "" if there was nothing to package.
func (cu *CldyUploader) ConstructPayload(sampleTime time.Time) (string, error) {
	clusterID := cu.liveClusterID()
	if clusterID == "" {
		return "", errors.New("no cluster ID: refusing to build a payload")
	}
	cu.queue.mu.Lock()
	defer cu.queue.mu.Unlock()
	return cu.packageSamplesLocked(clusterID, sampleTime)
}

func (cu *CldyUploader) packageSamples(clusterID string) error {
	_, err := cu.ConstructPayload(cu.clock().UTC())
	return err
}

// packageSamplesLocked packages every finalised sample of clusterID into one payload named for
// ts. Each sample's files are checked against its manifest first; a sample that fails is
// quarantined (invalid_sample). The caller holds cu.queue.mu.
func (cu *CldyUploader) packageSamplesLocked(clusterID string, ts time.Time) (string, error) {
	clusterDir := cu.queue.clusterDir(clusterID)
	samples, invalid, err := listFinalisedSamples(clusterDir)
	if errors.Is(err, fs.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	for _, name := range invalid {
		cu.quarantine(filepath.Join(clusterDir, name), dropReasonInvalidSample, "not a valid finalised sample")
	}
	quarantined := len(invalid) > 0
	var paths []string
	var sampleBytes int64
	for _, s := range samples {
		// The last check before the files leave the sample: the hashes, not only the sizes.
		if _, err := validateSample(s.Path, true); err != nil {
			cu.quarantine(filepath.Clean(s.Path), dropReasonInvalidSample, fmt.Sprintf("not a valid finalised sample: %v", err))
			quarantined = true
			continue
		}
		paths = append(paths, s.Path)
		sampleBytes += s.Manifest.totalBytes()
	}
	if quarantined {
		cu.trimQuarantine()
	}
	if len(paths) == 0 {
		return "", nil
	}
	return cu.buildPayload(clusterID, ts, paths, sampleBytes)
}

// buildPayload packages samples into upload/<clusterID>_<YYYY-MM-DD-HH-MM-SS>.tgz for ts and
// returns its path. It first makes room for the payload (payloadRoom), evicting the oldest
// queued data; with no room even then, the samples stay queued. The payload is written to upload/.<name>.partial,
// closed and fsynced, then renamed into place and the directory fsynced (F-04). The samples are
// removed only after the rename; on any error before it the temporary file is removed and the
// samples are kept (F-50). A name already taken moves ts on by a second, so no payload is
// overwritten. The caller holds cu.queue.mu, or is startup recovery.
func (cu *CldyUploader) buildPayload(clusterID string, ts time.Time, samples []string, sampleBytes int64) (string, error) {
	if clusterID == "" {
		return "", errors.New("no cluster ID: refusing to build a payload")
	}
	need := cu.payloadRoom(sampleBytes)
	_, ok, err := cu.queue.makeRoomLocked(clusterID, need)
	if err != nil {
		log.Errorf("cannot read free space on the Cloudability scratch volume, not evicting anything: %v", err)
	} else if !ok {
		return "", fmt.Errorf("no room on the scratch volume for a payload of up to %d bytes, even after evicting the oldest queued data", need)
	}
	// Making room may have evicted the oldest of these samples.
	samples = existingPaths(samples)
	if len(samples) == 0 {
		return "", nil
	}

	final, partial, f, err := cu.createPayloadFile(clusterID, ts)
	if err != nil {
		return "", err
	}
	err = crashPoint(crashAfterCreate)
	if err == nil {
		err = cu.createTGZ(f, clusterID, samples)
	}
	if err == nil {
		err = f.Sync()
	}
	if cErr := f.Close(); err == nil {
		err = cErr
	}
	if err == nil {
		err = crashPoint(crashBeforeRename)
	}
	if err == nil {
		err = os.Rename(partial, final)
	}
	if err != nil {
		if rmErr := os.Remove(partial); rmErr != nil && !errors.Is(rmErr, fs.ErrNotExist) {
			log.Errorf("failed to remove unfinished Cloudability payload %s; it is discarded at the next start: %v", partial, rmErr)
		}
		return "", err
	}
	if err := crashPoint(crashAfterRename); err != nil {
		log.Debugf("crash point %s: %v", crashAfterRename, err)
	}
	if err := syncDir(cu.UploadPathDir); err != nil {
		log.Errorf("payload %s is written but syncing %s failed; it may not survive a node crash: %v", final, cu.UploadPathDir, err)
	}
	if info, err := os.Stat(final); err == nil && sampleBytes > 0 {
		cu.payloadRatio = float64(info.Size()) / float64(sampleBytes)
	}
	removeSamples(samples)
	return final, nil
}

// defaultPayloadRatio is the assumed payload size as a fraction of its samples' size before the
// first payload is built. JSON under gzip -9 is usually much smaller.
const defaultPayloadRatio = 0.25

// payloadRoom is the free space to make before packaging sampleBytes of samples: twice the size
// the last payload's compression ratio predicts. Asking for the samples' own size would evict
// far more than the payload needs; if the estimate is short, the write fails and the samples
// stay queued for the next cycle.
func (cu *CldyUploader) payloadRoom(sampleBytes int64) uint64 {
	ratio := cu.payloadRatio
	if ratio <= 0 {
		ratio = defaultPayloadRatio
	}
	return uint64(2 * ratio * float64(sampleBytes))
}

// existingPaths returns the paths that still exist.
func existingPaths(paths []string) []string {
	kept := paths[:0]
	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			kept = append(kept, p)
		}
	}
	return kept
}

// createPayloadFile creates the temporary file for the payload named for ts, or for the first
// later second whose name is free, and returns the final and temporary paths.
func (cu *CldyUploader) createPayloadFile(clusterID string, ts time.Time) (final, partial string, f *os.File, err error) {
	for i := range maxPayloadNameAttempts {
		name := payloadName(clusterID, ts.Add(time.Duration(i)*time.Second))
		final = SafePath(cu.UploadPathDir, name)
		if _, err := os.Lstat(final); err == nil {
			continue
		} else if !errors.Is(err, fs.ErrNotExist) {
			return "", "", nil, err
		}
		if _, err := os.Lstat(filepath.Join(cu.queue.deferredDir(), name)); err == nil {
			continue
		}
		partial = SafePath(cu.UploadPathDir, "."+name+partialSuffix)
		f, err = os.OpenFile(partial, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		if err != nil {
			return "", "", nil, err
		}
		if i > 0 {
			log.Infof("Cloudability payload name for %s was taken; using %s", ts.UTC().Format(payloadTimeFormat), name)
		}
		return final, partial, f, nil
	}
	return "", "", nil, fmt.Errorf("no free payload name within %d seconds of %s", maxPayloadNameAttempts, ts.UTC().Format(payloadTimeFormat))
}

// removeSamples removes samples that are packaged in a payload. Each is first renamed back to a
// staging name, so a kill part-way through the removal leaves a staging directory, which is
// discarded as unfinalised, not a torn sample that would be quarantined and counted as a drop.
// A sample that can't be renamed is packaged and uploaded again, which at-least-once delivery
// allows.
func removeSamples(samples []string) {
	for _, sample := range samples {
		dir := filepath.Clean(sample)
		doomed := filepath.Join(filepath.Dir(dir), stagingPrefix+filepath.Base(dir))
		if err := os.Rename(dir, doomed); err != nil {
			log.Errorf("failed to remove packaged Cloudability sample %s; it will be uploaded again: %v", sample, err)
			continue
		}
		if err := crashPoint(crashPackagedSampleRenamed); err != nil {
			continue
		}
		if err := os.RemoveAll(doomed); err != nil {
			log.Errorf("failed to remove packaged Cloudability sample %s; it is discarded later as unfinalised: %v", doomed, err)
		}
	}
}

// uploadQueued uploads the queued payloads in order (queue.go) and reports whether the cycle
// got through the queue without stopping on a failure. See classifyUpload for what each
// failure does.
func (cu *CldyUploader) uploadQueued(clusterID string) bool {
	if len(cu.StorageServices) == 0 {
		return true
	}
	payloads, invalid, err := cu.queue.payloads()
	if err != nil {
		log.Errorf("failed to list the Cloudability upload queue: %v", err)
		return false
	}
	quarantined := false
	defer func() {
		if quarantined {
			cu.queue.withLock(cu.trimQuarantine)
		}
	}()
	// rejected is the current run of consecutive rejections, not yet quarantined. Only a run
	// ended by a delivery is quarantined: that delivery shows the backend accepts payloads, so
	// the fault is in these. A run of maxConsecutiveRejections is taken to be the backend
	// refusing everything: it stops the cycle and nothing is quarantined. A run at the end of
	// the queue waits for the next cycle.
	var rejected []rejection
	quarantineRejected := func() {
		for _, r := range rejected {
			cu.queue.withLock(func() { cu.quarantine(r.path, dropReasonRejectedByBackend, r.detail) })
			quarantined = true
		}
		rejected = nil
	}
	for _, path := range invalid {
		cu.queue.withLock(func() { cu.quarantine(path, dropReasonInvalidPayload, "not a <clusterID>_<timestamp>.tgz payload") })
		quarantined = true
	}
	for _, p := range payloads {
		name := filepath.Base(p.path)
		if p.clusterID != clusterID {
			cu.queue.withLock(func() {
				cu.quarantine(p.path, dropReasonClusterIDMismatch, fmt.Sprintf("payload for cluster %s, but this is cluster %s", p.clusterID, clusterID))
			})
			quarantined = true
			continue
		}
		if !cu.queue.startUpload(p.path) {
			// Evicted, and counted, since the listing.
			log.Infof("Cloudability payload %s left the queue before its upload", name)
			continue
		}
		if err := verifyPayload(p.path); err != nil {
			corrupt := errors.Is(err, errCorruptPayload)
			cu.queue.finishUpload(func() {
				if corrupt {
					cu.quarantine(p.path, dropReasonCorruptPayload, err.Error())
				}
			})
			if corrupt {
				quarantined = true
				continue
			}
			log.Errorf("cannot read queued Cloudability payload %s; retrying next cycle: %v", name, err)
			return false
		}

		err := cu.uploadData(p.path, clusterID)
		cu.progress()
		cu.events.UploadAttempt(uploadResult(err))
		outcome := classifyUpload(err)
		cu.queue.finishUpload(func() {
			if outcome != uploadDelivered {
				return
			}
			if err := os.Remove(p.path); err != nil {
				log.Errorf("delivered Cloudability payload %s could not be removed; it will be uploaded again: %v", name, err)
			}
		})
		switch outcome {
		case uploadDelivered:
			quarantineRejected()
			cu.delivered()
		case uploadRejected:
			rejected = append(rejected, rejection{path: p.path, detail: err.Error()})
			if len(rejected) < maxConsecutiveRejections {
				continue
			}
			cu.setCondition(conditionUploadsRejected, true, fmt.Sprintf("the Cloudability backend rejected %d payloads in a row, so the "+
				"fault is probably not in the payloads; keeping them and retrying next cycle: %v", len(rejected), err))
			return false
		case uploadAuthFailed:
			cu.setCondition(conditionUploadAuthFailed, true, fmt.Sprintf("the Cloudability backend refused the agent's credentials; nothing is deleted: %v", err))
			return false
		default:
			log.Warnf("uploading Cloudability payload %s failed; the queue is retried from it next cycle: %v", name, err)
			if cu.headFailed(p, err) {
				quarantined = true // or deferred; either way the next payload is now the head
				continue
			}
			return false
		}
	}
	if len(rejected) > 0 {
		// Nothing was delivered after them, so there is no evidence yet that the backend
		// accepts anything: keep them for the next cycle.
		log.Warnf("the Cloudability backend rejected the last %d payloads in the queue; keeping them until another payload is accepted", len(rejected))
	}
	return true
}

// maxConsecutiveRejections is how many payloads in a row the backend may reject before the
// uploader treats the rejections as the backend's fault rather than the payloads'.
const maxConsecutiveRejections = 3

// rejection is a payload the backend rejected, waiting to be quarantined.
type rejection struct {
	path   string
	detail string
}

// progress records that the running upload cycle made progress, for UploadStalled.
func (cu *CldyUploader) progress() {
	cu.hbMu.Lock()
	cu.heartbeat.LastProgress = cu.clock()
	cu.hbMu.Unlock()
}

// delivered records a delivered payload.
func (cu *CldyUploader) delivered() {
	cu.hbMu.Lock()
	cu.heartbeat.LastSuccess = cu.clock()
	cu.hbMu.Unlock()
	cu.setCondition(conditionUploadAuthFailed, false, "the Cloudability backend accepted a payload")
	cu.setCondition(conditionUploadConnectivityFailed, false, "the Cloudability backend accepted a payload")
	cu.setCondition(conditionUploadsRejected, false, "the Cloudability backend accepted a payload")
}

// headFailed applies the head-of-line rule to a retryable failure of p, the head of the queue.
// It reports whether p left the head. A failure to log in has nothing to do with the payload,
// so it doesn't count.
func (cu *CldyUploader) headFailed(p queuedPayload, err error) bool {
	var uploadErr *UploadError
	if errors.As(err, &uploadErr) && uploadErr.Stage == UploadStageLogin {
		return false
	}
	now := cu.clock()
	if cu.head.path != p.path {
		cu.head = headOfLine{path: p.path, since: now}
	}
	cu.head.failures++
	if cu.head.failures < headMaxFailures || now.Sub(cu.head.since) <= headMaxStuck {
		return false
	}
	stuck := now.Sub(cu.head.since).Round(time.Second)
	cu.head = headOfLine{}
	name := filepath.Base(p.path)

	if !p.deferred {
		moved := false
		cu.queue.withLock(func() { moved = cu.deferPayload(p.path, now) })
		if moved {
			cu.events.HeadDeferred(1)
			log.Warnf("Cloudability payload %s failed at the head of the upload queue for %s; moved it behind the rest", name, stuck)
		}
		return moved
	}
	cu.hbMu.Lock()
	lastSuccess := cu.heartbeat.LastSuccess
	cu.hbMu.Unlock()
	if !lastSuccess.After(p.mod) {
		log.Warnf("Cloudability payload %s keeps failing at the head of the upload queue, but nothing was delivered since it was "+
			"moved behind the rest, so the backend may be down; keeping it", name)
		return false
	}
	cu.queue.withLock(func() {
		cu.quarantine(p.path, dropReasonUndeliverable, fmt.Sprintf("failed at the head of the upload queue for %s after being moved behind the rest once, "+
			"while other payloads were delivered: %v", stuck, err))
	})
	return true
}

// deferPayload moves a payload to upload/deferred/, stamped with when it was moved. The stamp
// comes first, so a deferred payload always carries it: the head-of-line rule quarantines one
// only after a delivery later than the stamp. The caller holds cu.queue.mu.
func (cu *CldyUploader) deferPayload(path string, now time.Time) bool {
	dest := filepath.Join(cu.queue.deferredDir(), filepath.Base(path))
	err := os.Chtimes(path, now, now)
	if err == nil {
		err = os.MkdirAll(cu.queue.deferredDir(), os.ModePerm)
	}
	if err == nil {
		err = os.Rename(path, dest)
	}
	if err != nil {
		log.Errorf("failed to move Cloudability payload %s behind the rest of the upload queue: %v", path, err)
		return false
	}
	return true
}

// uploadData uploads the payload at path to every storage service.
func (cu *CldyUploader) uploadData(path, clusterID string) error {
	fileName, hash, err := getFileNameAndHash(path)
	if err != nil {
		return err
	}
	payload := UploadPayload{
		ClusterUID:   clusterID,
		FileName:     fileName,
		AgentVersion: cu.agentVersion,
		UploadHash:   hash,
		FilePath:     path,
	}
	for _, service := range cu.StorageServices {
		if err := service.Upload(payload); err != nil {
			return err
		}
	}
	return crashPoint(crashAfterUpload)
}

// setCondition records one of the uploader's conditions, logging only when it changes.
func (cu *CldyUploader) setCondition(name string, active bool, msg string) {
	recordCondition(cu.events, cu.conditions, name, active, msg)
}

// uploadOutcome is what an upload attempt means for the payload and the rest of the cycle.
type uploadOutcome int

const (
	// uploadDelivered: every service accepted the payload. It is removed.
	uploadDelivered uploadOutcome = iota
	// uploadRetryable: stop the cycle and keep the order; the next tick retries.
	uploadRetryable
	// uploadAuthFailed: the backend refused the credentials. Stop the cycle and raise
	// upload_auth_failed.
	uploadAuthFailed
	// uploadRejected: the backend will never accept this payload. Quarantine it and carry on.
	uploadRejected
)

// classifyUpload decides what a StorageService.Upload error means:
//
//	no error                                       delivered
//	401 or 403 at login, presign or store          auth: the credentials were refused
//	403 on a presigned PUT                         retryable: the URL expired (the service has
//	                                               already presigned again once)
//	400 or 413 at presign or on a presigned PUT    rejected: this payload will never be accepted,
//	                                               unless S3 says the URL's token expired
//	                                               (retryable). uploadQueued quarantines a
//	                                               rejection only once another payload is
//	                                               accepted after it
//	413 at store                                   rejected
//	400 at store (S3, Azure)                       retryable: those use 400 for configuration
//	                                               faults such as a wrong region
//	anything else: other statuses (404, 408, 429,  retryable
//	5xx), any status at login other than 401/403,
//	timeouts, refused connections, DNS failures
//	and local errors
func classifyUpload(err error) uploadOutcome {
	if err == nil {
		return uploadDelivered
	}
	code, ok := uploadStatusCode(err)
	if !ok {
		return uploadRetryable
	}
	stage := ""
	if uploadErr, ok := errors.AsType[*UploadError](err); ok {
		stage = uploadErr.Stage
	}
	switch {
	case code == 401 || code == 403:
		if stage == UploadStagePresignedPut {
			return uploadRetryable
		}
		return uploadAuthFailed
	case stage == UploadStageLogin:
		return uploadRetryable
	case code == 400 && stage == UploadStagePresignedPut && s3TokenExpired(err):
		return uploadRetryable
	case code == 413 || (code == 400 && stage != UploadStageStore):
		return uploadRejected
	default:
		return uploadRetryable
	}
}

// s3TokenExpired reports whether a 400 from S3 on a presigned PUT says the credentials behind
// the URL expired, which is the backend's fault and passes, not the payload's.
func s3TokenExpired(err error) bool {
	msg := err.Error()
	for _, code := range []string{"ExpiredToken", "InvalidToken", "TokenRefreshRequired"} {
		if strings.Contains(msg, "<Code>"+code+"</Code>") {
			return true
		}
	}
	return false
}

// uploadResult is the upload_attempts_total result label for an attempt.
func uploadResult(err error) string {
	switch classifyUpload(err) {
	case uploadDelivered:
		return uploadResultOK
	case uploadAuthFailed:
		return uploadResultAuth
	case uploadRejected:
		return uploadResultRejected
	}
	var netErr net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &netErr) && netErr.Timeout()) {
		return uploadResultTimeout
	}
	return uploadResultRetryable
}

// UploadHeartbeat is the upload loop's progress, for readiness and /status (chunk 08) and
// metrics (chunk 09).
type UploadHeartbeat struct {
	// LoopStart is when the upload loop started; zero if it hasn't.
	LoopStart      time.Time `json:"loopStart"`
	LastCycleStart time.Time `json:"lastCycleStart"`
	LastCycleEnd   time.Time `json:"lastCycleEnd"`
	// LastProgress is when the running cycle last finished packaging or an upload attempt.
	LastProgress time.Time `json:"lastProgress"`
	// LastSuccess is when a payload was last delivered.
	LastSuccess time.Time `json:"lastSuccess"`
	// ConsecutiveFailures counts the cycles in a row that stopped on a failed upload.
	ConsecutiveFailures int `json:"consecutiveFailures"`
	// BacklogFiles and BacklogBytes are the queued payloads and samples at the end of the last
	// cycle.
	BacklogFiles int   `json:"backlogFiles"`
	BacklogBytes int64 `json:"backlogBytes"`
}

// UploadHeartbeatSource is implemented by *CldyUploader.
type UploadHeartbeatSource interface {
	UploadHeartbeat() UploadHeartbeat
}

// UploadHeartbeat returns the upload loop's progress. It is safe to call from any goroutine.
func (cu *CldyUploader) UploadHeartbeat() UploadHeartbeat {
	cu.hbMu.Lock()
	defer cu.hbMu.Unlock()
	return cu.heartbeat
}

// createTGZ writes the samples to writer as a gzipped tar. Each file is stored as
// <sample>/<clusterID>/<file>; MANIFEST.json is left out. The tar and gzip writers are closed,
// and their errors returned, before it returns.
func (cu *CldyUploader) createTGZ(writer io.Writer, clusterID string, srcs []string) (rerr error) {
	gzw, _ := gzip.NewWriterLevel(writer, flate.BestCompression)
	defer safeClose(gzw.Close, &rerr)
	tw := tar.NewWriter(gzw)
	defer safeClose(tw.Close, &rerr)
	for _, src := range srcs {
		// ensure the src actually exists before trying to tar it
		if _, err := os.Stat(src); err != nil {
			return fmt.Errorf("unable to tar files - %v", err.Error())
		}

		// walk path
		err := filepath.Walk(src, func(file string, fileInfo os.FileInfo, err error) (rerr error) {

			// return on any error
			if err != nil {
				return err
			}

			// create a new dir/file header
			header, err := tar.FileInfoHeader(fileInfo, fileInfo.Name())
			if err != nil {
				return err
			}

			// return on directories since there will be no content to tar
			if fileInfo.Mode().IsDir() {
				return nil
			}
			// the manifest is for the agent, not the backend: leave the tar layout unchanged
			if fileInfo.Name() == manifestFileName {
				return nil
			}

			// if not a directory update the name to correctly reflect the desired destination when untaring
			if !fileInfo.Mode().IsDir() {
				header.Name = filepath.Join(filepath.Base(src), clusterID, strings.TrimPrefix(file, src))
			}
			// write the header
			if err := tw.WriteHeader(header); err != nil {
				return err
			}
			if err := crashPoint(crashMidTar); err != nil {
				return err
			}

			// open files for taring
			//nolint gosec
			f, err := os.Open(file)
			if err != nil {
				return err
			}

			defer safeClose(f.Close, &rerr)

			// copy file data into tar writer
			if _, err := io.Copy(tw, f); err != nil {
				return err
			}

			return err
		})
		if err != nil {
			return err
		}
	}
	return nil
}
