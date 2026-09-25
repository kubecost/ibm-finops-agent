package cldy

import (
	"archive/tar"
	"compress/flate"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ibm/finops-agent/pkg/version"
	"github.com/opencost/opencost/core/pkg/log"
)

var ErrDiskSpaceExceeded = errors.New("upload directory cleaned and disk issue persists. omitting current upload")

type Uploader interface {
	AddSample(sample string)
	RemoveSample(sample string)
	SetClusterID(id string)
}

type CldyUploader struct {
	config           UploaderConfig
	mu               sync.RWMutex // guards clusterID
	sampleSet        *set
	uploadSet        *set
	stop             chan struct{}
	clusterID        string
	agentVersion     string
	UploadPathDir    string
	StorageServices  []StorageService
	RecoveredSamples int
	RecoveredUploads int
	recoveryPeriod   time.Duration
	lastUploadSize   uint64

	// events receives drops and discards from startup recovery and payload handling. The emitter
	// built around this uploader shares it.
	events EventSink

	// now is the uploader's clock. It is nil in production (see clock) and only set by tests.
	now func() time.Time
}

func NewCldyUploader(config UploaderConfig, stop chan struct{}) Uploader {
	uploader := newCldyUploader(config, newStorageServices(config), stop, nil, nil)
	go uploader.uploadLoop()
	return uploader
}

// newStorageServices builds the storage service selected by the upload configuration.
func newStorageServices(config UploaderConfig) []StorageService {
	var storageServices []StorageService

	// Legacy metrics-collector upload path (API key / API Gateway)
	if hasAPIKeyConfigured(config.APIKeySecretManager) {
		metricsCollectorService, err := NewMetricsCollectorService(config.ApptioConfig)
		if err != nil {
			log.Errorf("Failed to create metrics-collector uploader: %v", err)
		}
		if metricsCollectorService != nil {
			storageServices = append(storageServices, metricsCollectorService)
		}

		// Apptio Frontdoor upload path
	} else if config.EnvID != "" {
		apptioService, err := NewApptioService(config.ApptioConfig)
		if err != nil {
			log.Errorf("Failed to create cloudability uploader: %v", err)
		}
		if apptioService != nil {
			storageServices = append(storageServices, apptioService)
		}

		// S3 emitter
	} else if config.CustomS3UploadBucket != "" && config.CustomS3UploadRegion != "" {
		s3Client, err := NewCustomS3Client(config.CustomS3UploadBucket, config.CustomS3UploadRegion)
		if err != nil {
			log.Errorf("Failed to create custom s3 uploader: %v", err)
		}
		if s3Client != nil {
			log.Infof("Successfully created custom s3 uploader")
			storageServices = append(storageServices, s3Client)
		}

		// Azure emitter
	} else if config.CustomAzureBlobContainerName != "" && config.CustomAzureBlobUrl != "" {
		blobClient, err := NewCustomBlobClient(config.CustomAzureBlobContainerName, config.CustomAzureBlobUrl, config.CustomAzureTenantID,
			config.CustomAzureClientID, config.CustomAzureClientSecret)
		if err != nil {
			log.Errorf("Failed to create custom azure blob uploader: %v", err)
		}
		if blobClient != nil {
			log.Infof("Successfully created custom azure blob uploader")
			storageServices = append(storageServices, blobClient)
		}
		// No env vars for any of the required configurations were set.
	} else {
		log.Errorf("No complete upload configurations were detected. Please ensure that you have set the required " +
			"environment variables for your upload type.")
	}
	return storageServices
}

// newCldyUploader creates the upload directory and runs startup recovery (recovery.go), but does
// not start uploadLoop. now is the uploader's clock; nil means time.Now. Tests use it to drive
// upload cycles directly (uploadCycle) against fake storage services and a fake clock. events
// nil means a new EventCounts.
func newCldyUploader(config UploaderConfig, storageServices []StorageService, stop chan struct{}, now func() time.Time, events EventSink) *CldyUploader {
	if events == nil {
		events = NewEventCounts()
	}
	uploadPathDir := config.ScratchDir + "/" + uploadPath
	err := createIfNotExists(uploadPathDir)
	if err != nil {
		panic("failed to create upload directory: " + err.Error())
	}
	recoveryPeriod := config.RecoveryPeriod
	if recoveryPeriod <= 0 {
		log.Warnf("Cloudability recovery period not set; using the default %s", defaultRecoveryPeriod)
		recoveryPeriod = defaultRecoveryPeriod
	}

	uploader := &CldyUploader{
		config:        config,
		sampleSet:     newSet(),
		uploadSet:     newSet(),
		stop:          stop,
		UploadPathDir: uploadPathDir,
		// TODO: dynamically pick client based upon upload config
		StorageServices: storageServices,
		recoveryPeriod:  recoveryPeriod,
		agentVersion:    version.Version,
		events:          events,
		now:             now,
	}
	err = uploader.recoverDataOnStartup()
	if err != nil {
		log.Errorf("Cloudability startup recovery was incomplete: %v", err)
	}
	if uploader.RecoveredUploads != 0 || uploader.RecoveredSamples != 0 {
		log.Infof("Cloudability successfully recovered %d samples and prepared %d uploads on startup",
			uploader.RecoveredSamples, uploader.RecoveredUploads)
	}
	return uploader
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

func (cu *CldyUploader) AddSample(sample string) {
	cu.sampleSet.add(sample)
}

func (cu *CldyUploader) RemoveSample(sample string) {
	cu.sampleSet.remove(sample)
}

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

	mismatched := false
	for _, path := range cu.uploadSet.contents() {
		clusterID, _, ok := parsePayloadName(filepath.Base(path))
		if !ok || clusterID == id {
			continue
		}
		cu.uploadSet.remove(path)
		cu.quarantine(path, dropReasonClusterIDMismatch, fmt.Sprintf("payload for cluster %s, but this is cluster %s", clusterID, id))
		mismatched = true
	}
	if mismatched {
		cu.trimQuarantine()
	}
}

// liveClusterID returns the cluster ID set by SetClusterID, or "" before it is known.
func (cu *CldyUploader) liveClusterID() string {
	cu.mu.RLock()
	defer cu.mu.RUnlock()
	return cu.clusterID
}

func (cu *CldyUploader) uploadLoop() {
	ticker := time.Tick(cu.config.UploadFrequency)
	for {
		select {
		case <-cu.stop:
			return
		case <-ticker:
			cu.uploadCycle()
		}
	}
}

// uploadCycle packages the pending samples and uploads the queued payloads. It is one tick
// of uploadLoop. Nothing is packaged or uploaded until the live cluster ID is known.
func (cu *CldyUploader) uploadCycle() {
	if cu.sampleSet.length() == 0 {
		return
	}
	if cu.liveClusterID() == "" {
		log.Warnf("Cloudability cluster ID is not known yet; not packaging or uploading samples")
		return
	}
	path, err := cu.ConstructPayload(cu.clock().UTC())
	if err != nil {
		log.Warnf("did not construct cldy payload: %s", err)
		return
	}
	cu.uploadSet.add(path)
	err = cu.uploadSet.operateAndRemove(cu.uploadData)
	if err != nil {
		log.Warnf("error uploading: %s", err.Error())
	}
}

// ConstructPayload packages every queued sample into one payload for the live cluster ID, named
// for sampleTime, and returns its path. See buildPayload.
func (cu *CldyUploader) ConstructPayload(sampleTime time.Time) (string, error) {
	var samples []string
	for _, sample := range cu.sampleSet.contents() {
		if _, err := os.Stat(sample); errors.Is(err, fs.ErrNotExist) {
			// Disk-pressure eviction dequeues a sample before removing it and counts the drop, so
			// this is a sample removed from outside the agent.
			log.Errorf("queued Cloudability sample %s vanished before it was packaged; dequeuing it", sample)
			cu.sampleSet.remove(sample)
			continue
		}
		samples = append(samples, sample)
	}
	if len(samples) == 0 {
		return "", errors.New("no samples to package")
	}
	return cu.buildPayload(cu.liveClusterID(), sampleTime, samples)
}

// buildPayload packages samples into upload/<clusterID>_<YYYY-MM-DD-HH-MM-SS>.tgz for ts and
// returns its path. The payload is written to upload/.<name>.partial, closed and fsynced, then
// renamed into place and the directory fsynced (F-04). The samples are removed only after the
// rename; on any error before it the temporary file is removed and the samples are kept, still
// queued (F-50). A name already taken moves ts on by a second, so no payload is overwritten.
func (cu *CldyUploader) buildPayload(clusterID string, ts time.Time, samples []string) (string, error) {
	if clusterID == "" {
		return "", errors.New("no cluster ID: refusing to build a payload")
	}
	if err := cu.ensureUploadSpace(); err != nil {
		if errors.Is(err, ErrDiskSpaceExceeded) {
			cu.removeSamples(samples)
			dropData(cu.events, dropReasonDiskPressure, len(samples),
				fmt.Sprintf("no room on the scratch volume to package %d samples, even after removing old payloads", len(samples)))
		}
		return "", err
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
	cu.removeSamples(samples)
	return final, nil
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

// removeSamples dequeues and removes samples that are packaged in a payload. A sample that can't
// be removed is uploaded again after a restart, which at-least-once delivery allows.
func (cu *CldyUploader) removeSamples(samples []string) {
	for _, sample := range samples {
		cu.sampleSet.remove(sample)
		if err := os.RemoveAll(sample); err != nil {
			log.Errorf("failed to remove packaged Cloudability sample %s; it will be uploaded again after a restart: %v", sample, err)
		}
	}
}

// ensureUploadSpace checks there is room for a payload twice the size of the last one. If not
// it removes old payloads (ClearOldUploadSamples) and returns ErrDiskSpaceExceeded if there is
// still no room. A statfs failure is not pressure: nothing is removed (F-49).
func (cu *CldyUploader) ensureUploadSpace() error {
	need := cu.lastUploadSize * 2
	avail, err := diskAvailable(cu.UploadPathDir)
	if err != nil {
		log.Errorf("cannot read free space on the Cloudability scratch volume, not removing anything: %v", err)
		return nil
	}
	if avail >= need {
		return nil
	}
	if err := cu.ClearOldUploadSamples(); err != nil {
		return err
	}
	if avail, err := diskAvailable(cu.UploadPathDir); err == nil && avail < need {
		return ErrDiskSpaceExceeded
	}
	return nil
}

func (cu *CldyUploader) uploadData(path string) error {
	if err := verifyPayload(path); err != nil {
		if !errors.Is(err, errCorruptPayload) {
			return err
		}
		// Dequeued by returning nil: the payload is no longer in upload/.
		cu.quarantine(path, dropReasonCorruptPayload, err.Error())
		cu.trimQuarantine()
		return nil
	}
	fileName, hash, err := getFileNameAndHash(path)
	if err != nil {
		return err
	}
	payload := UploadPayload{
		ClusterUID:   cu.liveClusterID(),
		FileName:     fileName,
		AgentVersion: cu.agentVersion,
		UploadHash:   hash,
		FilePath:     path,
	}

	for _, service := range cu.StorageServices {
		err = service.Upload(payload)
		if err != nil {
			return err
		}
	}
	if err := crashPoint(crashAfterUpload); err != nil {
		return err
	}

	// retain size of file before removal for disk calculation purposes
	f, err := os.Stat(path)
	if err != nil {
		return err
	}
	cu.lastUploadSize = uint64(f.Size())

	// uploads data, then removes tar from path if successful
	return os.Remove(path)
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

// ClearOldUploadSamples removes payloads older than half the recovery period to make room on the
// scratch volume. Each removal is a counted drop and dequeues the payload.
func (cu *CldyUploader) ClearOldUploadSamples() error {
	log.Infof("Disk space threshold met. Attempting to clean uploads older than %s.", cu.recoveryPeriod/2)

	files, err := os.ReadDir(cu.UploadPathDir)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() || isPartialPayload(file.Name()) {
			continue
		}
		filePath := filepath.Join(cu.UploadPathDir, file.Name())
		fileInfo, err := file.Info()
		if err != nil {
			log.Warnf("problem retrieving file information: %s", err)
			continue
		}
		if cu.clock().Sub(fileInfo.ModTime()) <= cu.recoveryPeriod/2 {
			continue
		}
		if err := os.Remove(filePath); err != nil {
			log.Errorf("problem deleting file: %s", err)
			continue
		}
		cu.uploadSet.remove(filePath)
		dropData(cu.events, dropReasonDiskPressure, 1,
			fmt.Sprintf("removed payload %s, older than half the recovery period, to make room on the scratch volume", file.Name()))
	}

	return nil
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

// classifyUpload decides what a StorageService.Upload error means.
func classifyUpload(err error) uploadOutcome {
	if err == nil {
		return uploadDelivered
	}
	return uploadRetryable
}

// uploadResult is the upload_attempts_total result label for an attempt.
func uploadResult(err error) string {
	if err == nil {
		return uploadResultOK
	}
	return uploadResultRetryable
}

// UploadHeartbeat is the upload loop's progress, for readiness and /status (chunk 08) and
// metrics (chunk 09).
type UploadHeartbeat struct {
	LastCycleStart      time.Time
	LastCycleEnd        time.Time
	LastSuccess         time.Time
	ConsecutiveFailures int
	BacklogFiles        int
	BacklogBytes        int64
}

// UploadHeartbeatSource is implemented by *CldyUploader.
type UploadHeartbeatSource interface {
	UploadHeartbeat() UploadHeartbeat
}

// UploadHeartbeat returns the upload loop's progress. It is safe to call from any goroutine.
func (cu *CldyUploader) UploadHeartbeat() UploadHeartbeat {
	return UploadHeartbeat{}
}
