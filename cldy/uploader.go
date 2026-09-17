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
	"github.com/opencost/opencost/core/pkg/util/json"
)

var requiredFiles = []string{"baseline-summary", "stats-summary", "statefulsets", "services", "replicationcontrollers", "replicasets", "pods", "persistentvolumes", "persistentvolumeclaims", "nodes", "namespaces", "jobs", "deployments", "daemonsets", "agent-measurement"}

var ErrDiskSpaceExceeded = errors.New("upload directory cleaned and disk issue persists. omitting current upload")

var ErrNoStorageServices = errors.New("no cloudability upload services are available, retaining sample")

type Uploader interface {
	AddSample(sample string)
	RemoveSample(sample string)
	SetClusterID(id string)
}

type CldyUploader struct {
	config          UploaderConfig
	sampleSet       *set
	uploadSet       *set
	stop            chan struct{}
	clusterID       string
	agentVersion    string
	UploadPathDir   string
	StorageServices []StorageService
	// servicesMutex guards StorageServices and lastServiceBuild, which are written from the
	// upload goroutine whenever construction is re-attempted
	servicesMutex    sync.RWMutex
	lastServiceBuild time.Time
	RecoveredSamples int
	RecoveredUploads int
	recoveryPeriod   time.Duration
	lastUploadSize   uint64
}

func NewCldyUploader(config UploaderConfig, stop chan struct{}) Uploader {
	uploadPathDir := config.ScratchDir + "/" + uploadPath
	err := createIfNotExists(uploadPathDir)
	if err != nil {
		panic("failed to create upload directory: " + err.Error())
	}

	uploader := CldyUploader{
		config:        config,
		sampleSet:     newSet(),
		uploadSet:     newSet(),
		stop:          stop,
		UploadPathDir: uploadPathDir,
		// TODO: dynamically pick client based upon upload config
		recoveryPeriod: config.RecoveryPeriod,
		agentVersion:   version.Version,
	}
	// build the configured storage services. failures here are not fatal, the upload path
	// re-attempts construction so a transient failure does not disable uploads for good
	uploader.ensureStorageServices()

	err = uploader.recoverDataOnStartup()
	if err != nil {
		log.Warnf("failed to recover historic samples on startup: %v", err)
	}
	if uploader.RecoveredUploads != 0 || uploader.RecoveredSamples != 0 {
		log.Infof("Cloudability successfully recovered %d samples and prepared %d uploads on startup",
			uploader.RecoveredSamples, uploader.RecoveredUploads)
	}

	go uploader.uploadLoop()
	return &uploader
}

type UploaderConfig struct {
	ApptioConfig
	UploadFrequency time.Duration
	ScratchDir      string
	RecoveryPeriod  time.Duration
}

// buildStorageServices creates the storage services described by config. It is used both on
// startup and by the upload path, so a transient failure (network blip, temporarily
// unavailable secret) can be recovered from rather than disabling uploads permanently.
func buildStorageServices(config UploaderConfig) []StorageService {
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

// ensureStorageServices returns the storage services, rebuilding them when there are none. A
// rebuild is allowed once UploadFrequency has passed since the previous build finished, so a
// broken configuration cannot hammer the upload endpoints.
func (cu *CldyUploader) ensureStorageServices() []StorageService {
	cu.servicesMutex.RLock()
	if len(cu.StorageServices) > 0 {
		services := cu.StorageServices
		cu.servicesMutex.RUnlock()
		return services
	}
	cu.servicesMutex.RUnlock()

	cu.servicesMutex.Lock()
	defer cu.servicesMutex.Unlock()
	// re-check now that the write lock is held, another caller may have built them
	if len(cu.StorageServices) > 0 {
		return cu.StorageServices
	}
	retry := !cu.lastServiceBuild.IsZero()
	if retry && time.Since(cu.lastServiceBuild) < cu.config.UploadFrequency {
		return nil
	}
	if retry {
		log.Infof("Re-attempting creation of cloudability upload services")
	}
	cu.StorageServices = buildStorageServices(cu.config)
	// stamp when the build finished: a build can outlast UploadFrequency, and a start-stamped
	// cooldown would already have expired by the time it returns
	cu.lastServiceBuild = time.Now()
	if retry && len(cu.StorageServices) > 0 {
		log.Infof("Successfully created %d cloudability upload service(s) on retry", len(cu.StorageServices))
	}
	return cu.StorageServices
}

func (cu *CldyUploader) AddSample(sample string) {
	cu.sampleSet.add(sample)
}

func (cu *CldyUploader) RemoveSample(sample string) {
	cu.sampleSet.remove(sample)
}

func (cu *CldyUploader) SetClusterID(id string) {
	cu.clusterID = id
}

func (cu *CldyUploader) recoverDataOnStartup() error {
	err := cu.recoverCompleteSamples()
	err = errors.Join(err, cu.recoverUploadFiles())
	if err != nil {
		return fmt.Errorf("error(s) occurred attempting to recover data on startup. errors: %w", err)
	}
	return nil
}

func (cu *CldyUploader) recoverCompleteSamples() error {
	var currentDir string
	var sampleTime time.Time
	first := true
	hasShipped := false
	filesNeeded := getNeededFiles()
	err := filepath.WalkDir(cu.config.ScratchDir+"/"+scratchPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		// skip first Walk (top level directory /scratch)
		if first {
			first = false
			return nil
		}

		if !d.IsDir() {
			// file found, gather timestamp from agent measure and remove from filesNeeded
			if strings.Contains(path, "agent-measurement") {
				sampleTime, err = collectSampleTime(path)
				if err != nil {
					return err
				}
			}
			for requiredFile := range filesNeeded {
				if strings.Contains(path, requiredFile) {
					delete(filesNeeded, requiredFile)
					break
				}
			}
		}
		// this directory has a complete sample and should be added
		if len(filesNeeded) == 0 && !hasShipped {
			hasShipped = true
			err = cu.recoverSample(currentDir, sampleTime)
			if err != nil {
				return err
			}
			return nil
		}

		if d.IsDir() {
			dir := path
			// on first pass, set currentDir to first found
			if currentDir == "" {
				currentDir = dir
			}
			// if dir changes, new sample set found, reset required files and delete previous from scratch
			if currentDir != dir {
				err = os.RemoveAll(currentDir)
				if err != nil {
					return err
				}
				filesNeeded = getNeededFiles()
				currentDir = dir
				hasShipped = false
			}
		}
		return nil
	})
	// last sample was incomplete, need to remove
	if !hasShipped {
		err = os.RemoveAll(currentDir)
		if err != nil {
			return err
		}
	}
	return err
}

// recover sample adds a completed sample to the set and constructs the upload file, construct handles
// scratch dir clean up, the sample will be uploaded in first upload loop of the agent
func (cu *CldyUploader) recoverSample(dir string, sampleTime time.Time) error {
	cu.RecoveredSamples++
	cu.AddSample(dir)
	_, err := cu.ConstructPayload(sampleTime)
	if err != nil {
		return fmt.Errorf("failed to construct sample payload: %v", err)
	}
	return nil
}

func collectSampleTime(path string) (t time.Time, rerr error) {
	file, err := os.Open(path)
	if err != nil {
		return time.Time{}, err
	}
	defer safeClose(file.Close, &rerr)
	bytes, err := io.ReadAll(file)
	if err != nil {
		return time.Time{}, err
	}
	var agentMeasure agentMeasurement
	err = json.Unmarshal(bytes, &agentMeasure)
	if err != nil {
		return time.Time{}, err
	}
	if agentMeasure.Timestamp == 0 {
		return time.Time{}, fmt.Errorf("agent-measurement timestamp is missing")
	}
	return time.Unix(agentMeasure.Timestamp, 0), nil
}

type agentMeasurement struct {
	Timestamp int64 `json:"ts"`
}

func (cu *CldyUploader) recoverUploadFiles() error {
	err := filepath.WalkDir(cu.UploadPathDir, func(path string, d fs.DirEntry, err error) error {
		if !d.IsDir() {
			parts := strings.Split(path, "_")
			if len(parts) == 0 {
				return fmt.Errorf("invalid path: %s", path)
			}
			date, dErr := time.Parse("2006-01-02-15-04-05", strings.TrimSuffix(parts[len(parts)-1], ".tgz"))
			if dErr != nil {
				return dErr
			}
			// remove and do not upload samples older than recovery Period
			if time.Since(date.UTC()).Hours() > cu.recoveryPeriod.Hours() {
				log.Infof("Cloudability sample is outside of recovery range, removing sample")
				return os.Remove(path)
			}
			// add to uploadSet for future shipping & clean up will occur during next upload
			cu.uploadSet.add(path)
			cu.RecoveredUploads++
		}
		return nil
	})
	return err
}

func getNeededFiles() map[string]struct{} {
	filesNeeded := map[string]struct{}{}
	for _, name := range requiredFiles {
		filesNeeded[name] = struct{}{}
	}
	return filesNeeded
}

// PendingUploads returns the paths of the tars currently queued for upload.
func (cu *CldyUploader) PendingUploads() []string {
	return cu.uploadSet.contents()
}

// drainBudgetDivisor is the fraction of an upload cycle a single drain may spend, leaving room
// in the tick for ConstructPayload and a possible storage service rebuild. The budget is checked
// between entries, not during one, so a pass costs up to budget + one in-flight upload.
const drainBudgetDivisor = 2

// drainBudget is the wall clock a single DrainUploads pass may spend. A non-positive
// UploadFrequency (no tick to overrun) means no bound.
func (cu *CldyUploader) drainBudget() time.Duration {
	if cu.config.UploadFrequency <= 0 {
		return 0
	}
	return cu.config.UploadFrequency / drainBudgetDivisor
}

// destinationUnavailable reports whether err means the destination as a whole is unusable, so
// the rest of the batch would fail identically. Per-tar failures must never qualify, or one bad
// entry starves everything behind it.
func destinationUnavailable(err error) bool {
	return errors.Is(err, ErrNoStorageServices)
}

// DrainUploads ships the queued tars, dropping each one that is shipped or no longer exists from
// the queue; failed and unreached tars stay queued. The pass starts no new entry once drainBudget
// is spent and stops once the destination is known to be unavailable: unbounded, a pass costs one
// full upload attempt per queued tar, and overrunning the tick drops ticks and stalls the loop.
func (cu *CldyUploader) DrainUploads() error {
	return cu.uploadSet.operateAndRemove(cu.UploadData, cu.drainBudget(), destinationUnavailable)
}

func (cu *CldyUploader) uploadLoop() {
	ticker := time.Tick(cu.config.UploadFrequency)
	for {
		select {
		case <-cu.stop:
			return
		case <-ticker:
			if cu.sampleSet.length() == 0 {
				continue
			}
			path, err := cu.ConstructPayload(time.Now().UTC())
			if err != nil {
				log.Warnf("did not construct cldy payload: %s", err)
				continue
			}
			cu.uploadSet.add(path)
			err = cu.DrainUploads()
			if err != nil {
				log.Warnf("error uploading: %s", err.Error())
			}
		}
	}
}

func (cu *CldyUploader) ConstructPayload(sampleTime time.Time) (path string, rerr error) {
	files := make([]*os.File, 0)
	for _, samplePath := range cu.sampleSet.contents() {
		file, err := os.Open(SafePath(samplePath))
		if err != nil {
			return "", err
		}
		files = append(files, file)
	}
	defer safeCloseFiles(files, &rerr)

	path = SafePath(
		cu.UploadPathDir,
		fmt.Sprintf(
			"%s_%s.tgz",
			cu.clusterID,
			sampleTime.Format("2006-01-02-15-04-05"),
		),
	)
	tw, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer safeClose(tw.Close, &rerr)
	err = cu.createTGZ(tw, files...)
	if err != nil && !errors.Is(err, ErrDiskSpaceExceeded) {
		return "", err
	}
	// if disk is maxed and cleaning doesn't help, delete sample set and problematic tar before returning error
	if err != nil && errors.Is(err, ErrDiskSpaceExceeded) {
		sErr := os.RemoveAll(path)
		if sErr != nil {
			log.Warnf("failed to remove problematic tar: %s", sErr)
		}
		sErr = cu.removeSamples(files)
		if sErr != nil {
			log.Warnf("failed to remove samples: %s", sErr)
		}
		return "", err
	}

	err = cu.removeSamples(files)
	if err != nil {
		return "", err
	}

	return path, nil
}

func (cu *CldyUploader) removeSamples(files []*os.File) error {
	for _, file := range files {
		cu.sampleSet.remove(file.Name())
		err := os.RemoveAll(file.Name())
		// TODO: eval this case, shouldn't happen
		if err != nil {
			return err
		}
	}

	return nil
}

// UploadData ships the tar at path to every configured storage service and removes it once
// every upload has succeeded. The tar is retained if there is nothing to upload it with, or
// if any upload fails, so that a later cycle can ship it.
func (cu *CldyUploader) UploadData(path string) error {
	services := cu.ensureStorageServices()
	if len(services) == 0 {
		log.Errorf("No cloudability upload services are available, retaining sample %s for a later upload attempt", path)
		return fmt.Errorf("%w: %s", ErrNoStorageServices, path)
	}

	fileName, hash, err := getFileNameAndHash(path)
	if err != nil {
		// The tar is gone: reclaimed by ClearOldUploadSamples under disk pressure, or removed
		// from outside the agent. There is nothing left to upload and nothing a
		// later cycle could do differently, so report success and let the caller drop the
		// entry instead of failing on it forever.
		if errors.Is(err, fs.ErrNotExist) {
			log.Warnf("Cloudability upload %s no longer exists, dropping it from the upload queue", path)
			return nil
		}
		return err
	}
	payload := UploadPayload{
		ClusterUID:   cu.clusterID,
		FileName:     fileName,
		AgentVersion: cu.agentVersion,
		UploadHash:   hash,
		FilePath:     path,
	}

	for _, service := range services {
		err = service.Upload(payload)
		if err != nil {
			return err
		}
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

// createTGZ takes a source and variable writers and walks 'source' writing each file
// found to the tar writer; the purpose for accepting multiple writers is to allow
// for multiple outputs
func (cu *CldyUploader) createTGZ(writer io.Writer, srcs ...*os.File) (rerr error) {
	// create a buffer of double the last upload size
	if !IsAvailableDiskSpace(cu.lastUploadSize*2, cu.UploadPathDir) {
		err := cu.ClearOldUploadSamples()
		if err != nil {
			return err
		}

		// Omit current sample if cleaning upload directory does not work
		if !IsAvailableDiskSpace(cu.lastUploadSize*2, cu.UploadPathDir) {
			return ErrDiskSpaceExceeded
		}
	}

	gzw, _ := gzip.NewWriterLevel(writer, flate.BestCompression)
	defer safeClose(gzw.Close, &rerr)
	tw := tar.NewWriter(gzw)
	defer safeClose(tw.Close, &rerr)
	for _, src := range srcs {
		// ensure the src actually exists before trying to tar it
		if _, err := os.Stat(src.Name()); err != nil {
			return fmt.Errorf("unable to tar files - %v", err.Error())
		}

		// walk path
		err := filepath.Walk(src.Name(), func(file string, fileInfo os.FileInfo, err error) (rerr error) {

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

			// if not a directory update the name to correctly reflect the desired destination when untaring
			if !fileInfo.Mode().IsDir() {
				header.Name = filepath.Join(filepath.Base(src.Name()), cu.clusterID, strings.TrimPrefix(file, src.Name()))
			}
			// write the header
			if err := tw.WriteHeader(header); err != nil {
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

func (cu *CldyUploader) ClearOldUploadSamples() error {
	log.Infof("Disk space threshold met. Attempting to clean uploads over recovery period.")

	files, err := os.ReadDir(cu.UploadPathDir)
	if err != nil {
		return err
	}

	for _, file := range files {
		filePath := filepath.Join(cu.UploadPathDir, file.Name())
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			log.Warnf("problem retrieving file information: %s", err)
			continue
		}

		if time.Since(fileInfo.ModTime()) > cu.recoveryPeriod/2 {
			err := os.RemoveAll(filePath)
			if err != nil {
				log.Warnf("problem deleting file: %s", err)
				continue
			}
			// the tar is gone, so the queue entry pointing at it has to go too. Leaving it
			// behind manufactures an entry whose file can never be opened again.
			cu.uploadSet.remove(filePath)
		}
	}

	return nil
}
