package cldy_test

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/ibm/finops-agent/cldy"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const goodFileName = "8604469a-1368-44ee-9f1c-c5cc8c2121c1_2025-05-05-18-05-17.tgz"

var _ = Describe("Uploader", func() {
	var tempDir string
	BeforeEach(func() {
		var err error
		tempDir, err = os.MkdirTemp("", "")
		Expect(err).ToNot(HaveOccurred())
		err = os.Mkdir(tempDir+"/scratch", os.ModePerm)
		Expect(err).ToNot(HaveOccurred())
	})
	AfterEach(func() {
		err := os.RemoveAll(tempDir)
		Expect(err).ToNot(HaveOccurred())
	})
	Context("TestBuildTar", func() {
		It("should build Tar", func() {
			config := defaultConfig(tempDir)
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)

			err := copyCompleteData(tempDir+"/scratch/temp_test_data", "testdata")
			Expect(err).ToNot(HaveOccurred())

			uploader.SetClusterID("test_id")
			uploader.AddSample(tempDir + "/scratch/temp_test_data")
			actualUploader := uploader.(*cldy.CldyUploader)
			path, err := actualUploader.ConstructPayload(time.Now())
			Expect(err).ToNot(HaveOccurred())
			fileInfo, err := os.Stat(path)
			Expect(err).ToNot(HaveOccurred())
			Expect(fileInfo.Size()).To(BeNumerically(">", 0))
		})
		It("should clean old tars on exceeded disk", func() {
			config := defaultConfig(tempDir)
			config.RecoveryPeriod = time.Hour

			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			uploader.SetClusterID("test_id")
			actualUploader := uploader.(*cldy.CldyUploader)

			// create existing tar in upload path
			_, err := os.Create(actualUploader.UploadPathDir + "/test.tgz")
			Expect(err).ToNot(HaveOccurred())

			// check number of files in upload path
			files, err := os.ReadDir(actualUploader.UploadPathDir)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(files)).To(BeNumerically("==", 1))

			// do not remove upload since it is recent
			err = actualUploader.ClearOldUploadSamples()
			Expect(err).ToNot(HaveOccurred())
			files, err = os.ReadDir(actualUploader.UploadPathDir)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(files)).To(BeNumerically("==", 1))

			// change file mod time to be very old
			filePath := filepath.Join(actualUploader.UploadPathDir, files[0].Name())
			err = os.Chtimes(filePath, time.Now(), time.Date(1, 1, 1, 1, 1, 1, 1, time.Local))
			Expect(err).ToNot(HaveOccurred())

			// purge old upload
			err = actualUploader.ClearOldUploadSamples()
			Expect(err).ToNot(HaveOccurred())

			// check there are no files in the upload path
			files, err = os.ReadDir(actualUploader.UploadPathDir)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(files)).To(BeNumerically("==", 0))
		})
	})
	Context("TestTarCleanup", func() {
		It("should remove the tar from disk and from the queue once it has been uploaded", func() {
			config := defaultConfig(tempDir)
			config.UploadFrequency = 100 * time.Millisecond
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)

			err := copyCompleteData(tempDir+"/scratch/temp_test_data", "testdata")
			Expect(err).ToNot(HaveOccurred())

			uploader.SetClusterID("test_id")
			actualUploader := uploader.(*cldy.CldyUploader)
			service := &mockStorageService{}
			actualUploader.StorageServices = []cldy.StorageService{service}
			uploader.AddSample(tempDir + "/scratch/temp_test_data")

			Eventually(service.uploaded, 5*time.Second, 10*time.Millisecond).Should(HaveLen(1))
			Expect(service.uploaded()[0].FilePath).To(HavePrefix(actualUploader.UploadPathDir))
			Eventually(func() ([]os.DirEntry, error) {
				return os.ReadDir(actualUploader.UploadPathDir)
			}, 5*time.Second, 10*time.Millisecond).Should(BeEmpty())
			Expect(actualUploader.PendingUploads()).To(BeEmpty())
		})
	})
	Context("TestStartupRecovery", func() {
		It("should recover complete sample", func() {
			config := defaultConfig(tempDir)
			config.RecoveryPeriod = 100000 * time.Hour
			// copy over data before creating uploader simulating recovery state
			err := copyCompleteData(tempDir+"/scratch/temp_test_data", "testdata")
			Expect(err).ToNot(HaveOccurred())
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			uploader.SetClusterID("123456-1234-1234-123456789012")
			actualUploader := uploader.(*cldy.CldyUploader)
			Expect(actualUploader.RecoveredSamples).To(Equal(1))
			Expect(actualUploader.RecoveredUploads).To(Equal(1))
			checkScratchEmpty(tempDir + "/scratch")

			// copy over another sample and ensure recovery does not break happy path
			checkCollectionAndConstruction(tempDir, uploader, actualUploader)
		})
		It("should recover sample but not upload when outside recovery range", func() {
			config := defaultConfig(tempDir)
			// 1 hour (will not recover as agent-measurement timestamp is old)
			config.RecoveryPeriod = 1 * time.Hour

			// copy over data before creating uploader simulating recovery state
			err := copyCompleteData(tempDir+"/scratch/temp_test_data", "testdata")
			Expect(err).ToNot(HaveOccurred())
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			uploader.SetClusterID("123456-1234-1234-123456789012")
			actualUploader := uploader.(*cldy.CldyUploader)
			Expect(actualUploader.RecoveredSamples).To(Equal(1))
			Expect(actualUploader.RecoveredUploads).To(Equal(0))
			checkScratchEmpty(tempDir + "/scratch")

			// copy over another sample and ensure recovery does not break happy path
			checkCollectionAndConstruction(tempDir, uploader, actualUploader)
		})
		It("should not recover incomplete sample", func() {
			config := defaultConfig(tempDir)
			// 100 years (should recover all samples if complete)
			config.RecoveryPeriod = 1000000 * time.Hour

			// copy over data before creating uploader simulating recovery state
			err := copyIncompleteData(tempDir+"/scratch/temp_test_data", "testdata", []string{"deployments.jsonl"})
			Expect(err).ToNot(HaveOccurred())
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			uploader.SetClusterID("123456-1234-1234-123456789012")
			actualUploader := uploader.(*cldy.CldyUploader)
			Expect(actualUploader.RecoveredSamples).To(Equal(0))
			Expect(actualUploader.RecoveredUploads).To(Equal(0))
			checkScratchEmpty(tempDir + "/scratch")

			// copy over another sample and ensure recovery does not break happy path
			checkCollectionAndConstruction(tempDir, uploader, actualUploader)
		})
		It("should recover multiple complete samples and ignore 1 incomplete sample", func() {
			config := defaultConfig(tempDir)
			// 100 years (should recover all samples)
			config.RecoveryPeriod = 1000000 * time.Hour

			// copy over data before creating uploader simulating recovery state
			err := copyCompleteData(tempDir+"/scratch/temp_test_data", "testdata")
			Expect(err).ToNot(HaveOccurred())
			// still valid test data, just with only 1 node file
			err = copyIncompleteData(tempDir+"/scratch/temp_test_data_1", "testdata", []string{"stats-summary-nodename2.json", "stats-summary-nodename3.json", "stats-summary-nodename4.json"})
			Expect(err).ToNot(HaveOccurred())
			err = updateAgentTimestamp(tempDir+"/scratch/temp_test_data_1/agent-measurement.json", 1743499000)
			Expect(err).ToNot(HaveOccurred())
			// invalid data set
			err = copyIncompleteData(tempDir+"/scratch/temp_test_data_2", "testdata", []string{"deployments.jsonl"})
			Expect(err).ToNot(HaveOccurred())
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			uploader.SetClusterID("123456-1234-1234-123456789012")
			actualUploader := uploader.(*cldy.CldyUploader)
			Expect(actualUploader.RecoveredSamples).To(Equal(2))
			Expect(actualUploader.RecoveredUploads).To(Equal(2))
			checkScratchEmpty(tempDir + "/scratch")

			// copy over another sample and ensure recovery does not break happy path
			checkCollectionAndConstruction(tempDir, uploader, actualUploader)
		})
	})
	Context("TestUpload", func() {
		It("should upload", func() {
			config := defaultConfig(tempDir)
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			err := os.CopyFS(tempDir+"/scratch/temp_test_data", os.DirFS("testdata"))
			Expect(err).ToNot(HaveOccurred())
			uploader.SetClusterID("test_id")
			actualUploader := uploader.(*cldy.CldyUploader)
			service := cldy.ApptioServiceImpl{
				CldyUploadClient: &mockClientService{},
				SecretManager:    cldy.NewKeyValueSecretManager("bad-key", ""),
			}
			actualUploader.StorageServices = []cldy.StorageService{&service}
			payload := cldy.UploadPayload{
				ClusterUID:   "bad-cluster",
				FileName:     "temp_test_data",
				AgentVersion: "1.0.0",
				UploadHash:   "aexCzQgBAnRYEZxKy71lAw==",
				FilePath:     tempDir + "/scratch/temp_test_data/daemonsets.jsonl",
			}
			// upload with bad froontdoor credentials
			err = actualUploader.StorageServices[0].Upload(payload)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("frontdoor service login call failed"))
			service.SecretManager = cldy.NewKeyValueSecretManager("good-key", "")
			// upload with good key but bad clusterUID
			err = actualUploader.StorageServices[0].Upload(payload)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cloudability clusters/upload request call failed with status"))
			// upload with successful login and successful url generation
			payload.ClusterUID = "good-cluster"
			err = actualUploader.StorageServices[0].Upload(payload)
			Expect(err).ToNot(HaveOccurred())
		})
		It("should only login once", func() {
			config := defaultConfig(tempDir)
			config.UploadFrequency = 250 * time.Millisecond

			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			err := os.CopyFS(tempDir+"/scratch/temp_test_data", os.DirFS("testdata"))
			Expect(err).ToNot(HaveOccurred())
			uploader.SetClusterID("test_id")

			actualUploader := uploader.(*cldy.CldyUploader)
			mcs := mockClientService{}
			service := cldy.ApptioServiceImpl{
				CldyUploadClient: &mcs,
				SecretManager:    cldy.NewKeyValueSecretManager("good-key", ""),
			}
			actualUploader.StorageServices = []cldy.StorageService{&service}

			uploader.AddSample(tempDir + "/scratch/temp_test_data")
			time.Sleep(500 * time.Millisecond)
			Expect(mcs.countByPath["/service/apikeylogin"]).To(Equal(1))
			Expect(mcs.countByPath["/v3/internal/containers/clusters/upload"]).To(Equal(1))
			Expect(mcs.countByPath["somewhere/valid-location"]).To(Equal(1))

			err = os.CopyFS(tempDir+"/scratch/temp_test_data", os.DirFS("testdata"))
			Expect(err).ToNot(HaveOccurred())
			uploader.AddSample(tempDir + "/scratch/temp_test_data")
			time.Sleep(500 * time.Millisecond)
			Expect(mcs.countByPath["/service/apikeylogin"]).To(Equal(1))
			Expect(mcs.countByPath["/v3/internal/containers/clusters/upload"]).To(Equal(2))
			Expect(mcs.countByPath["somewhere/valid-location"]).To(Equal(2))
		})

		It("should log back in if required", func() {
			config := defaultConfig(tempDir)
			config.UploadFrequency = 250 * time.Millisecond

			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			err := os.CopyFS(tempDir+"/scratch/temp_test_data", os.DirFS("testdata"))
			Expect(err).ToNot(HaveOccurred())
			uploader.SetClusterID("test_id")

			actualUploader := uploader.(*cldy.CldyUploader)
			mcs := mockClientService{}
			service := cldy.ApptioServiceImpl{
				CldyUploadClient: &mcs,
				SecretManager:    cldy.NewKeyValueSecretManager("short-lived-token", ""),
			}
			actualUploader.StorageServices = []cldy.StorageService{&service}

			uploader.AddSample(tempDir + "/scratch/temp_test_data")
			time.Sleep(500 * time.Millisecond)
			Expect(mcs.countByPath["/service/apikeylogin"]).To(Equal(1))
			Expect(mcs.countByPath["/v3/internal/containers/clusters/upload"]).To(Equal(1))
			Expect(mcs.countByPath["somewhere/valid-location"]).To(Equal(1))

			err = os.CopyFS(tempDir+"/scratch/temp_test_data", os.DirFS("testdata"))
			Expect(err).ToNot(HaveOccurred())
			uploader.AddSample(tempDir + "/scratch/temp_test_data")
			time.Sleep(500 * time.Millisecond)
			Expect(mcs.countByPath["/service/apikeylogin"]).To(Equal(2))
			Expect(mcs.countByPath["/v3/internal/containers/clusters/upload"]).To(Equal(2))
			Expect(mcs.countByPath["somewhere/valid-location"]).To(Equal(2))
		})
		It("should upload via metrics-collector api key", func() {
			config := defaultConfig(tempDir)
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			err := os.CopyFS(tempDir+"/scratch/temp_test_data", os.DirFS("testdata"))
			Expect(err).ToNot(HaveOccurred())
			uploader.SetClusterID("test_id")
			actualUploader := uploader.(*cldy.CldyUploader)
			service := cldy.MetricsCollectorServiceImpl{
				APIKey:           "goodkey123",
				BaseURL:          "https://metrics-collector.example.com/metricsample",
				UserAgent:        "cldy-client/test",
				CldyUploadClient: &mockClientService{},
			}
			actualUploader.StorageServices = []cldy.StorageService{&service}
			payload := cldy.UploadPayload{
				ClusterUID:   "good-cluster",
				FileName:     goodFileName,
				AgentVersion: "1.0.0",
				UploadHash:   "aexCzQgBAnRYEZxKy71lAw==",
				FilePath:     tempDir + "/scratch/temp_test_data/daemonsets.jsonl",
			}
			err = actualUploader.StorageServices[0].Upload(payload)
			Expect(err).ToNot(HaveOccurred())
		})
		It("should upload to custom s3 bucket", func() {
			config := defaultConfig(tempDir)
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			err := os.CopyFS(tempDir+"/scratch/temp_test_data", os.DirFS("testdata"))
			Expect(err).ToNot(HaveOccurred())
			uploader.SetClusterID("test_id")
			actualUploader := uploader.(*cldy.CldyUploader)
			uploadClient := &mockS3UploadService{}
			service := cldy.CustomS3Client{
				UploadClient: uploadClient,
			}
			actualUploader.StorageServices = []cldy.StorageService{&service}

			// Succeed on a good filename
			payload := cldy.UploadPayload{
				ClusterUID:   "good-cluster",
				FileName:     goodFileName,
				AgentVersion: "1.0.0",
				UploadHash:   "aexCzQgBAnRYEZxKy71lAw==",
				FilePath:     tempDir + "/scratch/temp_test_data/daemonsets.jsonl",
			}
			err = actualUploader.StorageServices[0].Upload(payload)
			Expect(err).ToNot(HaveOccurred())
			Expect(uploadClient.UploadedSampleName).To(Equal("production/data/metrics-agent/2025/05/05/good-cluster/good-cluster-20250505-18-05.tgz"))

			// Error on an unparseable filename
			payload.FileName = "badFileName"
			err = actualUploader.StorageServices[0].Upload(payload)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("error parsing name from sample filename"))
		})
		It("should upload to custom azure blob", func() {
			config := defaultConfig(tempDir)
			config.CustomAzureBlobContainerName = "a"
			config.CustomAzureBlobUrl = "testurl"
			config.CustomAzureTenantID = "1"
			config.CustomAzureClientID = "1"
			config.CustomAzureClientSecret = cldy.NewValueSecretManager("1")

			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			err := os.CopyFS(tempDir+"/scratch/temp_test_data", os.DirFS("testdata"))
			Expect(err).ToNot(HaveOccurred())
			uploader.SetClusterID("test_id")
			actualUploader := uploader.(*cldy.CldyUploader)
			uploadClient := &MockBlobUploadService{}
			service := cldy.CustomBlobClient{
				UploadClient: uploadClient,
			}
			actualUploader.StorageServices = []cldy.StorageService{&service}

			// Succeed on a good filename
			payload := cldy.UploadPayload{
				ClusterUID:   "good-cluster",
				FileName:     goodFileName,
				AgentVersion: "1.0.0",
				UploadHash:   "aexCzQgBAnRYEZxKy71lAw==",
				FilePath:     tempDir + "/scratch/temp_test_data/daemonsets.jsonl",
			}
			err = actualUploader.StorageServices[0].Upload(payload)
			Expect(err).ToNot(HaveOccurred())
			Expect(uploadClient.UploadedSampleName).To(Equal("production/data/metrics-agent/2025/05/05/good-cluster/good-cluster-20250505-18-05.tgz"))

			// Error on an unparseable filename
			payload.FileName = "badFileName"
			err = actualUploader.StorageServices[0].Upload(payload)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("error parsing name from sample filename"))
		})
	})
	Context("TestUploadQueue", func() {
		It("should keep the entries it uploaded when a later entry in the batch fails", func() {
			paths := seedUploadQueue(tempDir, 5)
			actualUploader := newUploader(noUploadConfig(tempDir))
			Expect(actualUploader.PendingUploads()).To(ConsistOf(paths))
			service := &mockStorageService{failPath: paths[2]}
			actualUploader.StorageServices = []cldy.StorageService{service}

			Expect(actualUploader.DrainUploads()).To(HaveOccurred())
			// the four that shipped are gone from disk, so retaining them would wedge the queue
			Expect(actualUploader.PendingUploads()).To(ConsistOf(paths[2]))
			Expect(paths[2]).To(BeAnExistingFile())
			Expect(paths[0]).ToNot(BeAnExistingFile())

			// the next cycle retries the failed tar
			service.failPath = ""
			Expect(actualUploader.DrainUploads()).To(Succeed())
			Expect(actualUploader.PendingUploads()).To(BeEmpty())
			Expect(service.uploaded()).To(HaveLen(len(paths)))
		})
		It("should bound the work one drain does instead of scaling it with the queue depth", func() {
			paths := seedUploadQueue(tempDir, 20)
			config := noUploadConfig(tempDir)
			config.UploadFrequency = 400 * time.Millisecond // 200ms drain budget
			actualUploader := newUploader(config)
			// the destination is down: every upload burns its timeout before failing
			service := &mockStorageService{delay: 50 * time.Millisecond, uploadErr: errors.New("unreachable")}
			actualUploader.StorageServices = []cldy.StorageService{service}

			start := time.Now()
			Expect(actualUploader.DrainUploads()).To(HaveOccurred())

			// unbounded this is 20 attempts and a full second; 200ms / 50ms admits 4, plus slack
			Expect(service.callCount()).To(BeNumerically("<=", 6))
			Expect(time.Since(start)).To(BeNumerically("<", config.UploadFrequency))
			Expect(actualUploader.PendingUploads()).To(ConsistOf(paths))
		})
		It("should abandon the batch only when the destination itself is unavailable", func() {
			paths := seedUploadQueue(tempDir, 5)
			actualUploader := newUploader(noUploadConfig(tempDir))

			// no storage services: every tar would fail identically, so only one is attempted
			err := actualUploader.DrainUploads()
			Expect(err).To(MatchError(cldy.ErrNoStorageServices))
			Expect(strings.Count(err.Error(), ".tgz")).To(Equal(1))
			Expect(actualUploader.PendingUploads()).To(ConsistOf(paths))

			// a per-tar failure must not starve the entries behind it
			service := &mockStorageService{uploadErr: errors.New("corrupt tar")}
			actualUploader.StorageServices = []cldy.StorageService{service}
			Expect(actualUploader.DrainUploads()).To(HaveOccurred())
			Expect(service.callCount()).To(Equal(len(paths)))
			Expect(actualUploader.PendingUploads()).To(ConsistOf(paths))
		})
		It("should drop queued entries whose tar no longer exists", func() {
			paths := seedUploadQueue(tempDir, 2)
			actualUploader := newUploader(noUploadConfig(tempDir))
			Expect(os.Remove(paths[0])).To(Succeed())
			service := &mockStorageService{}
			actualUploader.StorageServices = []cldy.StorageService{service}

			Expect(actualUploader.DrainUploads()).To(Succeed())
			Expect(actualUploader.PendingUploads()).To(BeEmpty())
			Expect(service.uploaded()).To(HaveLen(1))
		})
		It("should drop queued entries for the tars it clears under disk pressure", func() {
			paths := seedUploadQueue(tempDir, 1)
			actualUploader := newUploader(noUploadConfig(tempDir))

			// age the tar past half the recovery period so the cleanup reclaims it
			Expect(os.Chtimes(paths[0], time.Now(), time.Now().Add(-2*time.Hour))).To(Succeed())
			Expect(actualUploader.ClearOldUploadSamples()).To(Succeed())

			Expect(paths[0]).ToNot(BeAnExistingFile())
			Expect(actualUploader.PendingUploads()).To(BeEmpty())
		})
	})
	Context("TestUploadDataStorageServices", func() {
		It("should retain the sample and error when no storage services are configured", func() {
			actualUploader := newUploader(noUploadConfig(tempDir))
			samplePath := writeSample(actualUploader.UploadPathDir)

			Expect(actualUploader.UploadData(samplePath)).To(MatchError(cldy.ErrNoStorageServices))
			// the tar must survive so a later cycle can ship it
			Expect(samplePath).To(BeAnExistingFile())
		})
		It("should upload and remove the sample when a storage service is configured", func() {
			actualUploader := newUploader(noUploadConfig(tempDir))
			service := &mockStorageService{}
			actualUploader.StorageServices = []cldy.StorageService{service}
			samplePath := writeSample(actualUploader.UploadPathDir)

			Expect(actualUploader.UploadData(samplePath)).To(Succeed())
			Expect(service.uploaded()).To(HaveLen(1))
			Expect(service.uploaded()[0].FilePath).To(Equal(samplePath))
			Expect(samplePath).ToNot(BeAnExistingFile())
		})
		It("should re-attempt storage service construction once per upload cycle", func() {
			secretManager := &stubSecretManager{failures: 1}
			config := azureConfig(tempDir, secretManager)
			config.UploadFrequency = 50 * time.Millisecond
			actualUploader := newUploader(config)
			// the secret was unavailable on startup, leaving the uploader with no services
			Expect(actualUploader.StorageServices).To(BeEmpty())
			Expect(secretManager.callCount()).To(Equal(1))

			// within the cooldown construction is not re-attempted
			samplePath := writeSample(actualUploader.UploadPathDir)
			Expect(actualUploader.UploadData(samplePath)).To(MatchError(cldy.ErrNoStorageServices))
			Expect(secretManager.callCount()).To(Equal(1))
			Expect(samplePath).To(BeAnExistingFile())

			// a cycle later the secret is available and construction succeeds. A missing tar is
			// used so the rebuilt client never reaches azure; it is dropped without error
			time.Sleep(config.UploadFrequency)
			err := actualUploader.UploadData(filepath.Join(actualUploader.UploadPathDir, "missing.tgz"))
			Expect(err).ToNot(HaveOccurred())
			Expect(actualUploader.StorageServices).To(HaveLen(1))
			Expect(secretManager.callCount()).To(BeNumerically(">=", 2))
		})
		It("should measure the retry cooldown from when the build finished", func() {
			// the secret read outlasts a whole upload cycle and never succeeds
			secretManager := &stubSecretManager{failures: -1, delay: 300 * time.Millisecond}
			config := azureConfig(tempDir, secretManager)
			config.UploadFrequency = 200 * time.Millisecond
			actualUploader := newUploader(config)
			Expect(secretManager.callCount()).To(Equal(1))

			// a start-stamped cooldown would already have expired and let this attempt through
			samplePath := writeSample(actualUploader.UploadPathDir)
			Expect(actualUploader.UploadData(samplePath)).To(MatchError(cldy.ErrNoStorageServices))
			Expect(secretManager.callCount()).To(Equal(1))
		})
	})
})

// defaultConfig targets the cloudability upload path, which the suite redirects at a local server.
// Specs replace StorageServices rather than appending so their stub is the only service used.
func defaultConfig(tempDir string) cldy.UploaderConfig {
	return cldy.UploaderConfig{
		UploadFrequency: time.Hour,
		ScratchDir:      tempDir,
		SecretManager:   cldy.NewKeyValueSecretManager("", ""),
		EnvID:           "1",
	}
}

// copies the entire directory
func copyCompleteData(destination, source string) error {
	return os.CopyFS(destination, os.DirFS(source))
}

// copies directory set and removes files in provided list for incomplete data set testing purposes
func copyIncompleteData(destination, source string, filesToRemove []string) error {
	err := os.CopyFS(destination, os.DirFS(source))
	if err != nil {
		return err
	}
	for _, file := range filesToRemove {
		fErr := os.Remove(destination + "/" + file)
		if fErr != nil {
			return fErr
		}
	}
	return nil
}

// sample timestamps need to be unique otherwise .tgz file names will be the same and cause overwrite which would
// never occur in real data collection/uploading
func updateAgentTimestamp(filePath string, ts int64) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer safeClose(file.Close)
	data, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	measure := testAgentMeasure{}
	err = json.Unmarshal(data, &measure)
	if err != nil {
		return err
	}
	measure.Timestamp = ts
	jsonInfo, err := json.Marshal(&measure)
	if err != nil {
		return err
	}
	return os.WriteFile(filePath, jsonInfo, 0644)
}

type testAgentMeasure struct {
	Timestamp int64  `json:"ts"`
	Name      string `json:"name"`
}

func checkCollectionAndConstruction(tempDir string, uploader cldy.Uploader, actualUploader *cldy.CldyUploader) {
	err := copyCompleteData(tempDir+"/scratch/temp_test_data", "testdata")
	Expect(err).ToNot(HaveOccurred())
	uploader.AddSample(tempDir + "/scratch/temp_test_data")

	path, err := actualUploader.ConstructPayload(time.Now())
	Expect(err).ToNot(HaveOccurred())
	fileInfo, err := os.Stat(path)
	Expect(err).ToNot(HaveOccurred())
	Expect(fileInfo.Size()).To(BeNumerically(">", 0))
}

func checkScratchEmpty(dir string) {
	f, err := os.Open(dir)
	Expect(err).To(Not(HaveOccurred()))
	defer safeClose(f.Close)
	_, err = f.Readdir(1)
	Expect(err).To(BeEquivalentTo(io.EOF))
}

type mockClientService struct {
	countByPath map[string]int
}

type mockfrontdoorRequestBody struct {
	KeyAccess string `json:"KeyAccess"`
	KeySecret string `json:"KeySecret"`
}

type mockGetURLRequestBody struct {
	ClusterUID   string `json:"ClusterUID"`
	FileName     string `json:"FileName"`
	AgentVersion string `json:"AgentVersion"`
	UploadHash   string `json:"UploadHash"`
}

func (mcs *mockClientService) Do(r *http.Request, _ string) (res *http.Response, err error) {
	if mcs.countByPath == nil {
		mcs.countByPath = map[string]int{}
	}
	mcs.countByPath[r.URL.Path] += 1
	// request to login to Frontdoor
	if strings.Contains(r.URL.Path, "apikeylogin") {
		var body mockfrontdoorRequestBody
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.KeyAccess == "bad-key" {
			return &http.Response{StatusCode: 403, Body: r.Body}, nil
		} else {
			resp := http.Response{StatusCode: 200, Body: r.Body, Header: http.Header{}}
			resp.Header.Set("Apptio-Opentoken", "happytoken")
			if body.KeyAccess == "short-lived-token" {
				resp.Header.Set("valid_till", strconv.FormatInt(time.Now().UnixMilli(), 10))
			} else {
				resp.Header.Set("valid_till", strconv.FormatInt(time.Now().Add(time.Hour).UnixMilli(), 10))
			}
			return &resp, nil
		}
	}
	// request to acquire s3 url from metrics-collector
	if strings.Contains(r.URL.Path, "metricsample") {
		if r.Header.Get("x-api-key") == "bad-key" {
			return &http.Response{StatusCode: 403, Body: io.NopCloser(strings.NewReader(""))}, nil
		}
		responseBody, _ := json.Marshal(map[string]string{
			"location": "somewhere/valid-location",
		})
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(responseBody))}, nil
	}
	// request to acquire s3 url from Cloudability
	if strings.Contains(r.URL.Path, "clusters/upload") {
		var body mockGetURLRequestBody
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.ClusterUID == "bad-cluster" {
			return &http.Response{StatusCode: 400, Body: r.Body}, nil
		} else {
			responseBody, _ := json.Marshal(cldy.CloudabilityClustersUploadResponse{
				Result: cldy.CloudabilityClustersUploadInfo{
					Location: "somewhere/valid-location",
				}})
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(responseBody))}, nil
		}
	}
	// request to upload data to S3
	if strings.Contains(r.URL.Path, "valid-location") {
		hash := md5.New()
		// cannot close body here as it will close the underlying file
		if _, err = io.Copy(hash, r.Body); err != nil {
			return &http.Response{}, fmt.Errorf("invalid body: %s", err.Error())
		}
		fileHash := base64.StdEncoding.EncodeToString(hash.Sum(nil))
		if fileHash != r.Header.Get("Content-MD5") {
			return &http.Response{}, fmt.Errorf("invalid hash: calculated %s, expected %s",
				fileHash, r.Header.Get("Content-MD5"))
		}
		return &http.Response{StatusCode: 200, Body: r.Body, Header: http.Header{}}, nil
	}
	return &http.Response{}, fmt.Errorf("unknown request")
}

type mockS3UploadService struct {
	UploadedSampleName string
}

func (mcs *mockS3UploadService) Do(sampleToUpload *s3manager.UploadInput) error {
	if sampleToUpload.Body == nil {
		return fmt.Errorf("No sample detected")
	}

	mcs.UploadedSampleName = *sampleToUpload.Key
	return nil
}

type MockBlobUploadService struct {
	UploadedSampleName string
}

func (mcs *MockBlobUploadService) Do(sampleToUpload *cldy.BlobUploadInput) error {
	if sampleToUpload.Body == nil {
		return fmt.Errorf("No sample detected")
	}

	mcs.UploadedSampleName = sampleToUpload.BlobName
	return nil
}

// noUploadConfig has no upload destination configured, so the uploader starts with no storage
// services and makes no network calls
func noUploadConfig(tempDir string) cldy.UploaderConfig {
	return cldy.UploaderConfig{
		UploadFrequency: time.Hour,
		ScratchDir:      tempDir,
		RecoveryPeriod:  time.Hour,
	}
}

func newUploader(config cldy.UploaderConfig) *cldy.CldyUploader {
	stopCh := make(chan struct{})
	DeferCleanup(func() { close(stopCh) })
	uploader := cldy.NewCldyUploader(config, stopCh)
	uploader.SetClusterID("test_id")
	return uploader.(*cldy.CldyUploader)
}

// azureConfig targets the azure blob upload path with the given secret manager
func azureConfig(tempDir string, secretManager cldy.SecretManager) cldy.UploaderConfig {
	config := noUploadConfig(tempDir)
	config.CustomAzureBlobContainerName = "container"
	config.CustomAzureBlobUrl = "https://example.blob.core.windows.net/"
	config.CustomAzureTenantID = "tenant-id"
	config.CustomAzureClientID = "client-id"
	config.CustomAzureClientSecret = secretManager
	return config
}

func writeSample(uploadPathDir string) string {
	samplePath := filepath.Join(uploadPathDir, goodFileName)
	Expect(os.WriteFile(samplePath, []byte("sample contents"), 0600)).To(Succeed())
	return samplePath
}

// seedUploadQueue writes count tars with distinct timestamps into the upload directory, so a
// subsequently constructed uploader recovers all of them into its queue
func seedUploadQueue(tempDir string, count int) []string {
	uploadDir := filepath.Join(tempDir, "upload")
	Expect(os.MkdirAll(uploadDir, os.ModePerm)).To(Succeed())
	paths := make([]string, 0, count)
	for i := range count {
		stamp := time.Now().UTC().Add(-time.Duration(i) * time.Minute).Format("2006-01-02-15-04-05")
		path := filepath.Join(uploadDir, fmt.Sprintf("test-id_%s.tgz", stamp))
		Expect(os.WriteFile(path, []byte("sample contents"), 0600)).To(Succeed())
		paths = append(paths, path)
	}
	return paths
}

// mockStorageService stands in for an upload destination. The mutex is for specs that let the
// upload loop drive it.
type mockStorageService struct {
	mutex     sync.Mutex
	uploads   []cldy.UploadPayload
	calls     int
	uploadErr error         // fails every upload
	failPath  string        // fails only this tar
	delay     time.Duration // per-upload cost, standing in for a client timeout
}

func (mss *mockStorageService) Upload(payload cldy.UploadPayload) error {
	mss.mutex.Lock()
	mss.calls++
	delay, uploadErr, failPath := mss.delay, mss.uploadErr, mss.failPath
	mss.mutex.Unlock()

	time.Sleep(delay)
	if uploadErr != nil {
		return uploadErr
	}
	if failPath != "" && payload.FilePath == failPath {
		return fmt.Errorf("transient upload failure for %s", payload.FilePath)
	}

	mss.mutex.Lock()
	defer mss.mutex.Unlock()
	mss.uploads = append(mss.uploads, payload)
	return nil
}

func (mss *mockStorageService) uploaded() []cldy.UploadPayload {
	mss.mutex.Lock()
	defer mss.mutex.Unlock()
	return append([]cldy.UploadPayload(nil), mss.uploads...)
}

func (mss *mockStorageService) callCount() int {
	mss.mutex.Lock()
	defer mss.mutex.Unlock()
	return mss.calls
}

// stubSecretManager fails the first `failures` reads (every read when negative), taking `delay`
// each time
type stubSecretManager struct {
	failures int
	delay    time.Duration
	mutex    sync.Mutex
	calls    int
}

func (ssm *stubSecretManager) GetSecret() ([]byte, error) {
	ssm.mutex.Lock()
	ssm.calls++
	call := ssm.calls
	ssm.mutex.Unlock()
	time.Sleep(ssm.delay)
	if ssm.failures < 0 || call <= ssm.failures {
		return nil, fmt.Errorf("secret unavailable")
	}
	return []byte("client-secret"), nil
}

func (ssm *stubSecretManager) callCount() int {
	ssm.mutex.Lock()
	defer ssm.mutex.Unlock()
	return ssm.calls
}
