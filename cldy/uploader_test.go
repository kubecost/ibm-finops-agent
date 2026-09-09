package cldy_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
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
			expectTarHoldsSample(path, "test_id")
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
			// the cleanup only happens inside an upload cycle, so the tick has to actually
			// fire during the spec
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

			uploadDir := actualUploader.UploadPathDir
			Expect(os.ReadDir(uploadDir)).To(BeEmpty())

			uploader.AddSample(tempDir + "/scratch/temp_test_data")

			// the cycle builds a tar under the upload directory and ships it. The service is
			// handed the tar's path and its hash, which can only have been read off a tar that
			// existed at that point
			Eventually(service.uploaded, 5*time.Second, 10*time.Millisecond).Should(HaveLen(1))
			shipped := service.uploaded()[0]
			Expect(shipped.FilePath).To(HavePrefix(uploadDir + "/"))
			Expect(shipped.FilePath).To(HaveSuffix(".tgz"))
			Expect(shipped.UploadHash).ToNot(BeEmpty())

			// and the tar is cleaned up afterwards: gone from disk, gone from the queue
			Eventually(func() []os.DirEntry {
				entries, dErr := os.ReadDir(uploadDir)
				Expect(dErr).ToNot(HaveOccurred())
				return entries
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
			config := noUploadConfig(tempDir)
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			uploader.SetClusterID("test_id")
			actualUploader := uploader.(*cldy.CldyUploader)
			Expect(actualUploader.RecoveredUploads).To(Equal(len(paths)))
			Expect(actualUploader.PendingUploads()).To(ConsistOf(paths))

			// one of the queued tars hits a transient upload failure this cycle
			service := &mockStorageService{failPath: paths[2]}
			actualUploader.StorageServices = []cldy.StorageService{service}

			err := actualUploader.DrainUploads()
			Expect(err).To(HaveOccurred())
			// only the tar that failed is still queued: the four that shipped were removed
			// from disk, so retaining them would wedge the queue on files that cannot be
			// re-opened
			Expect(actualUploader.PendingUploads()).To(ConsistOf(paths[2]))
			for i, path := range paths {
				_, statErr := os.Stat(path)
				if i == 2 {
					Expect(statErr).ToNot(HaveOccurred())
				} else {
					Expect(os.IsNotExist(statErr)).To(BeTrue())
				}
			}

			// the next cycle retries the failed tar, and the queue drains
			service.setFailPath("")
			Expect(actualUploader.DrainUploads()).To(Succeed())
			Expect(actualUploader.PendingUploads()).To(BeEmpty())
			Expect(service.uploaded()).To(HaveLen(len(paths)))
			_, statErr := os.Stat(paths[2])
			Expect(os.IsNotExist(statErr)).To(BeTrue())
		})
		It("should bound the work one drain does instead of scaling it with the queue depth", func() {
			paths := seedUploadQueue(tempDir, 20)
			config := noUploadConfig(tempDir)
			// a 400ms cycle gives a single drain a 200ms budget
			config.UploadFrequency = 400 * time.Millisecond
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			uploader.SetClusterID("test_id")
			actualUploader := uploader.(*cldy.CldyUploader)
			Expect(actualUploader.PendingUploads()).To(HaveLen(len(paths)))

			// the destination is down: every upload burns its timeout and retries before failing
			service := &mockStorageService{
				delay:     50 * time.Millisecond,
				uploadErr: fmt.Errorf("upload destination unreachable"),
			}
			actualUploader.StorageServices = []cldy.StorageService{service}

			start := time.Now()
			Expect(actualUploader.DrainUploads()).To(HaveOccurred())
			elapsed := time.Since(start)

			// unbounded this is 20 attempts and a full second, proportional to the queue depth
			// and well past the tick it has to fit inside
			Expect(service.callCount()).To(BeNumerically("<=", 6))
			Expect(elapsed).To(BeNumerically("<", config.UploadFrequency))
			// nothing shipped, so the whole queue is retained for later cycles
			Expect(actualUploader.PendingUploads()).To(ConsistOf(paths))
		})
		It("should abandon the rest of the batch when the destination itself is unavailable", func() {
			paths := seedUploadQueue(tempDir, 5)
			config := noUploadConfig(tempDir)
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			actualUploader := uploader.(*cldy.CldyUploader)
			Expect(actualUploader.StorageServices).To(BeEmpty())
			Expect(actualUploader.PendingUploads()).To(ConsistOf(paths))

			err := actualUploader.DrainUploads()
			Expect(err).To(MatchError(cldy.ErrNoStorageServices))
			// there is nothing to upload with, so every queued tar would fail identically and
			// only the first is attempted. The joined error names one tar, not all five
			Expect(strings.Count(err.Error(), ".tgz")).To(Equal(1))

			// and nothing is dropped: the whole queue survives for a later cycle
			Expect(actualUploader.PendingUploads()).To(ConsistOf(paths))
			for _, path := range paths {
				_, statErr := os.Stat(path)
				Expect(statErr).ToNot(HaveOccurred())
			}
		})
		It("should keep attempting the batch when entries fail for their own reasons", func() {
			paths := seedUploadQueue(tempDir, 5)
			config := noUploadConfig(tempDir)
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			uploader.SetClusterID("test_id")
			actualUploader := uploader.(*cldy.CldyUploader)
			Expect(actualUploader.PendingUploads()).To(ConsistOf(paths))

			// a failure that is about the individual tar rather than the destination
			service := &mockStorageService{uploadErr: fmt.Errorf("corrupt tar")}
			actualUploader.StorageServices = []cldy.StorageService{service}

			Expect(actualUploader.DrainUploads()).To(HaveOccurred())
			// every entry gets its own attempt, so a single bad tar cannot starve the entries
			// behind it in the queue
			Expect(service.callCount()).To(Equal(len(paths)))
			Expect(actualUploader.PendingUploads()).To(ConsistOf(paths))
		})
		It("should drop queued entries whose tar no longer exists", func() {
			paths := seedUploadQueue(tempDir, 2)
			config := noUploadConfig(tempDir)
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			uploader.SetClusterID("test_id")
			actualUploader := uploader.(*cldy.CldyUploader)
			Expect(actualUploader.PendingUploads()).To(ConsistOf(paths))

			// the tar disappears from underneath the queue entry
			Expect(os.Remove(paths[0])).To(Succeed())

			service := &mockStorageService{}
			actualUploader.StorageServices = []cldy.StorageService{service}
			Expect(actualUploader.DrainUploads()).To(Succeed())
			Expect(actualUploader.PendingUploads()).To(BeEmpty())
			Expect(service.uploaded()).To(HaveLen(1))
			Expect(service.uploaded()[0].FilePath).To(Equal(paths[1]))
		})
		It("should drop queued entries for the tars it clears under disk pressure", func() {
			paths := seedUploadQueue(tempDir, 1)
			config := noUploadConfig(tempDir)
			config.RecoveryPeriod = time.Hour
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			actualUploader := uploader.(*cldy.CldyUploader)
			Expect(actualUploader.PendingUploads()).To(ConsistOf(paths))

			// age the tar past half the recovery period so the cleanup reclaims it
			Expect(os.Chtimes(paths[0], time.Now(), time.Now().Add(-2*time.Hour))).To(Succeed())
			Expect(actualUploader.ClearOldUploadSamples()).To(Succeed())

			_, err := os.Stat(paths[0])
			Expect(os.IsNotExist(err)).To(BeTrue())
			Expect(actualUploader.PendingUploads()).To(BeEmpty())
		})
	})
	Context("TestUploadDataStorageServices", func() {
		It("should retain the sample and error when no storage services are configured", func() {
			config := noUploadConfig(tempDir)
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			actualUploader := uploader.(*cldy.CldyUploader)
			Expect(actualUploader.StorageServices).To(BeEmpty())

			samplePath := writeSample(actualUploader.UploadPathDir)
			err := actualUploader.UploadData(samplePath)
			Expect(err).To(MatchError(cldy.ErrNoStorageServices))

			// the tar must survive so a later cycle can ship it
			_, err = os.Stat(samplePath)
			Expect(err).ToNot(HaveOccurred())
		})
		It("should upload and remove the sample when a storage service is configured", func() {
			config := noUploadConfig(tempDir)
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			uploader.SetClusterID("test_id")
			actualUploader := uploader.(*cldy.CldyUploader)
			service := &mockStorageService{}
			actualUploader.StorageServices = append(actualUploader.StorageServices, service)

			samplePath := writeSample(actualUploader.UploadPathDir)
			err := actualUploader.UploadData(samplePath)
			Expect(err).ToNot(HaveOccurred())
			Expect(service.uploaded()).To(HaveLen(1))
			Expect(service.uploaded()[0].ClusterUID).To(Equal("test_id"))
			Expect(service.uploaded()[0].FileName).To(Equal(goodFileName))
			Expect(service.uploaded()[0].FilePath).To(Equal(samplePath))

			_, err = os.Stat(samplePath)
			Expect(os.IsNotExist(err)).To(BeTrue())
		})
		It("should not re-attempt storage service construction twice in one upload cycle", func() {
			secretManager := &flakySecretManager{}
			config := flakyAzureConfig(tempDir, secretManager)
			// an upload cycle is an hour, so no retry is due yet
			config.UploadFrequency = time.Hour
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			actualUploader := uploader.(*cldy.CldyUploader)
			Expect(actualUploader.StorageServices).To(BeEmpty())
			Expect(secretManager.calls).To(Equal(1))

			samplePath := writeSample(actualUploader.UploadPathDir)
			err := actualUploader.UploadData(samplePath)
			Expect(err).To(MatchError(cldy.ErrNoStorageServices))
			// construction was not attempted again within the same cycle
			Expect(secretManager.calls).To(Equal(1))
			_, err = os.Stat(samplePath)
			Expect(err).ToNot(HaveOccurred())
		})
		It("should re-attempt storage service construction on a later upload cycle", func() {
			secretManager := &flakySecretManager{}
			config := flakyAzureConfig(tempDir, secretManager)
			config.UploadFrequency = 10 * time.Millisecond
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			actualUploader := uploader.(*cldy.CldyUploader)
			// the secret was unavailable on startup, leaving the uploader with no services
			Expect(actualUploader.StorageServices).To(BeEmpty())
			Expect(secretManager.calls).To(Equal(1))

			// a cycle later the secret is available again and construction succeeds. an
			// absent tar is used so the rebuilt client is never asked to reach azure; the
			// missing tar is simply dropped, which is why no error comes back
			time.Sleep(50 * time.Millisecond)
			err := actualUploader.UploadData(filepath.Join(actualUploader.UploadPathDir, "missing.tgz"))
			Expect(err).ToNot(HaveOccurred())
			Expect(actualUploader.StorageServices).To(HaveLen(1))
			Expect(secretManager.calls).To(BeNumerically(">", 1))
		})
		It("should measure the storage service retry cooldown from when the build finished", func() {
			// the secret read takes longer than a whole upload cycle and never succeeds, so
			// the build itself outlasts the cooldown it is supposed to start
			secretManager := &slowSecretManager{delay: 300 * time.Millisecond}
			config := flakyAzureConfig(tempDir, secretManager)
			config.UploadFrequency = 200 * time.Millisecond
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			actualUploader := uploader.(*cldy.CldyUploader)
			Expect(actualUploader.StorageServices).To(BeEmpty())
			Expect(secretManager.callCount()).To(Equal(1))

			samplePath := writeSample(actualUploader.UploadPathDir)
			err := actualUploader.UploadData(samplePath)
			Expect(err).To(MatchError(cldy.ErrNoStorageServices))
			// stamping the cooldown before the build would have let this attempt straight
			// through, because the build alone took longer than an upload cycle
			Expect(secretManager.callCount()).To(Equal(1))
		})
	})
})

// defaultConfig configures the cloudability upload path. The suite redirects the frontdoor and
// cloudability base URLs at a local server, so the resulting uploader starts with a working
// apptio storage service and makes no outbound network calls. Specs that drive their own stub
// service replace StorageServices rather than appending, so that theirs is the only one used.
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
	expectTarHoldsSample(path, "123456-1234-1234-123456789012")
}

// expectTarHoldsSample asserts that the gzipped tar at path actually carries the sample. A
// bare Size() > 0 check does not: gzipping an empty tar still yields ~20 non-zero bytes, so
// that assertion passes on a tar that captured nothing at all.
func expectTarHoldsSample(path, clusterID string) {
	file, err := os.Open(path)
	Expect(err).ToNot(HaveOccurred())
	defer safeClose(file.Close)

	gzr, err := gzip.NewReader(file)
	Expect(err).ToNot(HaveOccurred())
	defer safeClose(gzr.Close)

	sizeByName := map[string]int64{}
	reader := tar.NewReader(gzr)
	for {
		header, hErr := reader.Next()
		if errors.Is(hErr, io.EOF) {
			break
		}
		Expect(hErr).ToNot(HaveOccurred())
		// entries are namespaced under the cluster the sample came from
		Expect(header.Name).To(ContainSubstring(clusterID))
		sizeByName[filepath.Base(header.Name)] = header.Size
	}

	// the sample is in there, and its entries carry the bytes rather than just a header. Two
	// of the collected files (replicationcontrollers, statefulsets) are legitimately empty in
	// the test data, so the check is against the files that are known to have content
	sourceFiles, err := os.ReadDir("testdata")
	Expect(err).ToNot(HaveOccurred())
	Expect(sizeByName).To(HaveLen(len(sourceFiles)))
	for _, name := range []string{"agent-measurement.json", "deployments.jsonl", "nodes.jsonl", "pods.jsonl"} {
		Expect(sizeByName).To(HaveKeyWithValue(name, BeNumerically(">", 0)))
	}
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

// noUploadConfig has no upload destination configured at all, so the uploader starts with no
// storage services and makes no network calls
func noUploadConfig(tempDir string) cldy.UploaderConfig {
	return cldy.UploaderConfig{
		UploadFrequency: time.Hour,
		ScratchDir:      tempDir,
		RecoveryPeriod:  time.Hour,
	}
}

// flakyAzureConfig targets the azure blob upload path with a secret that is unavailable on
// the first read, simulating a transient failure during uploader startup
func flakyAzureConfig(tempDir string, secretManager cldy.SecretManager) cldy.UploaderConfig {
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
	err := os.WriteFile(samplePath, []byte("sample contents"), 0600)
	Expect(err).ToNot(HaveOccurred())
	return samplePath
}

// seedUploadQueue writes count tars into the upload directory using recent, distinct
// timestamps so that a subsequently constructed uploader recovers every one of them into its
// upload queue. It returns the paths in the order they were written.
func seedUploadQueue(tempDir string, count int) []string {
	uploadDir := filepath.Join(tempDir, "upload")
	Expect(os.MkdirAll(uploadDir, os.ModePerm)).To(Succeed())
	paths := make([]string, 0, count)
	for i := 0; i < count; i++ {
		stamp := time.Now().UTC().Add(-time.Duration(i) * time.Minute).Format("2006-01-02-15-04-05")
		path := filepath.Join(uploadDir, fmt.Sprintf("test-id_%s.tgz", stamp))
		Expect(os.WriteFile(path, []byte("sample contents"), 0600)).To(Succeed())
		paths = append(paths, path)
	}
	return paths
}

// mockStorageService stands in for an upload destination. It is guarded by a mutex because
// specs that let the upload loop run drive it from the upload goroutine while the spec reads
// its state.
type mockStorageService struct {
	mutex   sync.Mutex
	uploads []cldy.UploadPayload
	// calls counts every Upload attempt, including the ones that fail, which is what tells a
	// bounded drain apart from one that is proportional to the queue depth
	calls     int
	uploadErr error
	// failPath, when set, fails only the upload of that one tar, leaving the rest of the
	// batch to succeed
	failPath string
	// delay stands in for the client timeout and retries an upload burns before failing
	// against an unreachable destination
	delay time.Duration
}

func (mss *mockStorageService) Upload(payload cldy.UploadPayload) error {
	mss.mutex.Lock()
	mss.calls++
	delay, uploadErr, failPath := mss.delay, mss.uploadErr, mss.failPath
	mss.mutex.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}
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

func (mss *mockStorageService) setFailPath(path string) {
	mss.mutex.Lock()
	defer mss.mutex.Unlock()
	mss.failPath = path
}

// slowSecretManager takes delay to read the secret and never succeeds, standing in for a
// storage service build that outlasts an upload cycle before failing
type slowSecretManager struct {
	delay time.Duration
	mutex sync.Mutex
	calls int
}

func (ssm *slowSecretManager) GetSecret() ([]byte, error) {
	ssm.mutex.Lock()
	ssm.calls++
	ssm.mutex.Unlock()
	time.Sleep(ssm.delay)
	return nil, fmt.Errorf("secret unavailable")
}

func (ssm *slowSecretManager) callCount() int {
	ssm.mutex.Lock()
	defer ssm.mutex.Unlock()
	return ssm.calls
}

// flakySecretManager fails the first read of the secret and succeeds from then on
type flakySecretManager struct {
	calls int
}

func (fsm *flakySecretManager) GetSecret() ([]byte, error) {
	fsm.calls++
	if fsm.calls == 1 {
		return nil, fmt.Errorf("secret temporarily unavailable")
	}
	return []byte("client-secret"), nil
}
